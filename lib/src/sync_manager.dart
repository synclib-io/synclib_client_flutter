import 'dart:async';

import 'connection/websocket_manager.dart' show ConnectionState;
import 'sync_client.dart';

/// Which transport the sync loop is currently using.
///
/// Mirrors the TS `TransportMode` in `@synclib-io/client`. The manager flips to
/// [TransportMode.http] when the socket can't be established or held, and back
/// to [TransportMode.socket] when it recovers. See
/// `docs/http_fallback_sync_plan.md` in the sync_server repo.
enum TransportMode { socket, http }

/// Snapshot passed to [SyncManager.transportChanges] listeners.
class TransportState {
  const TransportState({required this.mode, required this.isSyncLive});

  final TransportMode mode;

  /// True only when the socket is connected, its channels are joined, AND a
  /// go-live catch-up sync has completed — the local cache is complete and
  /// fresh. When false, fetch the rows a screen needs on demand
  /// ([SyncManager.ensureRow] / [SyncManager.ensureRows] /
  /// [SyncManager.fetchPage]).
  final bool isSyncLive;
}

/// Transport auto-switch over a [SyncClient]: keeps an app working while the
/// WebSocket degrades by falling back to the HTTP sync surface
/// (`/api/sync/member/*`), then switching back when the socket recovers.
///
/// The Dart peer of the TypeScript `SynclibClient` wrapper in
/// `@synclib-io/client` — same state machine, same [isSyncLive] semantics, and
/// the same by-id ([ensureRow] / [ensureRows]) and by-page ([fetchPage])
/// on-demand fetch surface.
///
/// Usage: wrap an already-constructed [SyncClient], call [attach] once (after
/// the client is wired), drive catch-up through [syncUnified] (which routes to
/// HTTP automatically in degraded mode), and read rows on demand via
/// [ensureRow] / [ensureRows] / [fetchPage] while degraded. Listen to
/// [transportChanges] to render "offline / catching up / live" UI and to kick a
/// sync/poll when the mode flips to [TransportMode.http].
class SyncManager {
  SyncManager(this._client);

  final SyncClient _client;

  /// Debounce before falling back to HTTP, so a brief blip doesn't flap modes.
  static const Duration httpFallbackDelay = Duration(seconds: 5);

  TransportMode _transportMode = TransportMode.socket;
  bool _isSyncLive = false;
  ConnectionState _rawState = ConnectionState.disconnected;

  Timer? _httpFallbackTimer;
  StreamSubscription<ConnectionState>? _stateSub;
  final StreamController<TransportState> _transportController =
      StreamController<TransportState>.broadcast();

  /// Which transport the sync loop is currently using.
  TransportMode get transportMode => _transportMode;

  /// See [TransportState.isSyncLive].
  bool get isSyncLive => _isSyncLive;

  /// Fires whenever [transportMode] or [isSyncLive] changes.
  Stream<TransportState> get transportChanges => _transportController.stream;

  /// Start driving the auto-switch from the client's connection-state stream.
  /// Idempotent. Safe to call before `connect()` — the initial
  /// `connecting`/`disconnected` states arm the fallback, and a socket that
  /// comes up in time cancels it.
  void attach() {
    _stateSub?.cancel();
    _rawState = _client.connectionState;
    _stateSub = _client.stateChanges.listen(_onTransportStateChanged);
  }

  /// Socket is up and at least one channel joined — enough to run a WS sync.
  bool get _socketUsable =>
      _rawState == ConnectionState.connected &&
      _client.joinedChannels.isNotEmpty;

  void _onTransportStateChanged(ConnectionState state) {
    _rawState = state;

    if (state == ConnectionState.connected) {
      // Socket back: prefer it. isSyncLive stays false until the catch-up sync
      // completes (see syncUnified).
      _clearHttpFallbackTimer();
      _setTransportMode(TransportMode.socket);
      return;
    }

    // Any non-connected state, INCLUDING authFailed: the cache is no longer
    // live; fall back to HTTP after a debounce.
    //
    // authFailed is treated like any other failure on purpose. The inner client
    // flags a single FAST socket failure as authFailed, but in the field that is
    // usually a WS-blocking proxy / captive portal — exactly the case the HTTP
    // fallback exists for. A genuinely bad token 401s over HTTP too (harmless:
    // we stay degraded and not-live, and the token-refresh path still reconnects
    // the socket and flips us back), so attempting HTTP never makes things worse
    // and rescues a network block misread as auth.
    //
    // Arm the timer only ONCE (not clear + re-arm on every transition): a
    // fast-failing socket thrashes connecting→closed→authFailed→disconnected
    // many times a second, which would otherwise reset the debounce forever.
    _setSyncLive(false);
    if (_transportMode != TransportMode.http && _httpFallbackTimer == null) {
      _httpFallbackTimer = Timer(httpFallbackDelay, () {
        _httpFallbackTimer = null;
        if (_rawState != ConnectionState.connected) {
          _setTransportMode(TransportMode.http);
        }
      });
    }
  }

  void _clearHttpFallbackTimer() {
    _httpFallbackTimer?.cancel();
    _httpFallbackTimer = null;
  }

  void _setTransportMode(TransportMode mode) {
    if (_transportMode == mode) return;
    _transportMode = mode;
    _notify();
  }

  void _setSyncLive(bool live) {
    if (_isSyncLive == live) return;
    _isSyncLive = live;
    _notify();
  }

  void _notify() {
    if (_transportController.isClosed) return;
    _transportController
        .add(TransportState(mode: _transportMode, isSyncLive: _isSyncLive));
  }

  /// Run a catch-up sync using whichever transport is current. In
  /// [TransportMode.http] this pushes + paginated-pulls over HTTP ([isSyncLive]
  /// stays false, so consumers keep fetching on demand); on the socket it runs
  /// the WS sync and, once the socket is up with channels joined, marks the
  /// cache live.
  Future<void> syncUnified({List<String>? forceRefresh}) async {
    if (_transportMode == TransportMode.http) {
      await _client.syncUnifiedOverHttp(forceRefresh: forceRefresh);
      return;
    }

    await _client.syncUnified(forceRefresh: forceRefresh);

    if (_transportMode == TransportMode.socket && _socketUsable) {
      _setSyncLive(true);
    }
  }

  /// Consumer-B primitive (by-id): ensure `table`/`id` is available locally
  /// before a screen reads it. No-op when [isSyncLive] (the cache is complete +
  /// fresh); in degraded mode it fetches the row over HTTP as a targeted fetch
  /// (seqnum 0, so it can't inflate the pull cursor). `scope` is `"user"` or
  /// `"tribe"`. Best-effort: a failed on-demand fetch never throws to the
  /// caller.
  Future<void> ensureRow(String table, String id, String topic) async {
    if (_isSyncLive) return;
    try {
      await _client.fetchById(table, id, topic: topic);
    } catch (_) {
      // Best-effort: a failed on-demand fetch must not crash the screen.
    }
  }

  /// [ensureRow] for a set of ids, fetched in one round-trip.
  Future<void> ensureRows(String table, List<String> ids, String topic) async {
    if (_isSyncLive || ids.isEmpty) return;
    try {
      await _client.fetchByIds(table, ids, topic: topic);
    } catch (_) {
      // Best-effort.
    }
  }

  /// Consumer-B primitive (by-page): fetch one seqnum-ordered page of `table`
  /// over HTTP on demand and apply it locally — the paginated companion to
  /// [ensureRow] / [ensureRows]. Loop with `since: page.nextSeqnum` until
  /// `page.hasMore` is false. Unlike [ensureRow], NOT gated on [isSyncLive] — a
  /// caller that explicitly wants a fresh page may pull one even while live.
  Future<PullPage> fetchPage(
    String table, {
    required String topic,
    int since = 0,
    int limit = 500,
  }) {
    return _client.fetchPage(table, topic: topic, since: since, limit: limit);
  }

  /// Update the auth token used by the HTTP fallback (and the next socket
  /// reconnect) without tearing down the socket.
  void updateToken(String token) => _client.updateToken(token);

  /// Cancel the state subscription + any pending fallback timer and close the
  /// transport stream. Does not dispose the wrapped [SyncClient].
  Future<void> dispose() async {
    _clearHttpFallbackTimer();
    await _stateSub?.cancel();
    _stateSub = null;
    await _transportController.close();
  }
}
