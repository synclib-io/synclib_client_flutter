import 'dart:async';
import 'dart:convert';
import 'package:logging/logging.dart';
import 'package:synclib_flutter/synclib_flutter.dart';
import 'package:synchronized/synchronized.dart';
import 'connection/websocket_manager.dart';
import 'protocol/message.dart';
import 'protocol/codec.dart';

/// Sync readiness state (legacy - kept for backwards compatibility)
enum SyncReadyState {
  /// Waiting for initial hello reply from server
  waitingForHello,
  /// Applying schema migrations
  applyingMigrations,
  /// Ready to stream snapshots and sync data
  ready,
}

/// Simplified sync state for the new unified sync handshake
enum SyncState {
  /// Not connected to server
  disconnected,
  /// Connecting to server
  connecting,
  /// Sync operation in progress
  syncing,
  /// Connected and ready
  ready,
  /// Error occurred
  error,
}

/// Progress information during sync operation
class SyncProgress {
  /// Current phase of sync
  final String phase; // 'pushing', 'pulling', 'migrating', 'complete'

  /// Current table being synced (if applicable)
  final String? table;

  /// Row count for current batch (if applicable)
  final int? rowCount;

  /// Number of changes pushed (if applicable)
  final int? changesPushed;

  /// Number of changes acked (if applicable)
  final int? changesAcked;

  const SyncProgress({
    required this.phase,
    this.table,
    this.rowCount,
    this.changesPushed,
    this.changesAcked,
  });

  @override
  String toString() => 'SyncProgress(phase: $phase, table: $table, rowCount: $rowCount)';
}

/// Stats for a single table during sync
class SyncTableStats {
  /// Number of rows pulled from server
  final int rowsPulled;

  /// Number of stripped rows refreshed
  final int strippedRefreshed;

  /// Number of changes pushed to server
  final int changesPushed;

  /// Number of push changes that succeeded
  final int changesSucceeded;

  /// Number of push changes that failed
  final int changesFailed;

  const SyncTableStats({
    this.rowsPulled = 0,
    this.strippedRefreshed = 0,
    this.changesPushed = 0,
    this.changesSucceeded = 0,
    this.changesFailed = 0,
  });

  factory SyncTableStats.fromMap(Map<String, dynamic> map) => SyncTableStats(
    rowsPulled: map['rows'] as int? ?? 0,
    strippedRefreshed: map['stripped'] as int? ?? 0,
    changesPushed: map['total'] as int? ?? 0,
    changesSucceeded: map['success'] as int? ?? 0,
    changesFailed: map['failed'] as int? ?? 0,
  );

  @override
  String toString() => 'SyncTableStats(pulled: $rowsPulled, stripped: $strippedRefreshed, pushed: $changesPushed)';
}

/// Event emitted when a unified sync operation completes
class SyncCompleteEvent {
  /// Stream ID for this sync operation
  final String? streamId;

  /// Final schema version after sync
  final int schemaVersion;

  /// Final per-table seqnums after sync
  final Map<String, int> tableSeqnums;

  /// Whether schema was upgraded during this sync
  final bool schemaUpgraded;

  /// Schema version before upgrade (if upgraded)
  final int? schemaVersionBefore;

  /// Number of schema migrations applied (if upgraded)
  final int migrationsApplied;

  /// Total rows pulled from all tables
  final int totalRowsPulled;

  /// Total changes pushed to server
  final int totalChangesPushed;

  /// Total push changes that succeeded
  final int totalChangesSucceeded;

  /// Total push changes that failed
  final int totalChangesFailed;

  /// Per-table pull stats (rows pulled, stripped refreshed)
  final Map<String, SyncTableStats> pullStats;

  /// Per-table push stats (changes pushed, succeeded, failed)
  final Map<String, SyncTableStats> pushStats;

  /// Tables that had data pulled (non-empty pull)
  List<String> get tablesWithPulledData =>
      pullStats.entries.where((e) => e.value.rowsPulled > 0).map((e) => e.key).toList();

  /// Tables that had changes pushed
  List<String> get tablesWithPushedChanges =>
      pushStats.entries.where((e) => e.value.changesPushed > 0).map((e) => e.key).toList();

  const SyncCompleteEvent({
    this.streamId,
    required this.schemaVersion,
    required this.tableSeqnums,
    this.schemaUpgraded = false,
    this.schemaVersionBefore,
    this.migrationsApplied = 0,
    this.totalRowsPulled = 0,
    this.totalChangesPushed = 0,
    this.totalChangesSucceeded = 0,
    this.totalChangesFailed = 0,
    this.pullStats = const {},
    this.pushStats = const {},
  });

  @override
  String toString() {
    final parts = <String>['SyncCompleteEvent('];
    if (streamId != null) parts.add('stream: $streamId, ');
    parts.add('schema: v$schemaVersion');
    if (schemaUpgraded) parts.add(' (upgraded from v$schemaVersionBefore, $migrationsApplied migrations)');
    if (totalChangesPushed > 0) parts.add(', pushed: $totalChangesSucceeded/$totalChangesPushed');
    if (totalRowsPulled > 0) parts.add(', pulled: $totalRowsPulled rows');
    parts.add(', tables: ${tableSeqnums.keys.toList()})');
    return parts.join();
  }
}

/// Callback for conflict resolution
/// Returns the resolved change, or null to skip
typedef ConflictResolver = Future<ChangeMessage?> Function(
  ChangeMessage local,
  ChangeMessage remote,
);

/// Event emitted when a snapshot stream completes
class SnapshotCompleteEvent {
  final String streamId;
  final String channelId;

  const SnapshotCompleteEvent({
    required this.streamId,
    required this.channelId,
  });
}

/// Event emitted when a snapshot batch is received and processed
class SnapshotBatchEvent {
  final String streamId;
  final String table;
  final int rowCount;

  const SnapshotBatchEvent({
    required this.streamId,
    required this.table,
    required this.rowCount,
  });
}

/// Event emitted when starting to request a snapshot for tables
class SnapshotRequestEvent {
  final List<String> tables;
  final bool incremental;

  const SnapshotRequestEvent({
    required this.tables,
    required this.incremental,
  });
}

/// Info about a single stale table
class StaleTableInfo {
  final String table;
  final int behindBy;
  final int serverSeqnum;

  const StaleTableInfo({
    required this.table,
    required this.behindBy,
    required this.serverSeqnum,
  });

  factory StaleTableInfo.fromMap(Map<String, dynamic> map) {
    return StaleTableInfo(
      table: map['table'] as String,
      behindBy: map['behind_by'] as int,
      serverSeqnum: map['server_seqnum'] as int,
    );
  }
}

/// State of auto-sync operation
enum AutoSyncState {
  /// No stale tables detected or auto-sync not configured
  idle,
  /// Stale tables detected, sync in progress
  syncing,
  /// Auto-sync completed successfully
  completed,
  /// Auto-sync failed
  failed,
}

/// Event emitted when auto-sync state changes
/// Contains both the discovery (what's stale) and progress (syncing/completed)
class AutoSyncEvent {
  final AutoSyncState state;
  final List<StaleTableInfo> staleTables;
  final String? channelTopic;
  final String? error;

  const AutoSyncEvent({
    required this.state,
    this.staleTables = const [],
    this.channelTopic,
    this.error,
  });

  /// Convenience getter for just the table names
  List<String> get tableNames => staleTables.map((t) => t.table).toList();
}

class SyncClientChannel {
  final String channelName;
  final String channelId;
  final Map<String, String>? params;
  const SyncClientChannel({
    required this.channelName,
    required this.channelId,
    this.params
  });
}

/// Tracks the sync state of an individual channel
class ChannelSyncState {
  final String topic;
  bool joined;
  bool autoSyncInProgress;
  bool autoSyncComplete;
  Completer<void>? completer;
  List<StaleTableInfo> staleTables;

  ChannelSyncState({
    required this.topic,
    this.joined = false,
    this.autoSyncInProgress = false,
    this.autoSyncComplete = false,
    this.completer,
    this.staleTables = const [],
  });

  /// Whether this channel needs auto-sync (has stale tables)
  bool get needsAutoSync => staleTables.isNotEmpty;

  /// Whether this channel is fully ready (joined and sync complete if needed)
  bool get isReady => joined && (!needsAutoSync || autoSyncComplete);
}

/// Configuration for sync client
class SyncClientConfig {
  /// Path to local SQLite database
  final String dbPath;

  /// WebSocket server URL
  final String serverUrl;

  /// Unique client identifier
  final String clientId;

  /// channels to connect to initially
  // todo allow connecting to any channel dynamically later
  final List<SyncClientChannel> initialChannels;

  /// Codec for message encoding
  final SyncCodecType codec;

  /// How often to push local changes (null = manual only)
  final Duration? pushInterval;

  /// Batch size for pushing changes
  final int pushBatchSize;

  /// How often to pull remote changes (null = reactive only)
  final Duration? pullInterval;

  /// Conflict resolution strategy
  final ConflictResolver? onConflict;

  /// Optional metadata to send in hello message
  final Map<String, dynamic>? metadata;

  /// Channel to use for broadcasting/pushing messages (e.g., "sync:user:123")
  /// If not specified, uses the first channel
  final String? broadcastChannel;

  /// Channel to use for pulling remote changes (e.g., "sync:tribe:trainer123")
  /// If not specified, uses broadcastChannel (or first channel if that's also null).
  /// This allows pulling from a different channel than you push to.
  /// Example: Members pull from tribe channel (trainer content) but push to user channel.
  final String? pullChannel;

  /// Whether to pull remote changes periodically.
  /// If false, only pushes local changes. Defaults to true.
  final bool pullRemote;

  /// Whether to push local changes periodically.
  /// If false, only pulls remote changes. Defaults to true.
  final bool pushLocal;

  /// Whether to enable periodic sync at all.
  /// If false, no timers are started - sync is fully reactive (WebSocket events only).
  /// Defaults to true.
  final bool enablePeriodicSync;

  /// Whether to automatically push changes immediately when writes occur.
  /// If true, subscribes to database localChanges stream and pushes on each write.
  /// Uses debouncing to batch rapid writes (default 100ms debounce).
  /// Defaults to false for backward compatibility.
  final bool syncOnWrite;

  /// Debounce duration for syncOnWrite. Batches rapid writes together.
  /// Only used when syncOnWrite is true.
  /// Defaults to 100ms.
  final Duration syncOnWriteDebounce;

  /// Optional pre-opened database instance.
  /// If provided, SyncClient will use this instead of opening a new connection.
  /// This is useful when you want to share a database connection with other
  /// parts of your app (e.g., a UI layer that also reads from the database).
  final SynclibDatabase? database;

  /// Tables to auto-sync on connect.
  /// When specified, the client will send per-table seqnums to the server
  /// on channel join. The server reports which tables are stale, and the
  /// client automatically syncs them.
  /// Listen to [SyncClient.autoSyncEvents] for progress updates.
  /// If empty or null, auto-sync is disabled.
  final List<String>? autoSyncTables;

  /// Whether to automatically sync stale tables on connect.
  /// If false, auto-sync can be triggered manually via [SyncClient.runAutoSync()].
  /// Defaults to true.
  final bool autoSyncOnConnect;

  /// Whether to use the new unified sync mechanism.
  /// When true (default):
  /// - Periodic push/pull timers are disabled
  /// - syncOnWrite calls syncUnified() instead of legacy _pushLocalChanges()
  /// - All sync happens via the single syncUnified() call which handles:
  ///   - Push: Send pending local changes
  ///   - Pull: Get remote changes (incremental by seqnum)
  ///   - Schema: Apply migrations if needed
  ///   - Stripped: Refresh stripped content
  /// When false, uses legacy periodic push/pull timers.
  /// Defaults to true for new clients.
  final bool useUnifiedSync;

  const SyncClientConfig({
    required this.dbPath,
    required this.serverUrl,
    required this.clientId,
    required this.initialChannels,
    this.codec = SyncCodecType.json,
    this.pushInterval = const Duration(seconds: 5),
    this.pushBatchSize = 100,
    this.pullInterval,
    this.onConflict,
    this.metadata,
    this.broadcastChannel,
    this.pullChannel,
    this.pullRemote = true,
    this.pushLocal = true,
    this.enablePeriodicSync = true,
    this.syncOnWrite = false,
    this.syncOnWriteDebounce = const Duration(milliseconds: 100),
    this.database,
    this.autoSyncTables,
    this.autoSyncOnConnect = true,
    this.useUnifiedSync = true,
  });
}

/// Main sync client orchestrating bidirectional sync
class SyncClient {
  final SyncClientConfig config;
  final Logger _logger = Logger('SyncClient');

  late final WebSocketManager _ws;
  SynclibDatabase? _db;
  Timer? _pushTimer;
  Timer? _pullTimer;
  StreamSubscription? _messageSubscription;
  StreamSubscription? _stateSubscription;

  bool _isInitialized = false;
  bool _hasConnectedOnce = false;
  int _lastSyncedSeqnum = 0;
  final Set<int> _pendingAcks = {};
  /// Track pending changes by local seqnum so we can update server seqnum on ack.
  /// Key is local seqnum from _synclib_changes, value is (table, rowId).
  final Map<int, ({String table, String rowId})> _pendingChangeInfo = {};

  // Store channel subscription params for reconnection
  String? _token;
  bool _joinWorld = false;
  List<String>? _zones;
  List<String>? _guilds;
  List<String>? _parties;

  // Control whether periodic sync pulls from remote (initialized from config)
  late bool _pullRemoteEnabled;
  // Control whether periodic sync pushes local changes (initialized from config)
  late bool _pushLocalEnabled;

  // Stream controller for remote change notifications
  final StreamController<ChangeMessage> _remoteChangeController = StreamController<ChangeMessage>.broadcast();

  // Stream controller for snapshot complete events
  final StreamController<SnapshotCompleteEvent> _snapshotCompleteController = StreamController<SnapshotCompleteEvent>.broadcast();

  // Stream controller for snapshot batch events (per-table progress)
  final StreamController<SnapshotBatchEvent> _snapshotBatchController = StreamController<SnapshotBatchEvent>.broadcast();

  // Stream controller for snapshot request events (when tables are requested)
  final StreamController<SnapshotRequestEvent> _snapshotRequestController = StreamController<SnapshotRequestEvent>.broadcast();

  // Stream controller for job update events
  final StreamController<JobUpdateMessage> _jobUpdateController = StreamController<JobUpdateMessage>.broadcast();

  // Stream controller for livestream events
  final StreamController<LivestreamMessage> _livestreamController = StreamController<LivestreamMessage>.broadcast();

  // Stream controller for conversation events
  final StreamController<ConversationMessage> _conversationController = StreamController<ConversationMessage>.broadcast();

  // Stream controller for presence events (video/livestream viewers)
  final StreamController<PresenceMessage> _presenceController = StreamController<PresenceMessage>.broadcast();

  // Stream controller for feed status events (new videos, online count)
  final StreamController<FeedStatusMessage> _feedStatusController = StreamController<FeedStatusMessage>.broadcast();

  // Stream controller for interaction events (likes, comments, comment likes)
  final StreamController<InteractionMessage> _interactionController = StreamController<InteractionMessage>.broadcast();

  // Stream controller for direct stream events (participant changes, segment availability)
  final StreamController<DirectStreamMessage> _directStreamController = StreamController<DirectStreamMessage>.broadcast();

  // Stream controller for sync ready state changes
  final StreamController<SyncReadyState> _syncReadyStateController = StreamController<SyncReadyState>.broadcast();

  // Stream controller for auto-sync events (stale tables detection + sync progress)
  final StreamController<AutoSyncEvent> _autoSyncController = StreamController<AutoSyncEvent>.broadcast();

  // Stream controller for new simplified sync state
  final StreamController<SyncState> _syncStateController = StreamController<SyncState>.broadcast();

  // Stream controller for sync progress events
  final StreamController<SyncProgress> _syncProgressController = StreamController<SyncProgress>.broadcast();

  // Stream controller for unified sync completion events
  final StreamController<SyncCompleteEvent> _syncCompleteController = StreamController<SyncCompleteEvent>.broadcast();

  // Current sync state
  SyncState _syncState = SyncState.disconnected;

  // Whether a sync operation is currently in progress
  bool _isSyncing = false;

  // Track per-channel sync state
  final Map<String, ChannelSyncState> _channelStates = {};

  // Store join responses for deferred auto-sync
  Map<String, Map<String, dynamic>> _pendingJoinResponses = {};

  // Lock to ensure only one batch operation happens at a time
  final Lock _batchLock = Lock();
  final List<SnapshotBatchMessage> _batchQueue = [];

  // Unified sync: track active sync operations by stream_id
  final Map<String, Completer<void>> _activeSyncCompleters = {};
  final Map<String, List<Change>> _activeSyncPendingChanges = {};

  // syncOnWrite: subscription to local changes and debounce timer
  StreamSubscription? _localChangeSubscription;
  Timer? _syncOnWriteDebounceTimer;

  SyncReadyState _readyState = SyncReadyState.waitingForHello;

  SyncClient(this.config) {
    _pullRemoteEnabled = config.pullRemote;
    _pushLocalEnabled = config.pushLocal;
    _ws = WebSocketManager(
      url: config.serverUrl,
      codec: SyncCodecFactory.create(config.codec),
    );
  }

  /// Initialize the sync client
  Future<void> initialize() async {
    if (_isInitialized) {
      _logger.warning('Already initialized');
      return;
    }

    _logger.info('Initializing sync client');

    // Use provided database or open a new one
    if (config.database != null) {
      _db = config.database;
      _logger.info('Using provided database instance');
    } else {
      _db = await SynclibDatabase.open(config.dbPath);
      _logger.info('Database opened: ${config.dbPath}');
    }

    // Subscribe to WebSocket messages
    _messageSubscription = _ws.messages.listen(_handleMessage);
    _stateSubscription = _ws.stateChanges.listen(_handleStateChange);

    // Subscribe to local changes for syncOnWrite
    if (config.syncOnWrite) {
      _logger.info('syncOnWrite enabled - subscribing to local changes');
      _localChangeSubscription = _db!.localChanges.listen((_) {
        _onLocalChange();
      });
    }

    _isInitialized = true;
  }

  /// Called when a local change occurs (for syncOnWrite).
  /// Uses debouncing to batch rapid writes together.
  void _onLocalChange() {
    if (!_ws.isConnected) return;

    // Cancel any existing debounce timer
    _syncOnWriteDebounceTimer?.cancel();

    // Start a new debounce timer
    _syncOnWriteDebounceTimer = Timer(config.syncOnWriteDebounce, () {
      _logger.fine('syncOnWrite: pushing changes after debounce');
      if (config.useUnifiedSync) {
        // Use unified sync - handles push, pull, and cleanup in one call
        syncUnified().catchError((e) {
          _logger.warning('syncOnWrite: syncUnified failed: $e');
        });
      } else {
        // Legacy: just push local changes
        _pushLocalChanges();
      }
    });
  }

  /// Connect to sync server
  ///
  /// [token] - Authentication token (JWT) required by the server
  ///
  /// Optionally provide additional channels to subscribe to:
  // should connect to what is passed in to config for possible channels
  Future<void> connect({
    required String token,
    bool joinWorld = false,
    List<String>? zones,
    List<String>? guilds,
    List<String>? parties,
  }) async {
    if (!_isInitialized) {
      throw StateError('Not initialized. Call initialize() first.');
    }

    // Only disconnect if actually connected or connecting
    // This prevents phoenix_socket's "_joinedOnce" assertion error on reconnect
    // but avoids disrupting operations if we're already disconnected
    if (_ws.isConnected || _ws.state == ConnectionState.connecting) {
      _logger.info('Already connected/connecting (state: ${_ws.state}), disconnecting first...');
      await disconnect();
    }

    // Store connection params for reconnection
    _token = token;
    _joinWorld = joinWorld;
    _zones = zones;
    _guilds = guilds;
    _parties = parties;

    await _ws.connect(
      params: {
        'token': token,
        'client_id': config.clientId,
      },
    );
    await _joinChannels();
  }

  /// Join all configured channels
  // todo use initial channels
  Future<void> _joinChannels() async {
    // Clear channel states for fresh connection
    _channelStates.clear();

    // Get per-table seqnums if auto-sync is configured
    Map<String, int>? tableSeqnums;
    if (config.autoSyncTables != null && config.autoSyncTables!.isNotEmpty) {
      tableSeqnums = await _getPerTableSeqnums(config.autoSyncTables!);
      _logger.info('Sending table seqnums on join: $tableSeqnums');
    }

    // First, join ALL channels (don't wait for auto-sync)
    // This prevents timeouts from one channel's sync blocking the next join
    final joinResponses = <String, Map<String, dynamic>>{};

    for (final channel in config.initialChannels) {
      final topic = 'sync:${channel.channelName}:${channel.channelId}';
      _logger.info('Joining channel: $topic');

      // Create channel state
      _channelStates[topic] = ChannelSyncState(topic: topic);

      final joinParams = {
        'client_id': config.clientId,
        ...?channel.params,
        if (tableSeqnums != null) 'table_seqnums': tableSeqnums,
      };

      final response = await _ws.joinChannel(topic, joinParams);
      _logger.info('Channel join response: $response');

      // Mark channel as joined
      _channelStates[topic]!.joined = true;
      joinResponses[topic] = response;
    }

    _logger.info('All channels joined successfully');
    _hasConnectedOnce = true;
    // Only start periodic timers if NOT using unified sync
    // Unified sync handles push/pull in a single call, no timers needed
    if (config.enablePeriodicSync && !config.useUnifiedSync) {
      startPeriodicSync(pullRemote: _pullRemoteEnabled, pushLocal: _pushLocalEnabled);
    }
    await _sendHello();

    // Store join responses for potential later auto-sync
    _pendingJoinResponses = joinResponses;

    // Handle stale tables if auto-sync on connect is enabled
    if (config.autoSyncOnConnect) {
      _logger.info('Auto-sync on connect enabled, processing stale tables...');
      for (final entry in joinResponses.entries) {
        await _handleJoinResponse(entry.value, entry.key);
      }
    } else {
      _logger.info('Auto-sync on connect disabled, marking channels as ready');
      // Mark all channels as ready (no auto-sync needed)
      for (final channelState in _channelStates.values) {
        channelState.autoSyncComplete = true;
      }
    }
  }

  /// Handle join response from server, including stale tables
  Future<void> _handleJoinResponse(Map<String, dynamic> response, String channelTopic) async {
    final channelState = _channelStates[channelTopic];

    // Skip if auto-sync not configured - mark channel as ready
    if (config.autoSyncTables == null || config.autoSyncTables!.isEmpty) {
      if (channelState != null) {
        channelState.autoSyncComplete = true;
      }
      return;
    }

    final staleTablesRaw = response['stale_tables'];
    if (staleTablesRaw == null || staleTablesRaw is! List || staleTablesRaw.isEmpty) {
      // No stale tables - mark channel as ready
      if (channelState != null) {
        channelState.autoSyncComplete = true;
      }
      return;
    }

    final staleTables = staleTablesRaw
        .map((t) => StaleTableInfo.fromMap(Map<String, dynamic>.from(t as Map)))
        .toList();

    _logger.info('Server reports ${staleTables.length} stale table(s): ${staleTables.map((t) => '${t.table}(behind by ${t.behindBy})').join(', ')}');

    // Start auto-sync
    final tableNames = staleTables.map((t) => t.table).toList();
    _startAutoSync(staleTables, channelTopic);

    // Use incremental sync since we already have some data
    await streamSnapshot(
      tableNames,
      incremental: true,
      channelTopic: channelTopic,
      waitForReconnect: false, // We're already connected
    );
  }

  /// Start tracking auto-sync operation for a specific channel
  void _startAutoSync(List<StaleTableInfo> staleTables, String channelTopic) {
    final channelState = _channelStates[channelTopic];
    if (channelState == null) {
      _logger.warning('Cannot start auto-sync: channel $channelTopic not found in state map');
      return;
    }

    channelState.staleTables = staleTables;
    channelState.autoSyncInProgress = true;
    channelState.autoSyncComplete = false;
    channelState.completer = Completer<void>();

    _autoSyncController.add(AutoSyncEvent(
      state: AutoSyncState.syncing,
      staleTables: staleTables,
      channelTopic: channelTopic,
    ));

    _logger.info('Auto-sync started for ${staleTables.length} table(s) on $channelTopic');
  }

  /// Called when snapshot stream completes (from snapshot complete handler)
  void _onSnapshotStreamComplete(String channelId) {
    // Find the channel that matches this channelId
    // channelId format: "sync:tribe:xyz" or "sync:user:abc"
    for (final entry in _channelStates.entries) {
      if (entry.value.autoSyncInProgress && channelId.contains(entry.key)) {
        _completeAutoSync(entry.key);
        return;
      }
    }
  }

  /// Complete the auto-sync operation for a specific channel
  void _completeAutoSync(String channelTopic) {
    final channelState = _channelStates[channelTopic];
    if (channelState == null) {
      _logger.warning('Cannot complete auto-sync: channel $channelTopic not found');
      return;
    }

    final staleTables = channelState.staleTables;
    channelState.autoSyncInProgress = false;
    channelState.autoSyncComplete = true;

    _autoSyncController.add(AutoSyncEvent(
      state: AutoSyncState.completed,
      staleTables: staleTables,
      channelTopic: channelTopic,
    ));

    if (channelState.completer != null && !channelState.completer!.isCompleted) {
      channelState.completer!.complete();
    }

    _logger.info('Auto-sync completed for ${staleTables.length} table(s) on $channelTopic');

    // Check if all channels are now synced, and if so, re-emit ready state
    if (allChannelsSynced && _readyState == SyncReadyState.ready) {
      // Small delay to ensure state is consistent
      Future.delayed(const Duration(milliseconds: 100), () {
        if (allChannelsSynced && _readyState == SyncReadyState.ready) {
          _syncReadyStateController.add(SyncReadyState.ready);
          _logger.info('All channels synced - re-emitting ready state');
        }
      });
    }
  }

  /// Fail the auto-sync operation for a specific channel
  void _failAutoSync(String channelTopic, String error) {
    final channelState = _channelStates[channelTopic];
    if (channelState == null) {
      _logger.warning('Cannot fail auto-sync: channel $channelTopic not found');
      return;
    }

    channelState.autoSyncInProgress = false;
    channelState.autoSyncComplete = false; // Failed, not complete

    _autoSyncController.add(AutoSyncEvent(
      state: AutoSyncState.failed,
      staleTables: channelState.staleTables,
      channelTopic: channelTopic,
      error: error,
    ));

    if (channelState.completer != null && !channelState.completer!.isCompleted) {
      channelState.completer!.completeError(Exception(error));
    }

    _logger.warning('Auto-sync failed for $channelTopic: $error');
  }

  /// Manually trigger auto-sync for stale tables.
  /// Use this when [SyncClientConfig.autoSyncOnConnect] is false
  /// and you want to run auto-sync after other initialization steps.
  ///
  /// Returns immediately if auto-sync is already in progress or
  /// if there are no pending join responses to process.
  Future<void> runAutoSync() async {
    if (!_ws.isConnected) {
      throw StateError('Not connected. Call connect() first.');
    }

    if (_pendingJoinResponses.isEmpty) {
      _logger.info('No pending join responses for auto-sync');
      return;
    }

    // Check if any channel is already syncing
    if (isAutoSyncing) {
      _logger.info('Auto-sync already in progress');
      return;
    }

    _logger.info('Running manual auto-sync for ${_pendingJoinResponses.length} channel(s)');

    // Reset channel sync states before running
    for (final channelState in _channelStates.values) {
      channelState.autoSyncComplete = false;
    }

    // Process stored join responses
    for (final entry in _pendingJoinResponses.entries) {
      await _handleJoinResponse(entry.value, entry.key);
    }

    // Clear pending responses after processing
    _pendingJoinResponses = {};
  }

  /// Join an additional channel after initial connection
  ///
  /// Optionally provide [autoSyncTables] to check and auto-sync specific tables.
  /// If stale tables are found, they are automatically synced and an
  /// [AutoSyncEvent] is emitted on [autoSyncEvents].
  ///
  /// Example:
  /// ```dart
  /// await syncClient.joinChannel(
  ///   SyncClientChannel(
  ///     channelName: 'user',
  ///     channelId: userId,
  ///   ),
  ///   autoSyncTables: ['videos', 'users'],
  /// );
  /// ```
  Future<void> joinChannel(
    SyncClientChannel channel, {
    List<String>? autoSyncTables,
  }) async {
    if (!_ws.isConnected) {
      throw StateError('Not connected. Call connect() first.');
    }

    final topic = 'sync:${channel.channelName}:${channel.channelId}';
    _logger.info('Joining additional channel: $topic');

    // Create channel state for this dynamic channel
    _channelStates[topic] = ChannelSyncState(topic: topic);

    // Get per-table seqnums if auto-sync tables are specified
    Map<String, int>? tableSeqnums;
    if (autoSyncTables != null && autoSyncTables.isNotEmpty) {
      tableSeqnums = await _getPerTableSeqnums(autoSyncTables);
      _logger.info('Sending table seqnums on join: $tableSeqnums');
    }

    final response = await _ws.joinChannel(topic, {
      'client_id': config.clientId,
      ...?channel.params,
      if (tableSeqnums != null) 'table_seqnums': tableSeqnums,
    });

    // Mark channel as joined
    _channelStates[topic]!.joined = true;
    _logger.info('Successfully joined channel: $topic');

    // Handle stale tables from server response (auto-sync if tables specified)
    await _handleJoinResponseDynamic(response, topic, autoSyncTables);
  }

  /// Handle join response for dynamically joined channels
  Future<void> _handleJoinResponseDynamic(
    Map<String, dynamic> response,
    String channelTopic,
    List<String>? autoSyncTables,
  ) async {
    final channelState = _channelStates[channelTopic];

    // Skip if auto-sync not requested - mark channel as ready
    if (autoSyncTables == null || autoSyncTables.isEmpty) {
      if (channelState != null) {
        channelState.autoSyncComplete = true;
      }
      return;
    }

    final staleTablesRaw = response['stale_tables'];
    if (staleTablesRaw == null || staleTablesRaw is! List || staleTablesRaw.isEmpty) {
      // No stale tables - mark channel as ready
      if (channelState != null) {
        channelState.autoSyncComplete = true;
      }
      return;
    }

    final staleTables = staleTablesRaw
        .map((t) => StaleTableInfo.fromMap(Map<String, dynamic>.from(t as Map)))
        .toList();

    _logger.info('Server reports ${staleTables.length} stale table(s): ${staleTables.map((t) => '${t.table}(behind by ${t.behindBy})').join(', ')}');

    // Start auto-sync
    final tableNames = staleTables.map((t) => t.table).toList();
    _startAutoSync(staleTables, channelTopic);

    await streamSnapshot(
      tableNames,
      incremental: true,
      channelTopic: channelTopic,
      waitForReconnect: false,
    );
  }

  /// Check if a channel is currently joined
  ///
  /// Example:
  /// ```dart
  /// if (syncClient.isChannelJoined(
  ///   SyncClientChannel(channelName: 'user', channelId: userId),
  /// )) {
  ///   _logger.info('Already joined');
  /// }
  /// ```
  bool isChannelJoined(SyncClientChannel channel) {
    final topic = 'sync:${channel.channelName}:${channel.channelId}';
    return _ws.isChannelJoined(topic);
  }

  /// Get all currently joined channel topics
  List<String> get joinedChannels => _ws.joinedChannels;

  /// Leave a channel
  ///
  /// Example:
  /// ```dart
  /// await syncClient.leaveChannel(
  ///   SyncClientChannel(
  ///     channelName: 'user',
  ///     channelId: 'anon',
  ///   ),
  /// );
  /// ```
  Future<void> leaveChannel(SyncClientChannel channel) async {
    final topic = 'sync:${channel.channelName}:${channel.channelId}';
    _logger.info('Leaving channel: $topic');
    await _ws.leaveChannel(topic);
    _logger.info('Left channel: $topic');
  }

  /// Leave a channel by its topic ID (e.g., "sync:user:123")
  ///
  /// Example:
  /// ```dart
  /// await syncClient.leaveChannelById('sync:user:123');
  /// ```
  Future<void> leaveChannelById(String channelId) async {
    _logger.info('Leaving channel: $channelId');
    await _ws.leaveChannel(channelId);
    _logger.info('Left channel: $channelId');
  }

  /// Join a channel by its full topic (e.g., "grid:lobby:abc123")
  ///
  /// Use this for channels that don't follow the sync:{name}:{id} pattern.
  ///
  /// Example:
  /// ```dart
  /// await syncClient.joinChannelByTopic('grid:lobby:abc123');
  /// ```
  Future<void> joinChannelByTopic(String topic, {Map<String, dynamic>? params}) async {
    if (!_ws.isConnected) {
      throw StateError('Not connected. Call connect() first.');
    }

    _logger.info('Joining channel by topic: $topic');

    await _ws.joinChannel(topic, {
      'client_id': config.clientId,
      ...?params,
    });

    _logger.info('Successfully joined channel: $topic');
  }

  /// Disconnect from sync server
  Future<void> disconnect() async {
    _stopPeriodicSync();
    await _ws.disconnect();
  }

  /// Wait for connection to be established, triggering reconnect if needed.
  ///
  /// This is useful when you want to ensure the client is connected before
  /// sending messages, rather than immediately failing with "Not connected".
  ///
  /// Throws an exception if reconnection fails or times out.
  Future<void> _waitForConnection({Duration timeout = const Duration(seconds: 30)}) async {
    if (_ws.isConnected) return;

    // Trigger reconnect if not already reconnecting
    if (_ws.state == ConnectionState.disconnected || _ws.state == ConnectionState.failed) {
      _logger.info('Not connected, triggering reconnect...');
      // Don't await connect() - it may return before actually connected
      _ws.connect();
    }

    // If already reconnecting, just wait
    if (_ws.state == ConnectionState.reconnecting || _ws.state == ConnectionState.connecting) {
      _logger.info('Reconnection in progress, waiting...');
    }

    // Wait for connected state
    final completer = Completer<void>();
    StreamSubscription<ConnectionState>? sub;

    sub = _ws.stateChanges.listen((state) {
      if (state == ConnectionState.connected) {
        sub?.cancel();
        if (!completer.isCompleted) completer.complete();
      } else if (state == ConnectionState.failed || state == ConnectionState.authFailed) {
        sub?.cancel();
        if (!completer.isCompleted) {
          completer.completeError(Exception('Reconnection failed: $state'));
        }
      }
    });

    try {
      await completer.future.timeout(timeout, onTimeout: () {
        throw TimeoutException('Timed out waiting for reconnection', timeout);
      });
    } finally {
      await sub?.cancel();
    }
  }

  Future<void> syncOverTable(String table) async {
    await _pushLocalChanges();
    await _pullRemoteChangesForTable(table);
  }

  /// Manually trigger a sync cycle
  ///
  /// When useUnifiedSync is enabled (default), this calls syncUnified() which
  /// handles push, pull, schema, and stripped content in one request.
  /// Otherwise falls back to the legacy syncSafe() push/pull flow.
  Future<void> sync() async {
    if (config.useUnifiedSync) {
      await syncUnified();
    } else {
      await syncSafe();
    }
  }

  /// Push local changes to server (public API)
  ///
  /// Use this when you want to ensure local changes are sent to the server
  /// without pulling remote changes that might overwrite pending edits.
  Future<void> push() async {
    await _pushLocalChanges();
  }

  /// Safely sync by waiting for push acks before pulling
  ///
  /// This prevents race conditions where you might pull stale data
  /// before your local changes have been processed by the server.
  Future<void> syncSafe({Duration timeout = const Duration(seconds: 10)}) async {
    await _pushLocalChanges();

    // Wait for all pending acks before pulling
    if (_pendingAcks.isNotEmpty) {
      final startTime = DateTime.now();
      while (_pendingAcks.isNotEmpty) {
        if (DateTime.now().difference(startTime) > timeout) {
          _logger.warning('syncSafe: Timeout waiting for acks, ${_pendingAcks.length} still pending');
          break;
        }
        await Future.delayed(const Duration(milliseconds: 50));
      }
    }

    await _pullRemoteChanges();
  }

  // ==========================================================================
  // SIMPLIFIED SYNC HANDSHAKE - New unified sync implementation
  // ==========================================================================

  /// Find all rows that have _stripped=true in their document
  /// These rows need to be refreshed to get full content
  Future<List<RowRef>> _findStrippedRows(List<String> tables) async {
    final rows = <RowRef>[];

    for (final table in tables) {
      try {
        // First check how many rows exist in the table
        final countResult = await _db!.read('SELECT COUNT(*) as cnt FROM $table');
        final totalRows = countResult.isNotEmpty ? countResult.first['cnt'] as int : 0;

        // Check for NULL documents specifically
        // Note: JSONB columns store null as binary bytes, not SQL NULL
        // So we check both SQL NULL and json(document) = 'null'
        final nullDocResult = await _db!.read('''
          SELECT COUNT(*) as cnt FROM $table
          WHERE document IS NULL
             OR json(document) IS NULL
             OR json(document) = 'null'
        ''');
        final nullDocCount = nullDocResult.isNotEmpty ? nullDocResult.first['cnt'] as int : 0;

        // Stripped content has NULL/null document OR _stripped flag in document
        final result = await _db!.read('''
          SELECT id FROM $table
          WHERE document IS NULL
             OR json(document) IS NULL
             OR json(document) = 'null'
             OR json_extract(document, '\$._stripped') = 1
             OR json_extract(document, '\$._stripped') = true
        ''');

        for (final row in result) {
          rows.add(RowRef(table: table, rowId: row['id'] as String));
        }

        if (result.isNotEmpty) {
          _logger.info('Found ${result.length} stripped rows in $table (total rows: $totalRows)');
        } else if (nullDocCount > 0) {
          _logger.warning('$table has $nullDocCount rows with NULL document but query returned 0 stripped rows!');
        }
      } catch (e, stack) {
        // Log at warning level so we can see errors
        _logger.warning('Could not check stripped rows in $table: $e');
        _logger.fine('Stack trace: $stack');
      }
    }

    return rows;
  }

  /// Detect tables with corrupted JSONB storage (stored as TEXT instead of BLOB)
  /// Returns list of tables that need force refresh to fix corruption
  Future<List<String>> _detectCorruptedJsonbTables(List<String> tables) async {
    final corruptedTables = <String>[];

    for (final table in tables) {
      try {
        // Check if any documents are stored as TEXT instead of BLOB
        // Proper JSONB is stored as BLOB, corrupted data is stored as TEXT
        final result = await _db!.read('''
          SELECT COUNT(*) as cnt FROM $table
          WHERE document IS NOT NULL
            AND typeof(document) = 'text'
        ''');

        final corruptedCount = result.isNotEmpty ? result.first['cnt'] as int : 0;

        if (corruptedCount > 0) {
          _logger.warning('JSONB CORRUPTION DETECTED: $table has $corruptedCount rows with document stored as TEXT instead of BLOB');
          corruptedTables.add(table);
        }
      } catch (e) {
        // Table might not have document column - ignore
        _logger.fine('Could not check JSONB corruption in $table: $e');
      }
    }

    if (corruptedTables.isNotEmpty) {
      _logger.warning('Tables with corrupted JSONB will be force-refreshed: $corruptedTables');
    }

    return corruptedTables;
  }

  /// Update sync state and broadcast change
  void _updateSyncState(SyncState newState) {
    if (_syncState != newState) {
      _syncState = newState;
      _syncStateController.add(newState);
      _logger.info('Sync state changed to: $newState');
    }
  }

  /// Emit sync progress event
  void _emitSyncProgress(SyncProgress progress) {
    _syncProgressController.add(progress);
    _logger.fine('Sync progress: $progress');
  }

  /// The ONE unified sync method - handles push, pull, schema, and stripped content
  ///
  /// This is the simplified sync handshake that replaces the multi-step flow
  /// of sendHello(), requestSnapshots(), runAutoSync(), etc.
  ///
  /// [tables] - Specific tables to sync (null = all configured in autoSyncTables)
  /// [forceRefresh] - Tables to force refresh (ignore seqnums)
  /// [includeStripped] - Auto-detect and refresh stripped rows (default: true)
  /// [channelTopic] - Channel to sync on (default: broadcastChannel)
  /// [cleanupLegacyChanges] - Clean up old synced=1 records from legacy sync (default: true on first run)
  Future<void> syncUnified({
    List<String>? tables,
    List<String>? forceRefresh,
    bool includeStripped = true,
    String? channelTopic,
    bool? cleanupLegacyChanges,
  }) async {
    if (!_ws.isConnected) {
      _logger.warning('syncUnified: Not connected, waiting for connection...');
      await _waitForConnection();
    }

    if (_isSyncing) {
      _logger.info('syncUnified: Already syncing, skipping');
      return;
    }

    _isSyncing = true;
    _updateSyncState(SyncState.syncing);

    try {
      // Backwards compatibility: Clean up old synced=1 records from legacy sync
      // These records were marked synced but never deleted, causing table growth
      if (cleanupLegacyChanges ?? !_hasCleanedUpLegacyChanges) {
        await _cleanupLegacySyncedChanges();
        _hasCleanedUpLegacyChanges = true;
      }

      // Determine which tables to sync
      final tablesToSync = tables ?? config.autoSyncTables ?? [];
      if (tablesToSync.isEmpty) {
        _logger.warning('syncUnified: No tables configured for sync');
        _isSyncing = false;
        _updateSyncState(SyncState.ready);
        return;
      }

      // 1. Get current schema version first
      final schemaVersion = await _db!.getSchemaVersion();

      // 2. Get pending local changes to push
      _emitSyncProgress(const SyncProgress(phase: 'pushing'));
      final pendingChanges = await _db!.getPendingChanges(limit: config.pushBatchSize);
      _logger.info('syncUnified: ${pendingChanges.length} pending changes to push');

      // Convert to PendingChange format
      final pendingChangeMessages = pendingChanges.map((c) => PendingChange(
        localSeqnum: c.seqnum,
        table: c.tableName,
        rowId: c.rowId,
        operation: c.operation.name,
        data: c.data != null ? _parseJson(c.data!) : null,
      )).toList();

      // 3. Get per-table seqnums for incremental pull
      final tableSeqnums = await _getPerTableSeqnums(tablesToSync);
      _logger.info('syncUnified: Table seqnums: $tableSeqnums');

      // 4. Find stripped rows if needed
      List<RowRef>? strippedRows;
      if (includeStripped) {
        _logger.info('syncUnified: Checking for stripped rows in tables: $tablesToSync');
        strippedRows = await _findStrippedRows(tablesToSync);
        if (strippedRows.isNotEmpty) {
          _logger.info('syncUnified: Found ${strippedRows.length} stripped rows to refresh: ${strippedRows.map((r) => '${r.table}:${r.rowId}').join(', ')}');
        } else {
          _logger.info('syncUnified: No stripped rows found');
        }
      }

      // 4b. Detect JSONB corruption (documents stored as TEXT instead of BLOB)
      // If corruption found, auto-add to forceRefresh to fix it
      final corruptedTables = await _detectCorruptedJsonbTables(tablesToSync);
      final effectiveForceRefresh = <String>{
        ...?forceRefresh,
        ...corruptedTables,
      }.toList();

      if (corruptedTables.isNotEmpty) {
        _logger.warning('syncUnified: Auto-forcing refresh for ${corruptedTables.length} corrupted tables');
      }

      // 5. Determine channels for push and pull
      // Push uses broadcastChannel (user channel) - where user has write permission
      // Pull uses pullChannel (tribe channel) - where tribe query builders exist
      final pushChannel = config.broadcastChannel;
      final pullChannel = channelTopic ?? config.pullChannel ?? config.broadcastChannel;

      // 6. If we have pending changes, push them first on user channel
      String? streamId;
      if (pendingChangeMessages.isNotEmpty) {
        _logger.info('syncUnified: Pushing ${pendingChangeMessages.length} changes on $pushChannel');
        final pushRequest = SyncRequestMessage(
          clientId: config.clientId,
          schemaVersion: schemaVersion,
          tableSeqnums: {},  // No pull in push request
          tables: [],
          pendingChanges: pendingChangeMessages,
        );

        final pushResponse = await _ws.sendRaw('sync', pushRequest.toMap(), channelTopic: pushChannel);

        // Handle push response
        if (pushResponse['status'] == 'ok') {
          streamId = pushResponse['stream_id'] as String?;
          _logger.info('syncUnified: Push complete, stream_id=$streamId');
        } else if (pushResponse['status'] == 'error') {
          throw StateError('Push failed: ${pushResponse['error']}');
        }

        // Store pending changes for ack processing
        if (streamId != null) {
          _activeSyncPendingChanges[streamId] = pendingChanges;
        }
      }

      // 7. Pull data on tribe channel
      _logger.info('syncUnified: Pulling on $pullChannel (schema v$schemaVersion)');
      final pullRequest = SyncRequestMessage(
        clientId: config.clientId,
        schemaVersion: schemaVersion,
        tableSeqnums: tableSeqnums,
        tables: tablesToSync,
        forceRefreshTables: effectiveForceRefresh.isNotEmpty ? effectiveForceRefresh : null,
        strippedRows: strippedRows?.isNotEmpty == true ? strippedRows : null,
        pendingChanges: null,  // No push in pull request
      );

      final response = await _ws.sendRaw('sync', pullRequest.toMap(), channelTopic: pullChannel);

      // 8. Check if schema upgrade is required FIRST
      if (response['status'] == 'schema_upgrade_required') {
        final targetVersion = response['target_version'] as int;
        _logger.info('syncUnified: Schema upgrade required to v$targetVersion');
        _emitSyncProgress(const SyncProgress(phase: 'migrating'));

        // Apply migrations synchronously (this also sends schema_migrated confirmation)
        await _applyMigrations({
          'current_version': targetVersion,
          'migrations': response['migrations'],
        });

        _logger.info('syncUnified: Schema upgraded to v$targetVersion, restarting sync');

        // Recursively call sync with updated schema (don't cleanup again)
        _isSyncing = false;
        return syncUnified(
          tables: tables,
          forceRefresh: forceRefresh,
          includeStripped: includeStripped,
          channelTopic: channelTopic,
          cleanupLegacyChanges: false,
        );
      }

      // 9. Handle error responses
      if (response['status'] == 'error') {
        throw StateError('Sync error: ${response['error']}');
      }

      // 10. Get stream_id from pull response and set up completer for streamed response
      final pullStreamId = response['stream_id'] as String?;
      if (pullStreamId == null) {
        throw StateError('Server did not return stream_id');
      }

      _logger.info('syncUnified: Got pull stream_id $pullStreamId, waiting for streamed data');

      // Use the pull stream_id for tracking (push acks come on their own stream if any)
      streamId = pullStreamId;

      // Create completer for this sync operation
      final completer = Completer<void>();
      _activeSyncCompleters[streamId] = completer;

      // Wait for sync_complete message
      await completer.future.timeout(
        const Duration(minutes: 5),
        onTimeout: () {
          _activeSyncCompleters.remove(streamId);
          _activeSyncPendingChanges.remove(streamId);
          throw TimeoutException('Unified sync timed out', const Duration(minutes: 5));
        },
      );

      _updateSyncState(SyncState.ready);
      _logger.info('syncUnified: Complete');

    } catch (e, stack) {
      _logger.severe('syncUnified: Error - $e', e, stack);
      _updateSyncState(SyncState.error);
      rethrow;
    } finally {
      _isSyncing = false;
    }
  }

  /// Track whether we've cleaned up legacy synced changes
  bool _hasCleanedUpLegacyChanges = false;

  /// Clean up old synced=1 records from legacy sync that were never deleted.
  /// This runs once when upgrading to unified sync to prevent table growth.
  Future<void> _cleanupLegacySyncedChanges() async {
    try {
      // Check if there are any synced=1 records
      final result = await _db!.read(
        'SELECT COUNT(*) as count FROM _synclib_changes WHERE synced = 1'
      );
      final count = result.isNotEmpty ? (result.first['count'] as int? ?? 0) : 0;

      if (count > 0) {
        _logger.info('syncUnified: Cleaning up $count legacy synced records');
        await _db!.exec('DELETE FROM _synclib_changes WHERE synced = 1');
        _logger.info('syncUnified: Legacy cleanup complete');
      }
    } catch (e) {
      // Table might not exist or have different schema - that's fine
      _logger.fine('syncUnified: Could not cleanup legacy records: $e');
    }
  }

  /// Handle schema migrations from unified sync
  Future<void> _handleSyncSchemaMigrations(SchemaMigrationsMessage message) async {
    _logger.info('syncUnified: Received schema migrations to v${message.targetVersion}');
    _emitSyncProgress(const SyncProgress(phase: 'migrating'));

    // Apply migrations using existing logic
    await _applyMigrations({
      'current_version': message.targetVersion,
      'migrations': message.migrations,
    });
  }

  /// Handle change acknowledgments from unified sync
  Future<void> _handleSyncChangeAcks(ChangeAcksMessage message) async {
    _logger.info('syncUnified: Received ${message.acks.length} change acknowledgments');

    _emitSyncProgress(SyncProgress(
      phase: 'pushing',
      changesAcked: message.acks.length,
    ));

    // Find the pending changes for this stream (we may have multiple syncs)
    // For simplicity, process acks regardless of which stream they came from
    for (final ack in message.acks) {
      if (ack.success) {
        // DELETE from _sync_changes instead of just marking synced
        // This ensures the table is cleaned up and doesn't grow indefinitely
        try {
          await _db!.deleteChange(ack.localSeqnum);
          _logger.fine('Deleted acknowledged change: ${ack.localSeqnum}');
        } catch (e) {
          _logger.warning('Failed to delete change ${ack.localSeqnum}: $e');
        }

        // Update local row seqnum if provided
        if (ack.serverSeqnum != null) {
          // Look up the change info in pending changes
          for (final pending in _activeSyncPendingChanges.values) {
            final change = pending.where((c) => c.seqnum == ack.localSeqnum).firstOrNull;
            if (change != null) {
              await _updateLocalSeqnum(change.tableName, change.rowId, ack.serverSeqnum!);
              break;
            }
          }
        }
      } else {
        _logger.warning('Change ${ack.localSeqnum} failed: ${ack.error}');
      }
    }
  }

  /// Handle data batch from unified sync
  /// Uses _batchLock to ensure serial processing with other batch operations
  Future<void> _handleSyncDataBatch(SyncDataBatchMessage message) async {
    _logger.info('syncUnified: Received batch for ${message.table} with ${message.rows.length} rows');

    _emitSyncProgress(SyncProgress(
      phase: 'pulling',
      table: message.table,
      rowCount: message.rows.length,
    ));

    // Use lock to ensure serial processing - prevents race conditions when
    // multiple batches arrive faster than they can be processed
    await _batchLock.synchronized(() async {
      // Check if table exists
      try {
        final tableCheck = await _db!.read(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='${message.table}'"
        );
        if (tableCheck.isEmpty) {
          _logger.warning('Table ${message.table} does not exist, skipping batch');
          return;
        }
      } catch (e) {
        _logger.severe('Error checking if table ${message.table} exists: $e');
        return;
      }

      // Apply batch
      await _db!.beginBulkRemote();
      try {
        for (final row in message.rows) {
          final deletedAt = row['deleted_at'];

          if (deletedAt != null) {
            // Soft-deleted row - delete locally
            final rowId = row['id'] as String;
            final deleteSql = "DELETE FROM ${message.table} WHERE id = '${_escapeSql(rowId)}'";
            await _db!.execBulkRemote(deleteSql);
          } else {
            // Normal row - insert/replace
            final change = ChangeMessage(
              table: message.table,
              operation: 'insert',
              rowId: row['id'] as String,
              data: row,
            );
            final sql = _generateSql(change);
            await _db!.execBulkRemote(sql);
          }
        }
        await _db!.endBulkRemote();
        _logger.info('Applied sync batch for ${message.table}');
      } catch (e) {
        _logger.severe('Failed to apply sync batch for ${message.table}: $e');
        await _db!.endBulkRemote(rollback: true);
      }
    });
  }

  /// Handle sync complete from unified sync
  void _handleSyncComplete(SyncCompleteMessage message) {
    _logger.info('syncUnified: Sync complete - stream ${message.streamId}, '
        'schema v${message.schemaVersion}, '
        'pushed ${message.pushSuccess}/${message.pushTotal}, '
        'pulled ${message.pullTotal} rows, '
        'seqnums: ${message.tableSeqnums}');

    _emitSyncProgress(const SyncProgress(phase: 'complete'));

    // Build per-table stats
    final pullStats = <String, SyncTableStats>{};
    for (final entry in message.pullByTable.entries) {
      pullStats[entry.key] = SyncTableStats.fromMap(entry.value);
    }

    final pushStats = <String, SyncTableStats>{};
    for (final entry in message.pushByTable.entries) {
      pushStats[entry.key] = SyncTableStats.fromMap(entry.value);
    }

    // Emit sync complete event with all details for invalidation
    final event = SyncCompleteEvent(
      streamId: message.streamId,
      schemaVersion: message.schemaVersion,
      tableSeqnums: message.tableSeqnums,
      schemaUpgraded: message.schemaUpgraded,
      migrationsApplied: message.migrationsApplied,
      totalRowsPulled: message.pullTotal,
      totalChangesPushed: message.pushTotal,
      totalChangesSucceeded: message.pushSuccess,
      totalChangesFailed: message.pushFailed,
      pullStats: pullStats,
      pushStats: pushStats,
    );

    _logger.info('syncUnified: Emitting completion event - '
        'tables with pulled data: ${event.tablesWithPulledData}, '
        'tables with pushed changes: ${event.tablesWithPushedChanges}');

    _syncCompleteController.add(event);

    // Complete the specific sync operation by streamId if provided
    if (message.streamId != null && _activeSyncCompleters.containsKey(message.streamId)) {
      final completer = _activeSyncCompleters.remove(message.streamId);
      _activeSyncPendingChanges.remove(message.streamId);
      if (completer != null && !completer.isCompleted) {
        completer.complete();
      }
    } else {
      // Fallback: complete all active sync operations
      // In practice there should only be one, but handle multiple just in case
      for (final entry in _activeSyncCompleters.entries) {
        if (!entry.value.isCompleted) {
          entry.value.complete();
        }
      }

      // Clean up
      _activeSyncCompleters.clear();
      _activeSyncPendingChanges.clear();
    }
  }

  /// Parse JSON string to map
  static Map<String, dynamic>? _parseJson(String jsonString) {
    try {
      return jsonDecode(jsonString) as Map<String, dynamic>;
    } catch (e) {
      return null;
    }
  }

  /// Get current sync state
  SyncState get syncState => _syncState;

  /// Whether a sync is currently in progress
  bool get isSyncing => _isSyncing;

  /// Stream of sync state changes
  Stream<SyncState> get syncStateChanges => _syncStateController.stream;

  /// Stream of sync progress events (for UI progress indicators)
  Stream<SyncProgress> get syncProgressEvents => _syncProgressController.stream;

  /// Stream of sync completion events (emitted when unified sync completes)
  Stream<SyncCompleteEvent> get syncCompleteEvents => _syncCompleteController.stream;

  // ==========================================================================
  // END SIMPLIFIED SYNC HANDSHAKE
  // ==========================================================================

  /// Push local changes to server
  Future<void> _pushLocalChanges() async {
    if (!_ws.isConnected) return;

    try {
      final changes = await _db!.getPendingChanges(limit: config.pushBatchSize);
      if (changes.isEmpty) {
        _logger.fine('No pending changes to push');
        return;
      }

      _logger.info('Pushing ${changes.length} changes');

      final messages = changes.map((c) => ChangeMessage.fromChange(c)).toList();
      final batch = ChangesBatchMessage(
        changes: messages,
        fromSeqnum: changes.first.seqnum,
        toSeqnum: changes.last.seqnum,
      );

      await _ws.send(batch, channelTopic: config.broadcastChannel);

      // Track pending acks and change info for server seqnum updates
      for (final change in changes) {
        _pendingAcks.add(change.seqnum);
        _pendingChangeInfo[change.seqnum] = (table: change.tableName, rowId: change.rowId);
      }
    } catch (e, stack) {
      _logger.severe('Failed to push changes: $e', e, stack);
    }
  }

  /// Request remote changes from server
  Future<void> _pullRemoteChangesForTable(String table) async {
    if (!_ws.isConnected) return;

    // Don't pull until client is ready (hello received, migrations applied)
    if (!isReady) {
      _logger.fine('Skipping pull for $table - client not ready yet (state: $_readyState)');
      return;
    }

    if (_lastSyncedSeqnum == 0) {
      _lastSyncedSeqnum = await _getMaxSeqnumFromTable(table) ?? 0; // seqnum is global across all tables. we have anything under the max
    }

    try {
      final request = RequestChangesMessage(
        sinceSeqnum: _lastSyncedSeqnum,
        table: table
      );
      // Use pullChannel if specified, otherwise fall back to broadcastChannel
      final channelTopic = config.pullChannel ?? config.broadcastChannel;
      await _ws.send(request, channelTopic: channelTopic);
      _logger.fine('Requested remote changes since $_lastSyncedSeqnum on channel $channelTopic');
    } catch (e) {
      _logger.severe('Failed to request changes: $e');
    }
  }

  /// Request remote changes from server
  Future<void> _pullRemoteChanges() async {
    if (!_ws.isConnected) return;

    // Don't pull until client is ready (hello received, migrations applied)
    if (!isReady) {
      _logger.fine('Skipping pull - client not ready yet (state: $_readyState)');
      return;
    }

    try {
      final request = RequestChangesMessage(
        sinceSeqnum: _lastSyncedSeqnum ?? 0,
      );
      // Use pullChannel if specified, otherwise fall back to broadcastChannel
      final channelTopic = config.pullChannel ?? config.broadcastChannel;
      await _ws.send(request, channelTopic: channelTopic);
      _logger.fine('Requested remote changes since $_lastSyncedSeqnum on channel $channelTopic');
    } catch (e) {
      _logger.severe('Failed to request changes: $e');
    }
  }

  /// Send hello message to server
  Future<void> _sendHello() async {
    // Get current schema version from database
    final schemaVersion = await _db!.getSchemaVersion();

    final hello = HelloMessage(
      clientId: config.clientId,
      lastSeqnum: _lastSyncedSeqnum,
      schemaVersion: schemaVersion,
      metadata: config.metadata,
    );
    await _ws.send(hello, channelTopic: config.broadcastChannel);
    _logger.info('Sent hello message with schema version $schemaVersion');
  }

  /// Handle incoming message from server
  Future<void> _handleMessage(SyncMessage message) async {
    try {
      if (message is ChangeMessage) {
        await _applyRemoteChange(message);
      } else if (message is ChangesBatchMessage) {
        await _applyRemoteChanges(message.changes);
      } else if (message is AckMessage) {
        _handleAck(message);
      } else if (message is PhoenixReplyMessage) {
        await _handlePhoenixReply(message);
      } else if (message is ErrorMessage) {
        _handleError(message);
      } else if (message is SnapshotBatchMessage) {
        await _queueSnapshotBatch(message);
      } else if (message is SnapshotCompleteMessage) {
        _handleSnapshotComplete(message);
      } else if (message is JobUpdateMessage) {
        _handleJobUpdate(message);
      } else if (message is LivestreamMessage) {
        _handleLivestream(message);
      } else if (message is ConversationMessage) {
        _handleConversation(message);
      } else if (message is PresenceMessage) {
        _handlePresence(message);
      } else if (message is FeedStatusMessage) {
        _handleFeedStatus(message);
      } else if (message is InteractionMessage) {
        _handleInteraction(message);
      } else if (message is DirectStreamMessage) {
        _handleDirectStream(message);
      } else if (message is SchemaUpdateMessage) {
        await _handleSchemaUpdate(message);
      } else if (message is SchemaMigrationsMessage) {
        await _handleSyncSchemaMigrations(message);
      } else if (message is ChangeAcksMessage) {
        await _handleSyncChangeAcks(message);
      } else if (message is SyncDataBatchMessage) {
        await _handleSyncDataBatch(message);
      } else if (message is SyncCompleteMessage) {
        _handleSyncComplete(message);
      } else {
        _logger.warning('Unhandled message type: ${message.runtimeType}');
      }
    } catch (e, stack) {
      _logger.severe('Error handling message: $e', e, stack);
    }
  }

  /// Apply a single remote change to local database
  Future<void> _applyRemoteChange(ChangeMessage change) async {
    try {
      // Check if this is a soft-deleted row - delete locally instead of inserting
      if (change.data?['deleted_at'] != null) {
        final deleteSql = "DELETE FROM ${change.table} WHERE id = '${_escapeSql(change.rowId)}'";
        await _db!.applyRemote(
          tableName: change.table,
          rowId: change.rowId,
          operation: SynclibOperation.delete,
          sql: deleteSql,
          data: null,
        );
        _logger.fine('Soft-deleted row locally: ${change.table}:${change.rowId}');
        _remoteChangeController.add(change);
        return;
      }

      // Check for conflicts with pending local changes
      if (config.onConflict != null) {
        final localChanges = await _db!.getPendingChanges();
        try {
          final conflict = localChanges.firstWhere(
            (c) => c.tableName == change.table && c.rowId == change.rowId,
          );

          // Conflict found - resolve it
          _logger.info('Conflict detected for ${change.table}:${change.rowId}');
          final localChange = ChangeMessage.fromChange(conflict);
          final resolved = await config.onConflict!(localChange, change);
          if (resolved == null) {
            _logger.info('Conflict skipped for ${change.table}:${change.rowId}');
            return;
          }
          // Use resolved change
          change = resolved;
        } catch (e) {
          // No conflict found - proceed normally
          _logger.fine('No conflict for ${change.table}:${change.rowId}');
        }
      }

      // Check if change contains JSONB data that needs parameterized query
      final hasJsonbData = change.data != null &&
        (change.data is Map || change.data is List ||
         (change.data is Map<String, dynamic> &&
          (change.data as Map<String, dynamic>).values.any((v) => v is Map || v is List)));

      if (hasJsonbData && (change.operation == 'update' || change.operation == 'insert' || change.operation == 'upsert')) {
        // Use parameterized query for JSONB data
        final result = _generateSqlWithParams(change);
        await _db!.applyRemoteWithParams(
          tableName: change.table,
          rowId: change.rowId,
          operation: change.toSynclibOperation(),
          sql: result.sql,
          params: result.params,
          data: change.data?.toString(),
        );
      } else {
        // Use simple SQL for non-JSONB operations
        final sql = _generateSql(change);
        await _db!.applyRemote(
          tableName: change.table,
          rowId: change.rowId,
          operation: change.toSynclibOperation(),
          sql: sql,
          data: change.data?.toString(),
        );
      }

      _logger.fine('Applied remote change: ${change.operation} on ${change.table}');

      // Notify listeners that a remote change was applied
      _remoteChangeController.add(change);
    } catch (e, stack) {
      _logger.severe('Failed to apply remote change: $e', e, stack);
      // Note: We don't send acks for remote changes - only the server sends acks
    }
  }

  /// Apply multiple remote changes
  Future<void> _applyRemoteChanges(List<ChangeMessage> changes) async {
    _logger.info('Applying ${changes.length} remote changes');

    // Use lock to ensure serial processing with snapshot batches
    await _batchLock.synchronized(() async {
      // Use bulk mode for efficiency
      await _db!.beginBulkRemote();
      int deletedCount = 0;
      try {
        for (final change in changes) {
          // Check if this is a soft-deleted row - delete locally instead of inserting
          if (change.data?['deleted_at'] != null) {
            final deleteSql = "DELETE FROM ${change.table} WHERE id = '${_escapeSql(change.rowId)}'";
            await _db!.execBulkRemote(deleteSql);
            deletedCount++;
          } else {
            final sql = _generateSql(change);
            await _db!.execBulkRemote(sql);
          }
        }
        await _db!.endBulkRemote();
        if (deletedCount > 0) {
          _logger.info('Soft-deleted $deletedCount rows locally');
        }
        _logger.info('Successfully applied ${changes.length} changes');

        // Update last synced seqnum
        if (changes.isNotEmpty && changes.last.seqnum != null) {
          _lastSyncedSeqnum = changes.last.seqnum ?? 0;
        }
      } catch (e) {
        _logger.severe('Failed to apply changes batch: $e');
        await _db!.endBulkRemote(rollback: true);
      }
    });
  }

  /// Handle acknowledgment from server
  void _handleAck(AckMessage ack) {
    _logger.fine('Received ack for seqnum ${ack.seqnum}: ${ack.success}, server_seqnum: ${ack.serverSeqnum}');

    if (ack.success) {
      _pendingAcks.remove(ack.seqnum);

      // Get the change info for this local seqnum
      final changeInfo = _pendingChangeInfo.remove(ack.seqnum);

      // Mark as synced in local database
      _db!.markSynced(ack.seqnum).catchError((e) {
        _logger.severe('Failed to mark synced: $e');
      });

      // Update the local row's seqnum column with the server-assigned seqnum
      if (ack.serverSeqnum != null && changeInfo != null) {
        _updateLocalSeqnum(changeInfo.table, changeInfo.rowId, ack.serverSeqnum!).catchError((e) {
          _logger.warning('Failed to update local seqnum for ${changeInfo.table}:${changeInfo.rowId}: $e');
        });
      }
    } else {
      _logger.warning('Change ${ack.seqnum} failed: ${ack.error}');
      _pendingChangeInfo.remove(ack.seqnum);
      // TODO: Implement retry logic
    }
  }

  /// Update the seqnum column on a local row after server assigns it
  Future<void> _updateLocalSeqnum(String table, String rowId, int serverSeqnum) async {
    try {
      final sql = "UPDATE $table SET seqnum = $serverSeqnum WHERE id = '${rowId.replaceAll("'", "''")}'";
      await _db!.exec(sql);
      _logger.fine('Updated local seqnum for $table:$rowId to $serverSeqnum');
    } catch (e) {
      // Table might not have seqnum column - this is fine for some tables
      _logger.fine('Could not update seqnum for $table:$rowId (table may not have seqnum column): $e');
    }
  }

  /// Handle error message from server
  void _handleError(ErrorMessage error) {
    _logger.severe('Server error: ${error.code} - ${error.message}');
    // TODO: Implement error recovery strategies
  }

  /// Queue snapshot batch for processing (ensures batches are processed serially)
  Future<void> _queueSnapshotBatch(SnapshotBatchMessage batch) async {
    _batchQueue.add(batch);
    _logger.info('Queued snapshot batch for ${batch.table}: ${batch.rows.length} rows (queue size: ${_batchQueue.length})');

    // Process the queue - lock ensures only one process runs at a time
    await _processBatchQueue();
  }

  /// Process all batches in the queue serially (protected by lock)
  Future<void> _processBatchQueue() async {
    await _batchLock.synchronized(() async {
      while (_batchQueue.isNotEmpty) {
        final batch = _batchQueue.removeAt(0);
        _logger.info('Processing batch for ${batch.table} (${_batchQueue.length} remaining in queue)');
        await _handleSnapshotBatch(batch);
      }
    });
  }

  /// Handle snapshot batch message (called serially from queue)
  Future<void> _handleSnapshotBatch(SnapshotBatchMessage batch) async {
    final startTime = DateTime.now();
    _logger.info('Received snapshot batch for ${batch.table}: ${batch.rows.length} rows');

    // Check if table exists before trying to insert
    try {
      final tableCheck = await _db!.read("SELECT name FROM sqlite_master WHERE type='table' AND name='${batch.table}'");
      if (tableCheck.isEmpty) {
        _logger.warning('Table ${batch.table} does not exist, skipping batch');
        return;
      }
    } catch (e) {
      _logger.severe('Error checking if table ${batch.table} exists: $e');
      return;
    }

    // Apply each row to the database
    await _db!.beginBulkRemote();
    int processedCount = 0;
    int deletedCount = 0;
    try {
      for (final row in batch.rows) {
        final deletedAt = row['deleted_at'];

        if (deletedAt != null) {
          // Soft-deleted row - delete locally
          final rowId = row['id'] as String;
          final deleteSql = "DELETE FROM ${batch.table} WHERE id = '${_escapeSql(rowId)}'";
          await _db!.execBulkRemote(deleteSql);
          deletedCount++;
        } else {
          // Normal row - insert/replace as usual
          final change = ChangeMessage(
            table: batch.table,
            operation: 'insert',
            rowId: row['id'] as String,
            data: row,
          );

          final sql = _generateSql(change);
          await _db!.execBulkRemote(sql);
        }
        processedCount++;
      }
      await _db!.endBulkRemote();

      if (deletedCount > 0) {
        _logger.info('Soft-deleted $deletedCount rows from ${batch.table}');
      }

      final elapsed = DateTime.now().difference(startTime).inMilliseconds;
      _logger.info('Applied snapshot batch for ${batch.table}: $processedCount/${batch.rows.length} rows in ${elapsed}ms');

      // Emit batch event for UI progress tracking
      _snapshotBatchController.add(SnapshotBatchEvent(
        streamId: batch.streamId,
        table: batch.table,
        rowCount: processedCount,
      ));
    } catch (e, stackTrace) {
      final elapsed = DateTime.now().difference(startTime).inMilliseconds;
      _logger.severe('Failed to apply snapshot batch for ${batch.table} after $processedCount/${batch.rows.length} rows (${elapsed}ms): $e');
      _logger.fine('Stack trace: $stackTrace');
      await _db!.endBulkRemote(rollback: true);
      rethrow;
    }
  }

  /// Handle snapshot complete message
  void _handleSnapshotComplete(SnapshotCompleteMessage message) {
    _logger.info('Snapshot complete for stream ${message.streamId} on channel ${message.channelId}');
    _snapshotCompleteController.add(SnapshotCompleteEvent(
      streamId: message.streamId,
      channelId: message.channelId,
    ));

    // Check if this completes an auto-sync operation
    _onSnapshotStreamComplete(message.channelId);
  }

  /// Handle job update message
  void _handleJobUpdate(JobUpdateMessage message) {
    _logger.info('Job update: ${message.stepType} - step ${message.currentStep}/${message.totalSteps} for job ${message.jobId}');
    _jobUpdateController.add(message);
  }

  /// Handle livestream message
  void _handleLivestream(LivestreamMessage message) {
    _logger.info('Livestream event: ${message.event} - stream ${message.streamId} by user ${message.userId}');
    _livestreamController.add(message);
  }

  /// Handle conversation message
  void _handleConversation(ConversationMessage message) {
    _logger.info('Conversation event: ${message.event} - conversation ${message.conversationId} by user ${message.userId}');
    _conversationController.add(message);
  }

  /// Handle presence message (video/livestream viewers)
  void _handlePresence(PresenceMessage message) {
    _logger.info('Presence event: ${message.presenceType} - video ${message.videoId}, count ${message.viewerCount}');
    _presenceController.add(message);
  }

  /// Handle feed status message (new videos, online count)
  void _handleFeedStatus(FeedStatusMessage message) {
    _logger.info('Feed status event: ${message.statusType} - video ${message.videoId}, online ${message.onlineCount}');
    _feedStatusController.add(message);
  }

  /// Handle interaction message (likes, comments, comment likes)
  void _handleInteraction(InteractionMessage message) {
    _logger.info('Interaction event: ${message.interactionType} - video ${message.videoId}, comment ${message.commentId}');
    _interactionController.add(message);
  }

  /// Handle direct stream message (participant changes, segment availability)
  void _handleDirectStream(DirectStreamMessage message) {
    _logger.info('Direct stream event: ${message.event} - stream ${message.streamId}');
    _directStreamController.add(message);
  }

  /// Handle schema update notification
  Future<void> _handleSchemaUpdate(SchemaUpdateMessage message) async {
    _logger.warning('Schema update notification: server has new version ${message.newVersion}');

    // Get current client schema version
    final currentVersion = await _db!.getSchemaVersion();

    if (message.newVersion > currentVersion) {
      _logger.info('Client schema v$currentVersion is behind server v${message.newVersion}');

      // If migrations are included, apply them directly
      if (message.migrations != null && message.migrations!.isNotEmpty) {
        _logger.info('Applying ${message.migrations!.length} migrations from schema_update notification');
        _updateReadyState(SyncReadyState.applyingMigrations);
        await _applyMigrations({'migrations': message.migrations, 'current_version': message.newVersion});
        _updateReadyState(SyncReadyState.ready);
      } else {
        // Otherwise, log and let app handle (could trigger reconnect)
        _logger.warning('Schema update detected but no migrations provided - may need to reconnect');
      }
    } else {
      _logger.info('Client schema is already up to date (v$currentVersion)');
    }
  }

  /// Handle Phoenix reply messages (especially hello response with migrations)
  Future<void> _handlePhoenixReply(PhoenixReplyMessage reply) async {
    final response = reply.response;

    // Check if this is a hello response with migrations
    if (response['status'] == 'upgrade_needed') {
      _logger.info('Schema upgrade needed');
      _updateReadyState(SyncReadyState.applyingMigrations);
      await _applyMigrations(response);
      _updateReadyState(SyncReadyState.ready);
    } else if (response['status'] == 'up_to_date') {
      _logger.info('Schema is up to date');
      _updateReadyState(SyncReadyState.ready);
    } else if (response['status'] == 'ok') {
      _logger.fine('Received OK response');
      // If we were waiting for hello, mark as ready
      if (_readyState == SyncReadyState.waitingForHello) {
        _updateReadyState(SyncReadyState.ready);
      }
    }
  }

  /// Update sync ready state and broadcast change
  void _updateReadyState(SyncReadyState newState) {
    if (_readyState != newState) {
      _readyState = newState;
      _syncReadyStateController.add(newState);
      _logger.info('Sync ready state changed to: $newState');
    }
  }

  /// Apply schema migrations from server
  Future<void> _applyMigrations(Map<String, dynamic> response) async {
    final currentVersion = response['current_version'] as int;
    final migrations = response['migrations'] as List?;

    if (migrations == null || migrations.isEmpty) {
      _logger.warning('No migrations provided');
      return;
    }

    _logger.info('Applying ${migrations.length} migration(s) to reach version $currentVersion');

    for (final migration in migrations) {
      final migrationMap = migration as Map<String, dynamic>;
      final version = migrationMap['version'] as int;
      final description = migrationMap['description'] as String;
      final sqlStatements = migrationMap['sql'] as List;

      _logger.info('Applying migration v$version: $description');

      try {
        // Execute each SQL statement
        for (final sql in sqlStatements) {
          _logger.info('Executing: $sql');
          await _db!.exec(sql as String);
        }

        // Update schema version
        await _db!.setSchemaVersion(version);
        _logger.info('Successfully applied migration v$version');
      } catch (e, stack) {
        _logger.severe('Failed to apply migration v$version: $e', e, stack);
        rethrow;
      }
    }

    // Confirm migration to server
    final confirm = SchemaConfirmMessage(version: currentVersion);
    await _ws.send(confirm, channelTopic: config.broadcastChannel);

    _logger.info('Confirmed schema migration to server');
  }

  /// Handle connection state changes
  void _handleStateChange(ConnectionState state) {
    _logger.info('Connection state: $state');

    switch (state) {
      case ConnectionState.connected:
        // Update sync state
        _updateSyncState(SyncState.ready);
        // When reconnected (not initial connection), rejoin channels
        if (_hasConnectedOnce) {
          _logger.info('Reconnected - rejoining channels');
          _joinChannels().catchError((e) {
            _logger.severe('Failed to rejoin channels after reconnect: $e');
          });
        }
        break;
      case ConnectionState.connecting:
      case ConnectionState.reconnecting:
        _updateSyncState(SyncState.connecting);
        break;
      case ConnectionState.disconnected:
      case ConnectionState.failed:
        _updateSyncState(SyncState.disconnected);
        _stopPeriodicSync();
        break;
      case ConnectionState.authFailed:
        _updateSyncState(SyncState.error);
        break;
    }
  }

  /// Start periodic sync timers
  ///
  /// [pullRemote] - If false, only pushes local changes periodically (no pull).
  /// Defaults to true.
  /// [pushLocal] - If false, only pulls remote changes periodically (no push).
  /// Defaults to true.
  void startPeriodicSync({bool pullRemote = true, bool pushLocal = true}) {
    _pullRemoteEnabled = pullRemote;
    _pushLocalEnabled = pushLocal;

    if (config.pushInterval != null && _pushLocalEnabled) {
      _pushTimer?.cancel();
      _pushTimer = Timer.periodic(config.pushInterval!, (_) => _pushLocalChanges());
    } else if (!_pushLocalEnabled) {
      _pushTimer?.cancel();
      _pushTimer = null;
    }

    if (config.pullInterval != null && _pullRemoteEnabled) {
      _pullTimer?.cancel();
      _pullTimer = Timer.periodic(config.pullInterval!, (_) => _pullRemoteChanges());
    } else if (!_pullRemoteEnabled) {
      _pullTimer?.cancel();
      _pullTimer = null;
    }
  }

  /// Update whether periodic sync should pull from remote
  ///
  /// Can be called at any time to enable/disable remote pulling.
  set pullRemoteEnabled(bool enabled) {
    if (_pullRemoteEnabled == enabled) return;
    _pullRemoteEnabled = enabled;

    if (enabled && config.pullInterval != null) {
      _pullTimer?.cancel();
      _pullTimer = Timer.periodic(config.pullInterval!, (_) => _pullRemoteChanges());
    } else if (!enabled) {
      _pullTimer?.cancel();
      _pullTimer = null;
    }
  }

  bool get pullRemoteEnabled => _pullRemoteEnabled;

  /// Update whether periodic sync should push local changes
  ///
  /// Can be called at any time to enable/disable local pushing.
  set pushLocalEnabled(bool enabled) {
    if (_pushLocalEnabled == enabled) return;
    _pushLocalEnabled = enabled;

    if (enabled && config.pushInterval != null) {
      _pushTimer?.cancel();
      _pushTimer = Timer.periodic(config.pushInterval!, (_) => _pushLocalChanges());
    } else if (!enabled) {
      _pushTimer?.cancel();
      _pushTimer = null;
    }
  }

  bool get pushLocalEnabled => _pushLocalEnabled;

  /// Stop periodic sync timers
  void _stopPeriodicSync() {
    _pushTimer?.cancel();
    _pushTimer = null;
    _pullTimer?.cancel();
    _pullTimer = null;
  }

  /// Generate SQL statement from change message
  String _generateSql(ChangeMessage change) {
    // Filter out server-only fields and map field names
    Map<String, dynamic> filteredData = {};

    if (change.data != null) {
      for (final entry in change.data!.entries) {
        final key = entry.key;
        final value = entry.value;

        // Skip server-only timestamp fields and id (we add id separately)
        if (key == 'inserted_at' || key == 'id') continue;

        // Normalize field names to lowercase to match PostgreSQL column names
        // PostgreSQL identifiers are lowercase by default, so our schema has userid, createdat, etc.
        // But the document JSON has camelCase like userId, createdAt
        String normalizedKey = key.toLowerCase();

        filteredData[normalizedKey] = value;
      }
    }

    switch (change.operation) {
      case 'insert':
        // final columns = filteredData.keys.join(', ');
        // final values = filteredData.values
        //   .map((v) => _formatSqlValue(v))
        //   .join(', ');
        // return 'INSERT OR REPLACE INTO ${change.table} ($columns) VALUES ($values)';
        // the problems is that there is no isSusbcribed table. it should be document: {isSubscribed: true}
        final columns = ['id', ...filteredData.keys].join(', ');
        final values = ['\'${_escapeSql(change.rowId)}\'', ...filteredData.values.map((v) => _formatSqlValue(v))].join(', ');
        return 'INSERT OR REPLACE INTO ${change.table} ($columns) VALUES ($values)';
      case 'update':
        final sets = filteredData.entries
          .map((e) => '${e.key} = ${_formatSqlValue(e.value)}')
          .join(', ');
        return 'UPDATE ${change.table} SET $sets WHERE id = \'${_escapeSql(change.rowId)}\'';

      case 'delete':
        return 'DELETE FROM ${change.table} WHERE id = \'${_escapeSql(change.rowId)}\'';

      default:
        throw ArgumentError('Unknown operation: ${change.operation}');
    }
  }

  /// Generate SQL with parameters for JSONB data
  ({String sql, List<String?> params}) _generateSqlWithParams(ChangeMessage change) {
    final data = change.data as Map<String, dynamic>;
    final params = <String?>[];

    // Filter out metadata fields
    final filteredData = <String, dynamic>{};
    for (final entry in data.entries) {
      final key = entry.key;
      final value = entry.value;

      if (key == 'id' || key == 'updated_at' || key == 'inserted_at') continue;

      filteredData[key] = value;
    }

    switch (change.operation) {
      case 'insert':
      case 'upsert':
        final columns = ['id', ...filteredData.keys].join(', ');

        // Build placeholders - use jsonb(?) for Map/List values, ? for others
        final placeholderList = <String>['?']; // id is always a simple value
        for (final value in filteredData.values) {
          if (value is Map || value is List) {
            placeholderList.add('jsonb(?)');
          } else {
            placeholderList.add('?');
          }
        }
        final placeholders = placeholderList.join(', ');

        // Build parameters array
        params.add(change.rowId);
        for (final value in filteredData.values) {
          if (value is Map || value is List) {
            params.add(jsonEncode(value));
          } else if (value is bool) {
            params.add(value ? '1' : '0');
          } else {
            params.add(value?.toString());
          }
        }

        final insertSql = 'INSERT OR REPLACE INTO ${change.table} ($columns) VALUES ($placeholders)';
        return (sql: insertSql, params: params);

      case 'update':
        final setClauses = <String>[];
        for (final entry in filteredData.entries) {
          if (entry.value is Map || entry.value is List) {
            setClauses.add('${entry.key} = jsonb(?)');
            params.add(jsonEncode(entry.value));
          } else if (entry.value is bool) {
            setClauses.add('${entry.key} = ?');
            params.add(entry.value ? '1' : '0');
          } else {
            setClauses.add('${entry.key} = ?');
            params.add(entry.value?.toString());
          }
        }

        final updateSql = 'UPDATE ${change.table} SET ${setClauses.join(', ')} WHERE id = ?';
        params.add(change.rowId);
        return (sql: updateSql, params: params);

      default:
        throw ArgumentError('Unsupported operation for parameterized query: ${change.operation}');
    }
  }

  /// Format a value for SQL
  String _formatSqlValue(dynamic value) {
    if (value == null) {
      return 'null';
    } else if (value is String) {
      return "'${_escapeSql(value)}'";
    } else if (value is bool) {
      return value ? '1' : '0';
    } else if (value is num) {
      return value.toString();
    } else if (value is Map || value is List) {
      // For complex types (JSONB, arrays), encode as JSON string
      // final jsonString = jsonb.encode(value); // jsonEncode(value);
      // return "'${_escapeSql(jsonString)}'";

      // could try like this...
      // For complex types (JSONB, arrays), use SQLite's jsonb() function
      // This creates proper JSONB binary format for efficient storage/querying
      final jsonString = jsonEncode(value);
      return "jsonb('${_escapeSql(jsonString)}')";

    } else {
      return value.toString();
    }
  }

  /// Escape single quotes in SQL strings
  String _escapeSql(String value) {
    return value.replaceAll("'", "''");
  }

  /// Stream snapshot of tables from server
  ///
  /// Supports incremental sync by querying max seqnum from local tables.
  /// Supports custom ordering via [orderBy] and [orderDesc] parameters.
  ///
  /// Example:
  /// ```dart
  /// // Full sync (all data)
  /// await syncClient.streamSnapshot(['users', 'journal_entries']);
  ///
  /// // Incremental sync (only changes since last sync)
  /// await syncClient.streamSnapshot(['users', 'journal_entries'], incremental: true);
  ///
  /// // With custom ordering (newest first)
  /// await syncClient.streamSnapshot(['videos'], orderBy: 'created_at', orderDesc: true);
  /// ```
  Future<void> streamSnapshot(
    List<String> tables, {
    bool incremental = false,
    String? channelTopic,
    bool waitForReconnect = true,
    String? orderBy,       // Column name like 'created_at', 'last_modified_ms', 'seqnum'
    bool orderDesc = true, // true = DESC, false = ASC (default descending)
  }) async {
    if (!_ws.isConnected) {
      if (waitForReconnect) {
        _logger.info('streamSnapshot: Not connected, waiting for reconnection...');
        await _waitForConnection();
      } else {
        throw Exception('Not connected to server');
      }
    }

    // Emit request event so UI can show which tables are being requested
    _snapshotRequestController.add(SnapshotRequestEvent(
      tables: tables,
      incremental: incremental,
    ));

    Map<String, int>? tableSeqnums;

    if (incremental) {
      tableSeqnums = await _getPerTableSeqnums(tables);
      _logger.info('Requesting incremental snapshot with per-table seqnums: $tableSeqnums, orderBy: $orderBy, orderDesc: $orderDesc');
    } else {
      _logger.info('Requesting full snapshot, orderBy: $orderBy, orderDesc: $orderDesc');
    }

    final payload = {
      'tables': tables,
      if (tableSeqnums != null) 'table_seqnums': tableSeqnums,
      if (orderBy != null) 'order_by': orderBy,
      if (orderBy != null) 'order_desc': orderDesc,
    };

    await _ws.sendRaw('stream_snapshot', payload, channelTopic: channelTopic);
  }

  /// Get the max seqnum for each table separately
  /// Returns a map of table name to its max seqnum
  Future<Map<String, int>> _getPerTableSeqnums(List<String> tables) async {
    final result = <String, int>{};

    for (final table in tables) {
      result[table] = await _getMaxSeqnumFromTable(table);
    }

    return result;
  }

  /// Query the max seqnum from a local SQLite table
  Future<int> _getMaxSeqnumFromTable(String table) async {
    try {
      final result = await _db!.read('SELECT MAX(seqnum) as max_seqnum FROM $table');

      if (result.isEmpty) {
        return 0;
      }

      final maxSeqnum = result.first['max_seqnum'];
      if (maxSeqnum == null) {
        return 0;
      }

      return maxSeqnum is int ? maxSeqnum : int.parse(maxSeqnum.toString());
    } catch (e) {
      _logger.warning('Failed to get max seqnum for table $table: $e');
      return 0;
    }
  }

  /// Fetch a full row from the server (including JSONB fields)
  ///
  /// Example:
  /// ```dart
  /// final user = await syncClient.fetchRow('users', 'user123');
  /// _logger.info(user['document']); // Access JSONB field
  /// ```
  Future<Map<String, dynamic>> fetchRow(String table, String rowId) async {
    final response = await sendMessage('fetch_row', {
      'table': table,
      'row_id': rowId,
    });
    return response['row'] as Map<String, dynamic>;
  }

  /// Send a custom message to the server and wait for reply
  ///
  /// Example:
  /// ```dart
  /// final response = await syncClient.sendMessage('fetch_row', {
  ///   'table': 'users',
  ///   'row_id': 'user123',
  /// });
  /// ```
  Future<Map<String, dynamic>> sendMessage(
    String event,
    Map<String, dynamic> payload,
    {String? channelTopic, bool waitForReconnect = true}
  ) async {
    if (!_ws.isConnected) {
      if (waitForReconnect) {
        _logger.info('sendMessage: Not connected, waiting for reconnection...');
        await _waitForConnection();
      } else {
        throw Exception('Not connected to server');
      }
    }

    try {
      // Send the message and wait for response
      // The Phoenix library handles request/response matching automatically
      final response = await _ws.sendRaw(event, payload, channelTopic: channelTopic);
      return response;
    } catch (e) {
      _logger.severe('Failed to send message: $e');
      rethrow;
    }
  }

  /// Send a conversation presence event (user_joined or user_left)
  ///
  /// Example:
  /// ```dart
  /// // Notify that user joined a conversation
  /// await syncClient.sendConversationPresence(
  ///   conversationId: 'tribe_123',
  ///   userId: 'user_456',
  ///   event: 'conversation:user_joined',
  /// );
  /// ```
  Future<void> sendConversationPresence({
    required String conversationId,
    required String userId,
    required String event, // 'conversation:user_joined' or 'conversation:user_left'
    String? channelTopic,
    bool waitForReconnect = true,
  }) async {
    if (!_ws.isConnected) {
      if (waitForReconnect) {
        _logger.info('sendConversationPresence: Not connected, waiting for reconnection...');
        await _waitForConnection();
      } else {
        throw Exception('Not connected to server');
      }
    }

    try {
      final payload = {
        'conversation_id': conversationId,
        'user_id': userId,
        'timestamp': DateTime.now().millisecondsSinceEpoch ~/ 1000,
      };

      await _ws.sendRaw(event, payload, channelTopic: channelTopic);
      _logger.info('Sent conversation presence event: $event for conversation $conversationId');
    } catch (e) {
      _logger.severe('Failed to send conversation presence: $e');
      rethrow;
    }
  }

  /// Get current connection state
  ConnectionState get connectionState => _ws.state;

  /// Stream of connection state changes
  Stream<ConnectionState> get stateChanges => _ws.stateChanges;

  /// Stream of remote changes as they are applied
  Stream<ChangeMessage> get remoteChanges => _remoteChangeController.stream;

  /// Stream of snapshot complete events (emits streamId and channelId when snapshot finishes)
  Stream<SnapshotCompleteEvent> get snapshotComplete => _snapshotCompleteController.stream;

  /// Stream of snapshot batch events (emits table name and row count as each batch is processed)
  /// Use this to show loading progress in the UI
  Stream<SnapshotBatchEvent> get snapshotBatches => _snapshotBatchController.stream;

  /// Stream of snapshot request events (emits when snapshot is requested with list of tables)
  /// Use this to show which tables will be loaded, even for incremental syncs with no batches
  Stream<SnapshotRequestEvent> get snapshotRequests => _snapshotRequestController.stream;

  /// Stream of job update events (from ECS tasks via webhook)
  Stream<JobUpdateMessage> get jobUpdates => _jobUpdateController.stream;

  /// Stream of livestream events (started/stopped notifications)
  Stream<LivestreamMessage> get livestreamEvents => _livestreamController.stream;

  /// Stream of conversation events (user presence, message notifications, online count)
  Stream<ConversationMessage> get conversationEvents => _conversationController.stream;

  /// Stream of presence events (video/livestream viewer updates)
  Stream<PresenceMessage> get presenceEvents => _presenceController.stream;

  /// Stream of feed status events (new videos, online count)
  Stream<FeedStatusMessage> get feedStatusEvents => _feedStatusController.stream;

  /// Stream of interaction events (likes, comments, comment likes)
  Stream<InteractionMessage> get interactionEvents => _interactionController.stream;

  /// Stream of direct stream events (participant changes, segment availability)
  Stream<DirectStreamMessage> get directStreamEvents => _directStreamController.stream;

  /// Stream of sync ready state changes
  /// Listen to this to know when the client is ready to stream snapshots
  /// States: waitingForHello -> applyingMigrations -> ready
  Stream<SyncReadyState> get syncReadyState => _syncReadyStateController.stream;

  /// Get current sync ready state
  SyncReadyState get readyState => _readyState;

  /// Stream of auto-sync events
  /// Emitted when stale tables are detected and auto-sync starts, completes, or fails.
  /// Contains both the discovery (what's stale) and progress (syncing/completed).
  Stream<AutoSyncEvent> get autoSyncEvents => _autoSyncController.stream;

  /// Per-channel sync states (read-only view)
  Map<String, ChannelSyncState> get channelStates => Map.unmodifiable(_channelStates);

  /// Current auto-sync state (derived from channel states)
  /// Returns syncing if ANY channel is syncing, idle otherwise
  AutoSyncState get autoSyncState {
    if (_channelStates.values.any((c) => c.autoSyncInProgress)) {
      return AutoSyncState.syncing;
    }
    if (_channelStates.values.any((c) => c.autoSyncComplete)) {
      return AutoSyncState.completed;
    }
    return AutoSyncState.idle;
  }

  /// Whether ANY channel is currently auto-syncing
  bool get isAutoSyncing => _channelStates.values.any((c) => c.autoSyncInProgress);

  /// Whether ALL channels have completed their initial sync (or had no stale tables)
  bool get allChannelsSynced => _channelStates.isNotEmpty &&
      _channelStates.values.every((c) => c.isReady);

  /// Wait for ALL channels to complete their auto-sync
  /// Returns immediately if no channels are syncing.
  /// Throws if any auto-sync fails.
  Future<void> waitForAutoSyncComplete() async {
    final pendingCompleters = _channelStates.values
        .where((c) => c.completer != null && !c.completer!.isCompleted)
        .map((c) => c.completer!.future)
        .toList();

    if (pendingCompleters.isEmpty) {
      return;
    }

    await Future.wait(pendingCompleters);
  }

  /// Check if client is ready to stream snapshots
  /// Returns true only when state is SyncReadyState.ready AND all channels are synced.
  /// Use this to gate UI rendering until initial data is loaded.
  bool get isReady => _readyState == SyncReadyState.ready && allChannelsSynced;

  /// Dispose resources
  Future<void> dispose() async {
    _stopPeriodicSync();
    _syncOnWriteDebounceTimer?.cancel();
    await _localChangeSubscription?.cancel();
    await _messageSubscription?.cancel();
    await _stateSubscription?.cancel();
    await _remoteChangeController.close();
    await _snapshotCompleteController.close();
    await _snapshotBatchController.close();
    await _snapshotRequestController.close();
    await _jobUpdateController.close();
    await _livestreamController.close();
    await _conversationController.close();
    await _presenceController.close();
    await _feedStatusController.close();
    await _interactionController.close();
    await _directStreamController.close();
    await _syncReadyStateController.close();
    await _autoSyncController.close();
    await _syncStateController.close();
    await _syncProgressController.close();
    await _ws.dispose();
    await _db?.close();
    _isInitialized = false;
  }
}
