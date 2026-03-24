import 'dart:async';
import 'dart:convert';
import 'package:logging/logging.dart';
import 'package:synclib_flutter/synclib_flutter.dart';
import 'package:synchronized/synchronized.dart';
import 'connection/websocket_manager.dart';
import 'protocol/message.dart';
import 'protocol/codec.dart';

/// Default block size for Merkle verification (rows per block).
/// Server provides this value; this is only a fallback.
const int _defaultMerkleBlockSize = 100;

/// Adapter to bridge SynclibDatabase to MerkleDatabase interface.
class _SynclibMerkleDb implements MerkleDatabase {
  final SynclibDatabase _db;
  _SynclibMerkleDb(this._db);

  @override
  Future<List<Map<String, dynamic>>> read(String sql) => _db.read(sql);
}

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

/// Event emitted when Merkle verification completes
/// Fires after background verification, especially useful when repairs occurred
class MerkleVerificationEvent {
  /// Tables that were repaired (empty if all matched)
  final List<String> repairedTables;

  /// Whether any repairs were made
  bool get hadMismatches => repairedTables.isNotEmpty;

  const MerkleVerificationEvent({
    required this.repairedTables,
  });
}

/// Direction of repair when data differs between client and server.
/// Applies to both seqnum-based sync and merkle tree verification.
enum RepairDirection {
  /// Server is authoritative. Client overwrites local data with server data.
  /// Use for: tribe/shared tables, read-only content.
  pull,

  /// Client is authoritative. Client sends its rows to server.
  /// Use for: user-owned tables (journal entries, measurements, etc.)
  push,

  /// Last-write-wins. Compare last_modified_ms per row;
  /// newer version overwrites in either direction.
  /// Use for: tables where both client and server can modify rows.
  lww,
}

/// Role of a channel in sync topology.
enum ChannelRole {
  /// Client pushes its own data on this channel (e.g., user channel).
  push,

  /// Client pulls shared data from this channel (e.g., tribe channel).
  pull,

  /// Channel supports both push and pull.
  bidirectional,
}

/// A table associated with a sync channel.
///
/// [direction] overrides the channel's default repair direction (derived from
/// [ChannelRole]) for this specific table. If null, the channel default is used.
class SyncTable {
  final String name;

  /// Per-table repair direction override. When null, defaults to the channel
  /// role's implied direction (push channel → push, pull channel → pull,
  /// bidirectional → lww).
  final RepairDirection? direction;

  /// Columns to include in merkle hash computation (besides id which is always
  /// included). When set, only these columns are hashed — skipping jsonb, array,
  /// and other problematic column types entirely. The precomputed row_hash fast
  /// path is bypassed since it includes all columns.
  ///
  /// When null, all columns are hashed (minus skipColumns/jsonbColumns).
  ///
  /// Example: `hashColumns: ['last_modified_ms']` — since last_modified_ms is
  /// updated by a trigger on every write, this is sufficient to detect changes.
  final List<String>? hashColumns;

  const SyncTable(this.name, {this.direction, this.hashColumns});
  const SyncTable.pull(this.name, {this.hashColumns}) : direction = RepairDirection.pull;
  const SyncTable.push(this.name, {this.hashColumns}) : direction = RepairDirection.push;
  const SyncTable.lww(this.name, {this.hashColumns}) : direction = RepairDirection.lww;
}

/// A sync channel: its topic, role, associated tables, and join params.
///
/// Used for both seqnum-based sync (which tables to push/pull) and merkle
/// tree verification (which tables to verify and how to repair).
///
/// Example:
/// ```dart
/// SyncChannel(
///   topic: 'sync:tribe:$tribeId',
///   role: ChannelRole.pull,
///   tables: [
///     SyncTable('exercises'),  // inherits pull from channel role
///     SyncTable('workouts'),
///   ],
/// )
/// ```
class SyncChannel {
  /// The Phoenix channel topic (e.g., "sync:tribe:trainer123", "sync:user:user456")
  final String topic;

  /// Role of this channel in the sync topology.
  final ChannelRole role;

  /// Tables on this channel. Used for both seqnum sync and merkle verification.
  /// Each table's repair direction defaults to the channel role if not overridden.
  final List<SyncTable> tables;

  /// Optional params to send when joining this channel.
  final Map<String, String>? params;

  const SyncChannel({
    required this.topic,
    required this.role,
    this.tables = const [],
    this.params,
  });

  /// Default repair direction implied by this channel's role.
  RepairDirection get defaultDirection {
    switch (role) {
      case ChannelRole.push:
        return RepairDirection.push;
      case ChannelRole.pull:
        return RepairDirection.pull;
      case ChannelRole.bidirectional:
        return RepairDirection.lww;
    }
  }

  /// Get the effective repair direction for a table, using the table's
  /// override if set, otherwise the channel's default.
  RepairDirection directionFor(SyncTable table) =>
      table.direction ?? defaultDirection;

  /// Get table names for a specific repair direction (accounting for defaults).
  List<String> tablesForDirection(RepairDirection d) =>
      tables.where((t) => directionFor(t) == d).map((t) => t.name).toList();

  /// Get all table names in this channel.
  List<String> get allTableNames => tables.map((t) => t.name).toList();
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

  /// Channels to connect to and sync on.
  /// Each channel defines its topic, role, tables, and optional join params.
  final List<SyncChannel> channels;

  /// Codec for message encoding
  final SyncCodecType codec;

  /// Batch size for pushing changes
  final int pushBatchSize;

  /// Conflict resolution strategy
  final ConflictResolver? onConflict;

  /// Optional metadata to send in hello message
  final Map<String, dynamic>? metadata;

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

  /// Whether to automatically sync stale tables on connect.
  /// If false, auto-sync can be triggered manually via [SyncClient.runAutoSync()].
  /// Defaults to true.
  final bool autoSyncOnConnect;

  /// Interval for periodic background sync.
  /// When set, a timer calls syncUnified() at this interval.
  /// When null, sync is purely reactive (syncOnWrite + manual calls).
  /// Defaults to null (disabled).
  final Duration? periodicSyncInterval;

  /// Interval for periodic Merkle integrity verification.
  /// When set, the client will periodically verify data integrity
  /// for the specified tables using Merkle tree comparison.
  /// Defaults to null (disabled).
  final Duration? merkleVerifyInterval;

  /// Columns to skip in Merkle hash computation (excluded from SELECT and hash).
  /// Always includes 'row_hash'. Add array columns here that the server skips
  /// via is_array_field? (e.g. triballeaders, subscribedto, participants).
  final List<String> merkleSkipColumns;

  const SyncClientConfig({
    required this.dbPath,
    required this.serverUrl,
    required this.clientId,
    required this.channels,
    this.codec = SyncCodecType.json,
    this.pushBatchSize = 100,
    this.onConflict,
    this.metadata,
    this.syncOnWrite = false,
    this.syncOnWriteDebounce = const Duration(milliseconds: 100),
    this.database,
    this.autoSyncOnConnect = true,
    this.periodicSyncInterval,
    this.merkleVerifyInterval,
    this.merkleSkipColumns = const ['row_hash'],
  });
}

/// Main sync client orchestrating bidirectional sync
class SyncClient {
  final SyncClientConfig config;
  final Logger _logger = Logger('SyncClient');

  late final WebSocketManager _ws;
  SynclibDatabase? _db;
  MerkleComputer? _merkle;
  /// Merkle block size from server (received in hello response)
  int? _serverMerkleBlockSize;
  /// Hash columns from server, applied to all tables (received in join response)
  List<String>? _serverHashColumns;
  StreamSubscription? _messageSubscription;
  StreamSubscription? _stateSubscription;

  /// Message processing lock — ensures messages are handled one at a time.
  Future<void> _messageLock = Future.value();

  bool _isInitialized = false;
  bool _hasConnectedOnce = false;
  int _lastSyncedSeqnum = 0;
  final Set<int> _pendingAcks = {};

  /// Get all sync table names from all channels.
  List<String> get _allSyncTables =>
      config.channels.expand((c) => c.allTableNames).toSet().toList();

  /// Get channels by role.
  Iterable<SyncChannel> get _pushChannels =>
      config.channels.where((c) => c.role == ChannelRole.push || c.role == ChannelRole.bidirectional);

  Iterable<SyncChannel> get _pullChannels =>
      config.channels.where((c) => c.role == ChannelRole.pull || c.role == ChannelRole.bidirectional);
  /// Track pending changes by local seqnum so we can update server seqnum on ack.
  /// Key is local seqnum from _synclib_changes, value is (table, rowId).
  final Map<int, ({String table, String rowId})> _pendingChangeInfo = {};

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

  // Stream controller for Merkle verification events (repairs after background verification)
  final StreamController<MerkleVerificationEvent> _merkleVerificationController = StreamController<MerkleVerificationEvent>.broadcast();

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

  // Periodic background sync timer
  Timer? _periodicSyncTimer;

  SyncReadyState _readyState = SyncReadyState.waitingForHello;

  // Hello handshake gate — completed when server reply to hello is fully processed
  Completer<void>? _helloHandshakeCompleter;

  SyncClient(this.config) {
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
    _merkle = MerkleComputer(
      _SynclibMerkleDb(_db!),
      skipColumns: config.merkleSkipColumns,
    );

    // Skip local hash computation — server computes authoritative row_hash
    await _db!.skipLocalHash(true);

    // One-time migration: switch to server-authoritative row_hash.
    // Set all local row_hash to '' (sentinel) so merkle detects mismatch
    // and triggers repair from server.
    await _migrateToServerAuthoritativeRowHash();

    // Subscribe to WebSocket messages (serialized via message lock)
    _messageSubscription = _ws.messages.listen(_enqueueMessage);
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

  /// One-time migration: switch to server-authoritative row_hash.
  /// Sets all local row_hash values to '' (sentinel) so merkle comparison
  /// detects mismatch and triggers repair from server.
  Future<void> _migrateToServerAuthoritativeRowHash() async {
    try {
      await _db!.exec('''
        CREATE TABLE IF NOT EXISTS _synclib_metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
      ''');

      final results = await _db!.read(
        "SELECT value FROM _synclib_metadata WHERE key = 'server_authoritative_row_hash'"
      );
      if (results.isNotEmpty) return; // Already migrated

      _logger.info('Migrating to server-authoritative row_hash');
      for (final tableName in _allSyncTables) {
        try {
          await _db!.exec("UPDATE \"$tableName\" SET row_hash = ''");
          _logger.info('Reset row_hash to sentinel for $tableName');
        } catch (e) {
          _logger.fine('Could not reset row_hash for $tableName: $e');
        }
      }

      await _db!.exec(
        "INSERT OR REPLACE INTO _synclib_metadata (key, value) VALUES ('server_authoritative_row_hash', '1')"
      );
      _logger.info('Server-authoritative row_hash migration complete');
    } catch (e) {
      _logger.warning('Failed to migrate row_hash: $e');
    }
  }

  /// Called when a local change occurs (for syncOnWrite).
  /// Uses debouncing to batch rapid writes together.
  void _onLocalChange() {
    if (!_ws.isConnected) return;

    // Cancel any existing debounce timer
    _syncOnWriteDebounceTimer?.cancel();

    // Start a new debounce timer
    _syncOnWriteDebounceTimer = Timer(config.syncOnWriteDebounce, () {
      _logger.fine('syncOnWrite: syncing after debounce');
      syncUnified().catchError((e) {
        _logger.warning('syncOnWrite: syncUnified failed: $e');
      });
    });
  }

  /// Start periodic background sync timer if configured.
  void _startPeriodicSync() {
    _stopPeriodicSync();
    final interval = config.periodicSyncInterval;
    if (interval == null) return;
    _logger.info('Starting periodic sync every $interval');
    _periodicSyncTimer = Timer.periodic(interval, (_) {
      syncUnified().catchError((e) {
        _logger.warning('Periodic sync failed: $e');
      });
    });
  }

  /// Stop periodic background sync timer.
  void _stopPeriodicSync() {
    _periodicSyncTimer?.cancel();
    _periodicSyncTimer = null;
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

    await _ws.connect(
      params: {
        'token': token,
        'client_id': config.clientId,
      },
    );
    await _joinChannels();
  }

  /// Join all configured channels
  Future<void> _joinChannels() async {
    // Clear channel states for fresh connection
    _channelStates.clear();

    // Get per-table seqnums if auto-sync is configured
    final syncTables = _allSyncTables;
    Map<String, int>? tableSeqnums;
    if (syncTables.isNotEmpty) {
      tableSeqnums = await _getPerTableSeqnums(syncTables);
      _logger.info('Sending table seqnums on join: $tableSeqnums');
    }

    // First, join ALL channels (don't wait for auto-sync)
    // This prevents timeouts from one channel's sync blocking the next join
    final joinResponses = <String, Map<String, dynamic>>{};

    for (final channel in config.channels) {
      final topic = channel.topic;
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

      // Extract server-driven hash_columns from join response
      _extractServerHashColumns(response);

      // Mark channel as joined
      _channelStates[topic]!.joined = true;
      joinResponses[topic] = response;
    }

    _logger.info('All channels joined successfully');
    _hasConnectedOnce = true;

    // Configure synclibc with server-driven hash columns (enables precomputed row_hash)
    await _configureHashColumns();

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

    // Start periodic background sync if configured
    _startPeriodicSync();
  }

  /// Handle join response from server, including stale tables
  Future<void> _handleJoinResponse(Map<String, dynamic> response, String channelTopic) async {
    final channelState = _channelStates[channelTopic];

    // Skip if no sync tables configured - mark channel as ready
    if (_allSyncTables.isEmpty) {
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
  /// Tables to sync are derived from the channel's configured tables.
  /// If stale tables are found, they are automatically synced and an
  /// [AutoSyncEvent] is emitted on [autoSyncEvents].
  ///
  /// Example:
  /// ```dart
  /// await syncClient.joinChannel(
  ///   SyncChannel(
  ///     topic: 'sync:user:$userId',
  ///     role: ChannelRole.push,
  ///     tables: [SyncTable('videos'), SyncTable('users')],
  ///   ),
  /// );
  /// ```
  Future<void> joinChannel(SyncChannel channel) async {
    if (!_ws.isConnected) {
      throw StateError('Not connected. Call connect() first.');
    }

    final topic = channel.topic;
    _logger.info('Joining additional channel: $topic');

    // Create channel state for this dynamic channel
    _channelStates[topic] = ChannelSyncState(topic: topic);

    // Get per-table seqnums if channel has tables
    final autoSyncTables = channel.allTableNames;
    Map<String, int>? tableSeqnums;
    if (autoSyncTables.isNotEmpty) {
      tableSeqnums = await _getPerTableSeqnums(autoSyncTables);
      _logger.info('Sending table seqnums on join: $tableSeqnums');
    }

    final response = await _ws.joinChannel(topic, {
      'client_id': config.clientId,
      ...?channel.params,
      if (tableSeqnums != null) 'table_seqnums': tableSeqnums,
    });

    // Extract server-driven hash_columns from join response
    _extractServerHashColumns(response);

    // Configure synclibc with server-driven hash columns
    await _configureHashColumns();

    // Mark channel as joined
    _channelStates[topic]!.joined = true;
    _logger.info('Successfully joined channel: $topic');

    // Handle stale tables from server response (auto-sync if tables specified)
    await _handleJoinResponseDynamic(response, topic, autoSyncTables.isEmpty ? null : autoSyncTables);
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
  ///   SyncChannel(topic: 'sync:user:$userId', role: ChannelRole.push),
  /// )) {
  ///   _logger.info('Already joined');
  /// }
  /// ```
  bool isChannelJoined(SyncChannel channel) {
    return _ws.isChannelJoined(channel.topic);
  }

  /// Get all currently joined channel topics
  List<String> get joinedChannels => _ws.joinedChannels;

  /// Leave a channel
  ///
  /// Example:
  /// ```dart
  /// await syncClient.leaveChannel(
  ///   SyncChannel(topic: 'sync:user:anon', role: ChannelRole.push),
  /// );
  /// ```
  Future<void> leaveChannel(SyncChannel channel) async {
    _logger.info('Leaving channel: ${channel.topic}');
    await _ws.leaveChannel(channel.topic);
    _logger.info('Left channel: ${channel.topic}');
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
    late final StreamSubscription<ConnectionState> sub;

    sub = _ws.stateChanges.listen((state) {
      if (state == ConnectionState.connected) {
        sub.cancel();
        if (!completer.isCompleted) completer.complete();
      } else if (state == ConnectionState.failed || state == ConnectionState.authFailed) {
        sub.cancel();
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
      await sub.cancel();
    }
  }

  /// Manually trigger a sync cycle.
  /// Calls syncUnified() which handles push, pull, schema, and stripped content.
  Future<void> sync() async {
    await syncUnified();
  }

  // ==========================================================================
  // SIMPLIFIED SYNC HANDSHAKE - New unified sync implementation
  // ==========================================================================

  /// Ensure the stripped-row tracking table exists.
  Future<void> _ensureStrippedAckTable() async {
    await _db!.exec('''
      CREATE TABLE IF NOT EXISTS _synclib_stripped_ack (
        "table" TEXT NOT NULL,
        row_id TEXT NOT NULL,
        acked_at INTEGER NOT NULL,
        PRIMARY KEY ("table", row_id)
      )
    ''');
  }

  /// Record rows that came back still stripped after a refresh request.
  /// These won't be re-requested until access changes (purchase, etc.).
  Future<void> _ackStrippedRows(String table, List<String> rowIds) async {
    if (rowIds.isEmpty) return;
    await _ensureStrippedAckTable();
    final now = DateTime.now().millisecondsSinceEpoch;
    for (final rowId in rowIds) {
      await _db!.exec(
        "INSERT OR REPLACE INTO _synclib_stripped_ack (\"table\", row_id, acked_at) "
        "VALUES ('${_escapeSql(table)}', '${_escapeSql(rowId)}', $now)"
      );
    }
    _logger.info('Acked ${rowIds.length} still-stripped rows in $table (will skip on next sync)');
  }

  /// Clear stripped-ack records for given tables so they get re-requested.
  /// Call this after a purchase or access change.
  Future<void> clearStrippedAck([List<String>? tables]) async {
    await _ensureStrippedAckTable();
    if (tables == null || tables.isEmpty) {
      await _db!.exec('DELETE FROM _synclib_stripped_ack');
      _logger.info('Cleared all stripped-ack records');
    } else {
      for (final table in tables) {
        await _db!.exec("DELETE FROM _synclib_stripped_ack WHERE \"table\" = '${_escapeSql(table)}'");
      }
      _logger.info('Cleared stripped-ack records for: ${tables.join(', ')}');
    }
  }

  /// Find all rows that have _stripped=true in their document
  /// These rows need to be refreshed to get full content.
  /// Excludes rows that were already refreshed and came back still stripped.
  Future<List<RowRef>> _findStrippedRows(List<String> tables) async {
    final rows = <RowRef>[];
    await _ensureStrippedAckTable();

    for (final table in tables) {
      try {
        // First check how many rows exist in the table
        final countResult = await _db!.read('SELECT COUNT(*) as cnt FROM ${_quoteId(table)}');
        final totalRows = countResult.isNotEmpty ? countResult.first['cnt'] as int : 0;

        // Check for NULL documents specifically
        // Note: JSONB columns store null as binary bytes, not SQL NULL
        // So we check both SQL NULL and json(document) = 'null'
        final nullDocResult = await _db!.read('''
          SELECT COUNT(*) as cnt FROM ${_quoteId(table)}
          WHERE "document" IS NULL
             OR json("document") IS NULL
             OR json("document") = 'null'
        ''');
        final nullDocCount = nullDocResult.isNotEmpty ? nullDocResult.first['cnt'] as int : 0;

        // Stripped content has NULL/null document OR _stripped flag in document
        // Exclude rows already acked as intentionally stripped
        final result = await _db!.read('''
          SELECT t."id" FROM ${_quoteId(table)} t
          LEFT JOIN _synclib_stripped_ack sa
            ON sa."table" = '${_escapeSql(table)}' AND sa.row_id = t."id"
          WHERE sa.row_id IS NULL
            AND (
              t."document" IS NULL
              OR json(t."document") IS NULL
              OR json(t."document") = 'null'
              OR json_extract(t."document", '\$._stripped') = 1
              OR json_extract(t."document", '\$._stripped') = true
            )
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
          SELECT COUNT(*) as cnt FROM ${_quoteId(table)}
          WHERE "document" IS NOT NULL
            AND typeof("document") = 'text'
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
  /// [forceRefresh] - Tables to force refresh (ignore seqnums)
  /// [includeStripped] - Auto-detect and refresh stripped rows (default: true)
  /// [cleanupLegacyChanges] - Clean up old synced=1 records from legacy sync (default: true on first run)
  Future<void> syncUnified({
    List<String>? forceRefresh,
    bool includeStripped = true,
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
      final tablesToSync = _allSyncTables;
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

      // 5. Push pending changes to push channels
      String? streamId;
      final pushStreamIds = <String>[];
      if (pendingChangeMessages.isNotEmpty) {
        for (final channel in _pushChannels) {
          // Filter pending changes to this channel's tables
          final channelTableSet = channel.allTableNames.toSet();
          final channelChanges = pendingChangeMessages
              .where((c) => channelTableSet.contains(c.table))
              .toList();
          if (channelChanges.isEmpty) continue;

          _logger.info('syncUnified: Pushing ${channelChanges.length} changes on ${channel.topic}');
          final pushRequest = SyncRequestMessage(
            clientId: config.clientId,
            schemaVersion: schemaVersion,
            tableSeqnums: {},
            tables: [],
            pendingChanges: channelChanges,
            role: channel.role.name,
          );

          final pushResponse = await _ws.sendRaw('sync', pushRequest.toMap(), channelTopic: channel.topic);

          // sendRaw() already checks isOk and unwraps the response payload,
          // so pushResponse is {stream_id: "..."}, not {status: "ok", response: {...}}
          streamId = pushResponse['stream_id'] as String?;
          _logger.info('syncUnified: Push complete on ${channel.topic}, stream_id=$streamId');

          // Store pending changes for ack processing, and register a
          // completer so the push stream_id is recognized in
          // _handleSyncComplete (preventing the fallback else-branch
          // from clearing _activeSyncPendingChanges before ACKs arrive).
          if (streamId != null) {
            _activeSyncPendingChanges[streamId] = pendingChanges;
            _activeSyncCompleters[streamId] = Completer<void>();
            pushStreamIds.add(streamId);
          }
        }
      }

      // 5b. Wait for all push streams to complete (ACKs processed, row_hash stored)
      // before starting pull. This ensures:
      //   - Push ACKs with row_hash are applied before pull data could overwrite them
      //   - The pull sees all just-pushed items on the server
      //   - Table seqnums are properly updated from the pull
      for (final pushSid in pushStreamIds) {
        final pushCompleter = _activeSyncCompleters[pushSid];
        if (pushCompleter != null && !pushCompleter.isCompleted) {
          _logger.info('syncUnified: Waiting for push stream $pushSid to complete');
          await pushCompleter.future.timeout(
            const Duration(seconds: 30),
            onTimeout: () {
              _logger.warning('syncUnified: Push stream $pushSid timed out');
              _activeSyncCompleters.remove(pushSid);
              _activeSyncPendingChanges.remove(pushSid);
            },
          );
        }
      }

      // 6. Pull data from each channel (all channels may have data to pull)
      for (final channel in config.channels) {
        final channelTableNames = channel.allTableNames;
        if (channelTableNames.isEmpty) continue;

        // Build per-channel table seqnums and force refresh
        final channelTableSet = channelTableNames.toSet();
        final channelSeqnums = Map.fromEntries(
          tableSeqnums.entries.where((e) => channelTableSet.contains(e.key))
        );
        final channelForceRefresh = effectiveForceRefresh
            .where((t) => channelTableSet.contains(t))
            .toList();
        final channelStrippedRows = strippedRows
            ?.where((r) => channelTableSet.contains(r.table))
            .toList();

        _logger.info('syncUnified: Pulling ${channelTableNames.join(', ')} on ${channel.topic} (schema v$schemaVersion)');
        final pullRequest = SyncRequestMessage(
          clientId: config.clientId,
          schemaVersion: schemaVersion,
          tableSeqnums: channelSeqnums,
          tables: channelTableNames,
          forceRefreshTables: channelForceRefresh.isNotEmpty ? channelForceRefresh : null,
          strippedRows: channelStrippedRows?.isNotEmpty == true ? channelStrippedRows : null,
          pendingChanges: null,
          role: channel.role.name,
        );

        final response = await _ws.sendRaw('sync', pullRequest.toMap(), channelTopic: channel.topic);

        // Check if schema upgrade is required
        if (response['status'] == 'schema_upgrade_required') {
          final targetVersion = response['target_version'] as int;
          _logger.info('syncUnified: Schema upgrade required to v$targetVersion');
          _emitSyncProgress(const SyncProgress(phase: 'migrating'));

          await _applyMigrations({
            'current_version': targetVersion,
            'migrations': response['migrations'],
          });

          _logger.info('syncUnified: Schema upgraded to v$targetVersion, restarting sync');

          _isSyncing = false;
          return syncUnified(
            forceRefresh: forceRefresh,
            includeStripped: includeStripped,
            cleanupLegacyChanges: false,
          );
        }

        if (response['status'] == 'error') {
          throw StateError('Sync error on ${channel.topic}: ${response['error']}');
        }

        // Wait for this channel's pull stream to complete
        final pullStreamId = response['stream_id'] as String?;
        if (pullStreamId == null) {
          throw StateError('Server did not return stream_id for ${channel.topic}');
        }

        _logger.info('syncUnified: Got pull stream_id $pullStreamId for ${channel.topic}, waiting for data');
        streamId = pullStreamId;

        final completer = Completer<void>();
        _activeSyncCompleters[streamId] = completer;

        await completer.future.timeout(
          const Duration(seconds: 60),
          onTimeout: () {
            _activeSyncCompleters.remove(streamId);
            _activeSyncPendingChanges.remove(streamId);
            throw TimeoutException('Unified sync timed out on ${channel.topic}', const Duration(seconds: 60));
          },
        );
      }

      // Check if Merkle verification is needed (staleness check)
      // Must be awaited so it runs within the _isSyncing guard,
      // preventing concurrent bulk mode with a subsequent syncUnified call.
      await _checkMerkleVerification();

      // Cancel any syncOnWrite debounce triggered by merkle repair writes —
      // those are internal maintenance, not user changes needing a new sync.
      _syncOnWriteDebounceTimer?.cancel();

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

        // Look up the change info in pending changes to apply server seqnum and row_hash
        if (ack.serverSeqnum != null || ack.rowHash != null) {
          for (final pending in _activeSyncPendingChanges.values) {
            final change = pending.where((c) => c.seqnum == ack.localSeqnum).firstOrNull;
            if (change != null) {
              if (ack.serverSeqnum != null) {
                await _updateLocalSeqnum(change.tableName, change.rowId, ack.serverSeqnum!);
              }
              if (ack.rowHash != null) {
                await _updateLocalRowHash(change.tableName, change.rowId, ack.rowHash!);
              }
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
      int skippedCount = 0;
      // Track rows that came back still stripped after a refresh request
      final stillStrippedIds = <String>[];

      try {
        for (final row in message.rows) {
          try {
            final deletedAt = row['deleted_at'];

            if (deletedAt != null) {
              // Soft-deleted row - delete locally
              final rowId = row['id'] as String;
              final deleteSql = "DELETE FROM ${_quoteId(message.table)} WHERE \"id\" = '${_escapeSql(rowId)}'";
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

              // If this was a stripped refresh and the row is still stripped,
              // track it so we don't re-request it next sync
              if (message.isStrippedRefresh) {
                final doc = row['document'];
                final isStillStripped = doc is Map && doc['_stripped'] == true;
                if (isStillStripped) {
                  stillStrippedIds.add(row['id'] as String);
                }
              }
            }
          } catch (rowErr) {
            skippedCount++;
            _logger.severe('Skipping bad row in ${message.table} (id=${row['id']}): $rowErr');
          }
        }
        await _db!.endBulkRemote();
        // row_hash is now server-authoritative — included in row data from server
        if (skippedCount > 0) {
          _logger.warning('Skipped $skippedCount bad rows in ${message.table}');
        }
        _logger.info('Applied sync batch for ${message.table}');

        // Ack rows that are confirmed still stripped (user has no access)
        if (stillStrippedIds.isNotEmpty) {
          await _ackStrippedRows(message.table, stillStrippedIds);
        }
      } catch (e) {
        _logger.severe('Failed to apply sync batch for ${message.table}: $e');
        await _db!.endBulkRemote(rollback: true);
      }
    });
  }

  /// Handle sync complete from unified sync
  /// Waits for all pending batch operations to complete before signaling done.
  Future<void> _handleSyncComplete(SyncCompleteMessage message) async {
    _logger.info('syncUnified: Sync complete message received - stream ${message.streamId}, '
        'schema v${message.schemaVersion}, '
        'pushed ${message.pushSuccess}/${message.pushTotal}, '
        'pulled ${message.pullTotal} rows, '
        'seqnums: ${message.tableSeqnums}');

    // CRITICAL: Wait for all pending batch operations to complete before
    // signaling sync is done. Without this, merkle verification can run
    // before all DB writes finish, causing false hash mismatches.
    await _batchLock.synchronized(() async {
      _logger.info('syncUnified: All batch operations complete');
    });

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

  /// Send hello message to server
  /// Extract server-driven hash_columns from a channel join response.
  void _extractServerHashColumns(Map<String, dynamic> response) {
    final serverHashCols = response['hash_columns'];
    if (serverHashCols is List && serverHashCols.isNotEmpty) {
      _serverHashColumns = serverHashCols.cast<String>();
      _logger.info('Server hash_columns: ${_serverHashColumns!.join(', ')}');
    }
  }

  /// Configure synclibc with hash column metadata from the server.
  ///
  /// Note: With server-authoritative row_hash (the default), calling
  /// setHashColumns() on the native library is no longer needed — the server
  /// computes row_hash via Postgres triggers and sends it in ACK messages and
  /// row data. The row_hash column is created by server-sent DDL migrations.
  ///
  /// The native setHashColumns() is only useful if you want optional client-side
  /// hash generation (e.g., for local data integrity checks without a server).
  /// It is intentionally not called here to avoid conflicts with server-managed
  /// schema (duplicate column errors).
  ///
  /// _serverHashColumns is still used by merkle repair to tell the server which
  /// columns to hash when computing comparison trees.
  Future<void> _configureHashColumns() async {
    // _serverHashColumns is extracted from the join response and used by merkle
    // repair — no native-side configuration needed for server-authoritative mode.
  }

  Future<void> _sendHello() async {
    // Get current schema version from database
    final schemaVersion = await _db!.getSchemaVersion();

    final hello = HelloMessage(
      clientId: config.clientId,
      lastSeqnum: _lastSyncedSeqnum,
      schemaVersion: schemaVersion,
      metadata: config.metadata,
    );

    // Create a gate so connect() doesn't return until the server reply
    // (which may trigger migrations) is fully processed
    _helloHandshakeCompleter = Completer<void>();

    await _ws.send(hello, channelTopic: config.channels.first.topic);
    _logger.info('Sent hello message with schema version $schemaVersion');

    await _helloHandshakeCompleter!.future;
  }

  /// Enqueue a message for serialized processing.
  /// Prevents interleaving when multiple async messages arrive rapidly.
  void _enqueueMessage(SyncMessage message) {
    _messageLock = _messageLock.then((_) => _handleMessage(message));
  }

  /// Handle incoming message from server
  Future<void> _handleMessage(SyncMessage message) async {
    try {
      if (message is ChangeMessage) {
        await _applyRemoteChange(message);
      } else if (message is ChangesBatchMessage) {
        await _applyRemoteChanges(message.changes);
      } else if (message is AckMessage) {
        await _handleAck(message);
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
        await _handleSyncComplete(message);
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
        final deleteSql = "DELETE FROM ${_quoteId(change.table)} WHERE \"id\" = '${_escapeSql(change.rowId)}'";
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
      int skippedCount = 0;
      try {
        for (final change in changes) {
          try {
            // Check if this is a soft-deleted row - delete locally instead of inserting
            if (change.data?['deleted_at'] != null) {
              final deleteSql = "DELETE FROM ${_quoteId(change.table)} WHERE \"id\" = '${_escapeSql(change.rowId)}'";
              await _db!.execBulkRemote(deleteSql);
              deletedCount++;
            } else {
              final sql = _generateSql(change);
              await _db!.execBulkRemote(sql);
            }
          } catch (rowErr) {
            skippedCount++;
            _logger.severe('Skipping bad row in ${change.table} (id=${change.rowId}): $rowErr');
          }
        }
        await _db!.endBulkRemote();
        // row_hash is now server-authoritative — included in row data from server
        if (deletedCount > 0) {
          _logger.info('Soft-deleted $deletedCount rows locally');
        }
        if (skippedCount > 0) {
          _logger.warning('Skipped $skippedCount bad rows in changes batch');
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
  Future<void> _handleAck(AckMessage ack) async {
    _logger.fine('Received ack for seqnum ${ack.seqnum}: ${ack.success}, server_seqnum: ${ack.serverSeqnum}');

    if (ack.success) {
      _pendingAcks.remove(ack.seqnum);

      // Get the change info for this local seqnum
      final changeInfo = _pendingChangeInfo.remove(ack.seqnum);

      // Mark as synced in local database
      await _db!.markSynced(ack.seqnum);

      // Update the local row's seqnum column with the server-assigned seqnum
      if (ack.serverSeqnum != null && changeInfo != null) {
        await _updateLocalSeqnum(changeInfo.table, changeInfo.rowId, ack.serverSeqnum!);
      }

      // Store server-computed row_hash locally
      if (ack.rowHash != null && changeInfo != null) {
        await _updateLocalRowHash(changeInfo.table, changeInfo.rowId, ack.rowHash!);
      }
    } else {
      _logger.warning('Change ${ack.seqnum} failed: ${ack.error}');
      _pendingChangeInfo.remove(ack.seqnum);
    }
  }

  /// Update the seqnum column on a local row after server assigns it
  Future<void> _updateLocalSeqnum(String table, String rowId, int serverSeqnum) async {
    try {
      final sql = "UPDATE ${_quoteId(table)} SET \"seqnum\" = $serverSeqnum WHERE \"id\" = '${rowId.replaceAll("'", "''")}'";
      await _db!.exec(sql);
      _logger.fine('Updated local seqnum for $table:$rowId to $serverSeqnum');
    } catch (e) {
      // Table might not have seqnum column - this is fine for some tables
      _logger.fine('Could not update seqnum for $table:$rowId (table may not have seqnum column): $e');
    }
  }

  /// Update the row_hash column on a local row with the server-computed value
  Future<void> _updateLocalRowHash(String table, String rowId, String rowHash) async {
    try {
      final escapedRowId = rowId.replaceAll("'", "''");
      final escapedHash = rowHash.replaceAll("'", "''");
      final sql = "UPDATE ${_quoteId(table)} SET \"row_hash\" = '$escapedHash' WHERE \"id\" = '$escapedRowId'";
      await _db!.exec(sql);
      _logger.fine('Updated local row_hash for $table:$rowId');
    } catch (e) {
      _logger.fine('Could not update row_hash for $table:$rowId: $e');
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
          final deleteSql = "DELETE FROM ${_quoteId(batch.table)} WHERE \"id\" = '${_escapeSql(rowId)}'";
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
          // DEBUG: Log first row SQL to verify column names are lowercase
          if (processedCount == 0) {
            _logger.warning('DEBUG: First row SQL for ${batch.table}: $sql');
          }
          await _db!.execBulkRemote(sql);
        }
        processedCount++;
      }
      await _db!.endBulkRemote();
      // row_hash is now server-authoritative — included in row data from server

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

    // Store merkle_block_size from server if provided
    if (response['merkle_block_size'] != null) {
      _serverMerkleBlockSize = response['merkle_block_size'] as int;
      _logger.info('Server merkle_block_size: $_serverMerkleBlockSize');
    }

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
    } else if (response['status'] == 'error') {
      final error = response['error'] ?? 'unknown';
      _logger.severe('Hello error from server: $error '
          '(server_version: ${response['server_version']}, '
          'client_version: ${response['client_version']})');
      // Don't throw — syncUnified will also detect schema mismatch and throw
      // a catchable StateError. Logging here ensures the hello error is visible.
    }

    // Resolve the hello handshake gate so connect() can proceed
    if (_helloHandshakeCompleter != null && !_helloHandshakeCompleter!.isCompleted) {
      _helloHandshakeCompleter!.complete();
      _helloHandshakeCompleter = null;
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

      await _db!.exec('BEGIN TRANSACTION');
      try {
        // Execute each SQL statement
        for (final sql in sqlStatements) {
          _logger.info('Executing: $sql');
          try {
            await _db!.exec(sql as String);
          } catch (e) {
            // Handle idempotent DDL operations (e.g., duplicate column from prior partial migration)
            final msg = e.toString().toLowerCase();
            if (msg.contains('duplicate column') || msg.contains('already exists')) {
              _logger.info('Skipping already-applied DDL: $sql');
            } else {
              rethrow;
            }
          }
        }

        // Update schema version
        await _db!.setSchemaVersion(version);
        await _db!.exec('COMMIT');
        _logger.info('Successfully applied migration v$version');
      } catch (e, stack) {
        await _db!.exec('ROLLBACK');
        _logger.severe('Failed to apply migration v$version: $e', e, stack);
        rethrow;
      }
    }

    // Confirm migration to server
    final confirm = SchemaConfirmMessage(version: currentVersion);
    await _ws.send(confirm, channelTopic: config.channels.first.topic);

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
        _stopPeriodicSync();
        // Fail any pending sync completers so syncUnified() doesn't hang
        // waiting for a sync_complete that will never arrive on the dead connection
        if (_activeSyncCompleters.isNotEmpty) {
          _logger.warning('Connection lost with ${_activeSyncCompleters.length} pending sync completers — failing them');
          for (final entry in _activeSyncCompleters.entries) {
            if (!entry.value.isCompleted) {
              entry.value.completeError(
                StateError('WebSocket disconnected during sync (stream ${entry.key})'),
              );
            }
          }
          _activeSyncCompleters.clear();
          _activeSyncPendingChanges.clear();
        }
        _updateSyncState(SyncState.disconnected);
        break;
      case ConnectionState.authFailed:
        _updateSyncState(SyncState.error);
        break;
    }
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

    final qt = _quoteId(change.table);

    switch (change.operation) {
      case 'insert':
        // Use INSERT ON CONFLICT DO UPDATE to only update provided columns.
        // INSERT OR REPLACE wipes all non-provided columns to NULL which
        // destroys data like last_modified_ms when broadcasts send partial rows.
        final columns = ['"id"', ...filteredData.keys.map((k) => _quoteId(k))].join(', ');
        final values = ['\'${_escapeSql(change.rowId)}\'', ...filteredData.values.map((v) => _formatSqlValue(v))].join(', ');
        final updates = filteredData.entries
          .map((e) => '${_quoteId(e.key)} = ${_formatSqlValue(e.value)}')
          .join(', ');
        if (filteredData.isEmpty) {
          return 'INSERT OR IGNORE INTO $qt ("id") VALUES (\'${_escapeSql(change.rowId)}\')';
        }
        return 'INSERT INTO $qt ($columns) VALUES ($values) '
            'ON CONFLICT("id") DO UPDATE SET $updates';
      case 'update':
        final sets = filteredData.entries
          .map((e) => '${_quoteId(e.key)} = ${_formatSqlValue(e.value)}')
          .join(', ');
        return 'UPDATE $qt SET $sets WHERE "id" = \'${_escapeSql(change.rowId)}\'';

      case 'delete':
        return 'DELETE FROM $qt WHERE "id" = \'${_escapeSql(change.rowId)}\'';

      default:
        throw ArgumentError('Unknown operation: ${change.operation}');
    }
  }

  /// Generate SQL with parameters for JSONB data
  ({String sql, List<String?> params}) _generateSqlWithParams(ChangeMessage change) {
    final data = change.data as Map<String, dynamic>;
    final params = <String?>[];

    // Filter out metadata fields and normalize COLUMN NAMES to lowercase
    // PostgreSQL identifiers are lowercase by default, so our schema has userid, createdat, etc.
    // But the JSON keys from server are camelCase like userId, createdAt
    // Note: This only affects column names - the content inside 'document' is jsonEncoded
    // and preserves its internal camelCase keys (e.g., document: {"someField": "value"})
    final filteredData = <String, dynamic>{};
    for (final entry in data.entries) {
      final key = entry.key;
      final value = entry.value;

      if (key == 'id' || key == 'updated_at' || key == 'inserted_at') continue;

      // Normalize column name to lowercase to match schema (values stay as-is)
      filteredData[key.toLowerCase()] = value;
    }

    final qt = _quoteId(change.table);

    switch (change.operation) {
      case 'insert':
      case 'upsert':
        final columns = ['"id"', ...filteredData.keys.map((k) => _quoteId(k))].join(', ');

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

        // Build parameters array for INSERT values
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

        // Use INSERT ON CONFLICT DO UPDATE to only update provided columns.
        // INSERT OR REPLACE wipes all non-provided columns to NULL which
        // destroys data like last_modified_ms when broadcasts send partial rows.
        if (filteredData.isEmpty) {
          final insertSql = 'INSERT OR IGNORE INTO $qt ("id") VALUES (?)';
          return (sql: insertSql, params: [change.rowId]);
        }
        final updateClauses = <String>[];
        for (final entry in filteredData.entries) {
          if (entry.value is Map || entry.value is List) {
            updateClauses.add('${_quoteId(entry.key)} = jsonb(?)');
            params.add(jsonEncode(entry.value));
          } else if (entry.value is bool) {
            updateClauses.add('${_quoteId(entry.key)} = ?');
            params.add(entry.value ? '1' : '0');
          } else {
            updateClauses.add('${_quoteId(entry.key)} = ?');
            params.add(entry.value?.toString());
          }
        }
        final insertSql = 'INSERT INTO $qt ($columns) VALUES ($placeholders) '
            'ON CONFLICT("id") DO UPDATE SET ${updateClauses.join(', ')}';
        return (sql: insertSql, params: params);

      case 'update':
        final setClauses = <String>[];
        for (final entry in filteredData.entries) {
          if (entry.value is Map || entry.value is List) {
            setClauses.add('${_quoteId(entry.key)} = jsonb(?)');
            params.add(jsonEncode(entry.value));
          } else if (entry.value is bool) {
            setClauses.add('${_quoteId(entry.key)} = ?');
            params.add(entry.value ? '1' : '0');
          } else {
            setClauses.add('${_quoteId(entry.key)} = ?');
            params.add(entry.value?.toString());
          }
        }

        final updateSql = 'UPDATE $qt SET ${setClauses.join(', ')} WHERE "id" = ?';
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

  /// Quote a SQL identifier (table or column name) to prevent SQL injection.
  String _quoteId(String name) => '"${name.replaceAll('"', '""')}"';

  /// Escape single quotes in SQL strings
  String _escapeSql(String value) {
    return value.replaceAll("'", "''");
  }

  /// Build a SELECT clause for repair that wraps JSONB/BLOB columns with json().
  /// Avoids SELECT * which triggers binary-data warnings for BLOB columns.
  Future<String> _buildRepairSelectSql(String table) async {
    final qt = _quoteId(table);
    final pragmaResult = await _db!.read('PRAGMA table_info($qt)');
    if (pragmaResult.isEmpty) return 'SELECT * FROM $qt';
    final columns = pragmaResult.map((row) {
      final colName = row['name'] as String;
      final colType = (row['type'] as String? ?? '').toUpperCase();
      if (colName == 'row_hash') return null;
      final qc = _quoteId(colName);
      // Wrap BLOB/JSONB columns with json() to avoid binary data warnings
      if (colType.contains('BLOB') || colType.contains('JSONB')) {
        return 'json($qc) as $qc';
      }
      return qc;
    }).where((c) => c != null).join(', ');
    return 'SELECT $columns FROM $qt';
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
      final result = await _db!.read('SELECT MAX("seqnum") as max_seqnum FROM ${_quoteId(table)}');

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

  /// Stream of Merkle verification events
  /// Emitted when background Merkle verification completes, especially if repairs occurred.
  /// Subscribe to this to invalidate caches/refs when data was repaired.
  Stream<MerkleVerificationEvent> get merkleVerificationEvents => _merkleVerificationController.stream;

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

  // ==========================================================================
  // MERKLE TREE INTEGRITY VERIFICATION
  // ==========================================================================

  /// Check if Merkle verification is needed based on staleness.
  /// Called at the end of syncUnified to run verification if interval has elapsed.
  Future<void> _checkMerkleVerification() async {
    // Determine if any tables are configured for verification
    final hasChannels = config.channels.any((c) => c.tables.isNotEmpty);

    if (config.merkleVerifyInterval == null || !hasChannels) {
      return;
    }

    try {
      final now = DateTime.now().millisecondsSinceEpoch;

      // Ensure metadata table exists (for merkle verification tracking)
      await _db!.exec('''
        CREATE TABLE IF NOT EXISTS _synclib_metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
      ''');

      // Read last verified timestamp from metadata
      final results = await _db!.read(
        "SELECT value FROM _synclib_metadata WHERE key = 'merkle_last_verified'"
      );
      int lastVerified = 0;
      if (results.isNotEmpty) {
        final value = results.first['value'];
        if (value is int) {
          lastVerified = value;
        } else if (value is String) {
          lastVerified = int.tryParse(value) ?? 0;
        }
      }

      // On first run (no timestamp), skip verification but set the timestamp.
      // Seqnum-based sync is sufficient for initial population.
      if (lastVerified == 0) {
        _logger.info('Merkle verification: first run - skipping, setting initial timestamp');
        await _db!.exec(
          "INSERT OR REPLACE INTO _synclib_metadata (key, value) VALUES ('merkle_last_verified', '$now')"
        );
        return;
      }

      final intervalMs = config.merkleVerifyInterval!.inMilliseconds;
      if (now - lastVerified < intervalMs) {
        _logger.info('Merkle verification not needed (last: ${DateTime.fromMillisecondsSinceEpoch(lastVerified)})');
        return;
      }

      _logger.info('Running Merkle verification (last: ${DateTime.fromMillisecondsSinceEpoch(lastVerified)})');

      final blockSize = _serverMerkleBlockSize ?? _defaultMerkleBlockSize;
      final allRepairedTables = <String>[];

      await _merkleVerifyFromChannels(config.channels, blockSize, allRepairedTables);

      // Update last verified timestamp in metadata
      await _db!.exec(
        "INSERT OR REPLACE INTO _synclib_metadata (key, value) VALUES ('merkle_last_verified', '$now')"
      );

      if (allRepairedTables.isNotEmpty) {
        _logger.info('Merkle verification repaired tables: ${allRepairedTables.join(', ')}');
      } else {
        _logger.info('Merkle verification: all tables match');
      }

      // Emit event so clients can invalidate caches if repairs occurred
      _merkleVerificationController.add(MerkleVerificationEvent(
        repairedTables: allRepairedTables,
      ));
    } catch (e) {
      _logger.warning('Merkle verification failed: $e');
      // Don't rethrow - verification failure shouldn't break sync
    }
  }

  /// Merkle verification using the new channels config.
  /// Verifies all tables per channel, then dispatches repair by direction.
  Future<void> _merkleVerifyFromChannels(
    List<SyncChannel> channels,
    int blockSize,
    List<String> allRepairedTables,
  ) async {
    for (final channel in channels) {
      if (channel.tables.isEmpty) continue;

      final allTables = channel.allTableNames;
      _logger.fine('Merkle verification on ${channel.topic}: ${allTables.join(', ')}');

      // 1. Verify root hashes — wrap in timeout so a slow server response
      //    doesn't block the entire sync for 10s+.
      List<MerkleMismatch> mismatches;
      try {
        mismatches = await _verifyRoots(
          tables: allTables,
          blockSize: blockSize,
          channelTopic: channel.topic,
          hashColumns: _serverHashColumns,
        ).timeout(const Duration(seconds: 8), onTimeout: () {
          _logger.warning('Merkle root verification timed out on ${channel.topic} — skipping');
          return <MerkleMismatch>[];
        });
      } catch (e) {
        _logger.warning('Merkle root verification failed on ${channel.topic}: $e');
        continue;
      }

      // 2. For each mismatch, repair using the appropriate direction.
      //    Each table repair is independently try-caught so one failure
      //    doesn't skip the rest. However, if the channel enters errored
      //    state (e.g. from a timeout), bail out — all subsequent pushes
      //    will fail immediately.
      for (final mismatch in mismatches) {
        // Check connection health before attempting repair
        if (!_ws.isConnected) {
          _logger.warning('Merkle repair: connection lost, skipping remaining repairs on ${channel.topic}');
          break;
        }

        final tableName = mismatch.table;
        final syncTable = channel.tables
            .where((t) => t.name == tableName)
            .firstOrNull;
        final direction = syncTable != null
            ? channel.directionFor(syncTable)
            : channel.defaultDirection;

        _logger.info('Merkle repair: $tableName on ${channel.topic} direction=${direction.name}');

        try {
          switch (direction) {
            case RepairDirection.pull:
              await _repairTablePull(tableName, blockSize, channelTopic: channel.topic, hashColumns: _serverHashColumns, scopedRowIds: mismatch.rowIds);
              break;
            case RepairDirection.push:
              await _repairTablePush(tableName, blockSize, channelTopic: channel.topic, hashColumns: _serverHashColumns, scopedRowIds: mismatch.rowIds);
              break;
            case RepairDirection.lww:
              await _repairTableLww(tableName, blockSize, channelTopic: channel.topic, hashColumns: _serverHashColumns, scopedRowIds: mismatch.rowIds);
              break;
          }
          allRepairedTables.add(tableName);
        } catch (e) {
          final msg = e.toString();
          if (msg.contains('errored channel') || msg.contains('ChannelTimeout')) {
            _logger.warning('Merkle repair: channel errored on $tableName, skipping remaining repairs on ${channel.topic}');
            break;
          }
          _logger.warning('Merkle repair failed for $tableName on ${channel.topic}: $e — skipping');
        }
      }
    }
  }

  /// Verify data integrity using Merkle trees.
  ///
  /// Compares local Merkle roots against server and repairs any mismatched blocks.
  /// This is a consistency audit that catches:
  /// - Data corruption during development
  /// - Seqnum drift from manual database edits
  /// - Missed changes from network issues
  /// - Any state where seqnums match but data differs
  ///
  /// Returns list of tables that were repaired. Empty list means all tables matched.
  ///
  /// Example:
  /// ```dart
  /// final repairedTables = await syncClient.verifyIntegrity(
  ///   tables: ['users', 'workouts'],
  ///   blockSize: 100,
  /// );
  /// if (repairedTables.isNotEmpty) {
  ///   print('Repaired tables: $repairedTables');
  /// }
  /// ```
  /// Compare local Merkle roots against server and return mismatches.
  /// This is the shared root-verification step used by both the new channels
  /// config path and the legacy verifyIntegrity path.
  Future<List<MerkleMismatch>> _verifyRoots({
    required List<String> tables,
    required int blockSize,
    String? channelTopic,
    List<String>? hashColumns,
  }) async {
    if (!_ws.isConnected) {
      throw StateError('Not connected to server');
    }

    // Compute local Merkle roots
    final tableHashes = <String, MerkleTableInfo>{};
    for (final table in tables) {
      try {
        final merkleInfo = await _merkle!.merkleRoot(table, blockSize: blockSize, hashColumns: hashColumns);
        tableHashes[table] = MerkleTableInfo(
          rootHash: merkleInfo.rootHash,
          blockCount: merkleInfo.blockCount,
          rowCount: merkleInfo.rowCount,
        );
        final hashPreview = merkleInfo.rootHash.length >= 16
            ? '${merkleInfo.rootHash.substring(0, 16)}...'
            : merkleInfo.rootHash.isEmpty ? '(empty)' : merkleInfo.rootHash;
        _logger.fine('verifyRoots: $table - root=$hashPreview, '
            'blocks=${merkleInfo.blockCount}, rows=${merkleInfo.rowCount}');
      } catch (e) {
        _logger.warning('verifyRoots: Failed to compute Merkle root for $table: $e');
      }
    }

    if (tableHashes.isEmpty) return [];

    // Send verification request to server
    final payload = <String, dynamic>{
      'table_hashes': tableHashes.map((table, info) => MapEntry(table, {
        'root_hash': info.rootHash,
        'block_count': info.blockCount,
        'row_count': info.rowCount,
      })),
      'block_size': blockSize,
    };

    final responseMap = await _ws.sendRaw('merkle_verify', payload, channelTopic: channelTopic);
    final response = MerkleVerifyResponse.fromMap(responseMap);

    if (response.isOk) {
      return [];
    }

    // Recheck mismatches with server-provided row_ids scoping.
    // The initial check may mismatch because the client has rows from multiple
    // channels (e.g. users from both user + tribe channels). Re-compute using
    // only the server's scoped row_ids — if they now match, it's a false alarm.
    final realMismatches = <MerkleMismatch>[];
    for (final mismatch in response.mismatches ?? []) {
      if (mismatch.rowIds != null && mismatch.rowIds!.isNotEmpty) {
        try {
          final scopedInfo = await _merkle!.merkleRoot(
            mismatch.table,
            blockSize: blockSize,
            hashColumns: hashColumns,
            scopedRowIds: mismatch.rowIds,
          );
          if (scopedInfo.rootHash == mismatch.serverRootHash) {
            _logger.info('verifyRoots: ${mismatch.table} matches after scoping '
                '(${scopedInfo.rowCount} scoped rows vs ${tableHashes[mismatch.table]?.rowCount} total)');
            continue; // Not a real mismatch — just extra rows from other channels
          }
        } catch (e) {
          _logger.warning('verifyRoots: scoped recheck failed for ${mismatch.table}: $e');
        }
      }
      realMismatches.add(mismatch);
    }

    return realMismatches;
  }

  Future<List<String>> verifyIntegrity({
    List<String>? tables,
    int blockSize = 100,
    String? channelTopic,
  }) async {
    if (!_ws.isConnected) {
      _logger.warning('verifyIntegrity: Not connected');
      throw StateError('Not connected to server');
    }

    final tablesToVerify = tables ?? _allSyncTables;
    if (tablesToVerify.isEmpty) {
      _logger.warning('verifyIntegrity: No tables specified');
      return [];
    }

    _logger.info('verifyIntegrity: Starting integrity check for ${tablesToVerify.length} tables');

    // 1. Compute local Merkle roots for each table
    final tableHashes = <String, MerkleTableInfo>{};
    for (final table in tablesToVerify) {
      try {
        final merkleInfo = await _merkle!.merkleRoot(table, blockSize: blockSize);
        tableHashes[table] = MerkleTableInfo(
          rootHash: merkleInfo.rootHash,
          blockCount: merkleInfo.blockCount,
          rowCount: merkleInfo.rowCount,
        );
        final hashPreview = merkleInfo.rootHash.length >= 16
            ? '${merkleInfo.rootHash.substring(0, 16)}...'
            : merkleInfo.rootHash.isEmpty ? '(empty)' : merkleInfo.rootHash;
        _logger.fine('verifyIntegrity: $table - root=$hashPreview, '
            'blocks=${merkleInfo.blockCount}, rows=${merkleInfo.rowCount}');
      } catch (e) {
        _logger.warning('verifyIntegrity: Failed to compute Merkle root for $table: $e');
        // Skip tables that fail - they may not exist or have no id column
      }
    }

    if (tableHashes.isEmpty) {
      _logger.warning('verifyIntegrity: No valid tables to verify (merkle methods may not be implemented)');
      throw StateError('No valid tables to verify - merkleRoot() may not be implemented on database');
    }

    // 2. Send verification request to server
    // Build payload matching MerkleVerifyMessage structure
    final payload = <String, dynamic>{
      'table_hashes': tableHashes.map((table, info) => MapEntry(table, {
        'root_hash': info.rootHash,
        'block_count': info.blockCount,
        'row_count': info.rowCount,
      })),
      'block_size': blockSize,
    };

    try {
      final responseMap = await _ws.sendRaw('merkle_verify', payload, channelTopic: channelTopic);
      _logger.fine('verifyIntegrity: Response received: $responseMap');
      final response = MerkleVerifyResponse.fromMap(responseMap);

      if (response.isOk) {
        _logger.info('verifyIntegrity: All tables verified OK');
        return [];
      }

      // 3. Handle mismatches - repair each table
      final repairedTables = <String>[];
      for (final mismatch in response.mismatches ?? []) {
        final localHash = tableHashes[mismatch.table]?.rootHash ?? '';
        final serverHash = mismatch.serverRootHash;
        final localPreview = localHash.length >= 16 ? '${localHash.substring(0, 16)}...' : localHash.isEmpty ? '(empty)' : localHash;
        final serverPreview = serverHash.length >= 16 ? '${serverHash.substring(0, 16)}...' : serverHash.isEmpty ? '(empty)' : serverHash;
        _logger.info('verifyIntegrity: Mismatch detected for ${mismatch.table} - '
            'local: $localPreview, server: $serverPreview');

        await _repairTablePull(
          mismatch.table,
          blockSize,
          channelTopic: channelTopic,
        );
        repairedTables.add(mismatch.table);
      }

      _logger.info('verifyIntegrity: Repaired ${repairedTables.length} tables');
      return repairedTables;

    } catch (e, stack) {
      _logger.severe('verifyIntegrity: Error - $e', e, stack);
      rethrow;
    }
  }

  /// Repair a table by fetching differing blocks from server (pull: server → client).
  Future<void> _repairTablePull(
    String table,
    int blockSize, {
    String? channelTopic,
    List<String>? hashColumns,
    List<String>? scopedRowIds,
  }) async {
    // 1. Get block hashes
    final blockHashes = await _merkle!.merkleBlockHashes(table, blockSize: blockSize, hashColumns: hashColumns, scopedRowIds: scopedRowIds);
    _logger.fine('verifyIntegrity: $table has ${blockHashes.length} blocks');

    // 2. Send block hashes to server to find differences
    final payload = <String, dynamic>{
      'table': table,
      'block_hashes': blockHashes,
      'block_size': blockSize,
    };

    try {
      final responseMap = await _ws.sendRaw('merkle_block_hashes', payload, channelTopic: channelTopic);
      final response = MerkleBlockHashesResponse.fromMap(responseMap);

      if (response.differingBlocks.isEmpty && scopedRowIds == null) {
        _logger.info('verifyIntegrity: $table - no differing blocks (hash collision resolved)');
        return;
      }

      if (response.differingBlocks.isNotEmpty) {
        _logger.info('verifyIntegrity: $table - ${response.differingBlocks.length} differing blocks: '
            '${response.differingBlocks}');

        // 3. Fetch and apply each differing block
        for (final blockIndex in response.differingBlocks) {
          await _fetchAndApplyBlock(table, blockIndex, blockSize, channelTopic: channelTopic);
        }
      }

      // 4. Delete local rows that are outside server's scope.
      //    This handles the case where server has fewer rows than client
      //    (e.g. rolling time window filters out old posts).
      if (scopedRowIds != null) {
        final allLocalIds = await _merkle!.getAllRowIds(table);
        final scopedSet = scopedRowIds.toSet();
        final outOfScope = allLocalIds.where((id) => !scopedSet.contains(id)).toList();
        if (outOfScope.isNotEmpty) {
          _logger.info('verifyIntegrity: $table - deleting ${outOfScope.length} out-of-scope rows');
          _db!.beginBulkRemote();
          try {
            for (final rowId in outOfScope) {
              final change = ChangeMessage(
                table: table,
                operation: 'delete',
                rowId: rowId,
              );
              final sql = _generateSql(change);
              _db!.execBulkRemote(sql);
            }
            await _db!.endBulkRemote();
          } catch (e) {
            await _db!.endBulkRemote(rollback: true);
            rethrow;
          }
        }
      }

      _logger.info('verifyIntegrity: $table - repair complete');

    } catch (e) {
      _logger.severe('verifyIntegrity: Failed to repair $table: $e');
      rethrow;
    }
  }

  /// Fetch a block from server and apply to local database
  Future<void> _fetchAndApplyBlock(
    String table,
    int blockIndex,
    int blockSize, {
    String? channelTopic,
  }) async {
    final payload = <String, dynamic>{
      'table': table,
      'blocks': [blockIndex],
      'block_size': blockSize,
    };

    try {
      final responseMap = await _ws.sendRaw('merkle_fetch_blocks', payload, channelTopic: channelTopic);
      final response = MerkleFetchBlocksResponse.fromMap(responseMap);

      // Get existing row IDs in this block to detect deletions
      final existingRowIds = await _merkle!.getBlockRowIds(table, blockIndex, blockSize: blockSize);
      final serverRowIds = response.rows.map((r) => r['id']?.toString() ?? '').toSet();

      // Use bulk operations for efficiency (no individual logging)
      _db!.beginBulkRemote();
      var updatedCount = 0;
      var insertedCount = 0;
      var deletedCount = 0;

      try {
        // Apply each row from server (inserts or updates)
        for (final row in response.rows) {
          final rowId = row['id']?.toString();
          if (rowId == null) continue;

          final isUpdate = existingRowIds.contains(rowId);
          final change = ChangeMessage(
            table: table,
            operation: isUpdate ? 'update' : 'insert',
            rowId: rowId,
            data: row,
          );
          final sql = _generateSql(change);
          _db!.execBulkRemote(sql);
          if (isUpdate) {
            updatedCount++;
          } else {
            insertedCount++;
          }
        }

        // Delete rows that exist locally but not on server
        for (final localRowId in existingRowIds) {
          if (!serverRowIds.contains(localRowId)) {
            final change = ChangeMessage(
              table: table,
              operation: 'delete',
              rowId: localRowId,
            );
            final sql = _generateSql(change);
            _db!.execBulkRemote(sql);
            deletedCount++;
          }
        }

        await _db!.endBulkRemote();

        // row_hash is now server-authoritative — included in row data from server via _generateSql

        _logger.info('verifyIntegrity: $table block $blockIndex - applied ${response.rows.length} rows '
            '(updated: $updatedCount, inserted: $insertedCount, deleted: $deletedCount)');
      } catch (e) {
        await _db!.endBulkRemote(rollback: true);
        rethrow;
      }

    } catch (e) {
      _logger.severe('verifyIntegrity: Failed to fetch block $blockIndex for $table: $e');
      rethrow;
    }
  }

  /// Repair a table by pushing local rows to server (push: client → server).
  ///
  /// Used for user-owned tables where the client is authoritative.
  /// Server applies each row through its authorization checks.
  Future<void> _repairTablePush(
    String table,
    int blockSize, {
    String? channelTopic,
    List<String>? hashColumns,
    List<String>? scopedRowIds,
  }) async {
    // 1. Get block hashes and find differing blocks (same as pull)
    // When scopedRowIds is empty (server has 0 rows), compute over ALL local
    // rows so the server can identify which blocks the client needs to push.
    final effectiveScope = (scopedRowIds != null && scopedRowIds.isEmpty) ? null : scopedRowIds;
    final blockHashes = await _merkle!.merkleBlockHashes(table, blockSize: blockSize, hashColumns: hashColumns, scopedRowIds: effectiveScope);
    _logger.fine('repairPush: $table has ${blockHashes.length} blocks');

    final payload = <String, dynamic>{
      'table': table,
      'block_hashes': blockHashes,
      'block_size': blockSize,
    };

    try {
      final responseMap = await _ws.sendRaw('merkle_block_hashes', payload, channelTopic: channelTopic);
      final response = MerkleBlockHashesResponse.fromMap(responseMap);

      if (response.differingBlocks.isEmpty) {
        _logger.info('repairPush: $table - no differing blocks');
        return;
      }

      _logger.info('repairPush: $table - ${response.differingBlocks.length} differing blocks: '
          '${response.differingBlocks}');

      // 2. For each differing block, read local rows and push to server
      for (final blockIndex in response.differingBlocks) {
        await _pushBlockToServer(table, blockIndex, blockSize, channelTopic: channelTopic, scopedRowIds: scopedRowIds);
      }

      _logger.info('repairPush: $table - push repair complete');

    } catch (e) {
      _logger.severe('repairPush: Failed to repair $table: $e');
      rethrow;
    }
  }

  /// Read local rows for a block and push them to the server.
  Future<void> _pushBlockToServer(
    String table,
    int blockIndex,
    int blockSize, {
    String? channelTopic,
    List<String>? scopedRowIds,
  }) async {
    // Read local row IDs for this block
    final rowIds = await _merkle!.getBlockRowIds(table, blockIndex, blockSize: blockSize, scopedRowIds: scopedRowIds);

    // Build SELECT with json() for JSONB columns to avoid binary warnings
    final selectSql = await _buildRepairSelectSql(table);

    // Read full row data for each ID, skipping stripped rows (document IS NULL)
    final rows = <Map<String, dynamic>>[];
    for (final rowId in rowIds) {
      final rowData = await _db!.read(
        "$selectSql WHERE id = '${_escapeSql(rowId)}'"
      );
      if (rowData.isNotEmpty) {
        final row = rowData.first;
        // Skip stripped rows — client doesn't have the real content
        if (row.containsKey('document') && row['document'] == null) {
          _logger.fine('repairPush: skipping stripped row $table:$rowId');
          continue;
        }
        rows.add(row);
      }
    }

    // Push to server
    final payload = <String, dynamic>{
      'table': table,
      'block_index': blockIndex,
      'block_size': blockSize,
      'rows': rows,
      'client_row_ids': rowIds,
    };

    try {
      final responseMap = await _ws.sendRaw('merkle_push_blocks', payload, channelTopic: channelTopic);
      final response = MerklePushBlocksResponse.fromMap(responseMap);

      _logger.info('repairPush: $table block $blockIndex - '
          'applied: ${response.applied}, rejected: ${response.rejected}, deleted: ${response.deleted}');

      if (response.errors.isNotEmpty) {
        for (final error in response.errors) {
          _logger.warning('repairPush: $table block $blockIndex - error: $error');
        }
      }
    } catch (e) {
      _logger.severe('repairPush: Failed to push block $blockIndex for $table: $e');
      rethrow;
    }
  }

  /// Repair a table using last-write-wins (LWW) resolution.
  ///
  /// Sends local rows to server which compares last_modified_ms timestamps.
  /// Server returns rows where it wins (client should overwrite local data),
  /// and accepts rows where client wins.
  Future<void> _repairTableLww(
    String table,
    int blockSize, {
    String? channelTopic,
    List<String>? hashColumns,
    List<String>? scopedRowIds,
  }) async {
    // 1. Get block hashes and find differing blocks (same as pull/push)
    // When scopedRowIds is empty (server has 0 rows), compute over ALL local
    // rows so the server can identify which blocks differ.
    final effectiveScope = (scopedRowIds != null && scopedRowIds.isEmpty) ? null : scopedRowIds;
    final blockHashes = await _merkle!.merkleBlockHashes(table, blockSize: blockSize, hashColumns: hashColumns, scopedRowIds: effectiveScope);
    _logger.fine('repairLww: $table has ${blockHashes.length} blocks');

    final payload = <String, dynamic>{
      'table': table,
      'block_hashes': blockHashes,
      'block_size': blockSize,
    };

    try {
      final responseMap = await _ws.sendRaw('merkle_block_hashes', payload, channelTopic: channelTopic);
      final response = MerkleBlockHashesResponse.fromMap(responseMap);

      if (response.differingBlocks.isEmpty) {
        _logger.info('repairLww: $table - no differing blocks');
        return;
      }

      _logger.info('repairLww: $table - ${response.differingBlocks.length} differing blocks: '
          '${response.differingBlocks}');

      // 2. For each differing block, resolve via LWW
      for (final blockIndex in response.differingBlocks) {
        await _lwwResolveBlock(table, blockIndex, blockSize, channelTopic: channelTopic, scopedRowIds: scopedRowIds);
      }

      _logger.info('repairLww: $table - lww repair complete');

    } catch (e) {
      _logger.severe('repairLww: Failed to repair $table: $e');
      rethrow;
    }
  }

  /// Resolve a single block using last-write-wins.
  Future<void> _lwwResolveBlock(
    String table,
    int blockIndex,
    int blockSize, {
    String? channelTopic,
    List<String>? scopedRowIds,
  }) async {
    // Read local rows for this block
    final rowIds = await _merkle!.getBlockRowIds(table, blockIndex, blockSize: blockSize, scopedRowIds: scopedRowIds);
    final selectSql = await _buildRepairSelectSql(table);
    final localRows = <Map<String, dynamic>>[];
    for (final rowId in rowIds) {
      final rowData = await _db!.read(
        "$selectSql WHERE id = '${_escapeSql(rowId)}'"
      );
      if (rowData.isNotEmpty) {
        final row = rowData.first;
        // Skip stripped rows — client doesn't have the real content
        if (row.containsKey('document') && row['document'] == null) {
          _logger.fine('repairLww: skipping stripped row $table:$rowId');
          continue;
        }
        localRows.add(row);
      }
    }

    // Send to server for LWW resolution
    final payload = <String, dynamic>{
      'table': table,
      'block_index': blockIndex,
      'block_size': blockSize,
      'rows': localRows,
      'client_row_ids': rowIds,
    };

    try {
      final responseMap = await _ws.sendRaw('merkle_lww_blocks', payload, channelTopic: channelTopic);
      final response = MerkleLwwBlocksResponse.fromMap(responseMap);

      // Apply server-wins rows locally
      if (response.serverWins.isNotEmpty) {
        _db!.beginBulkRemote();
        try {
          for (final row in response.serverWins) {
            final rowId = row['id']?.toString();
            if (rowId == null) continue;

            final existsLocally = rowIds.contains(rowId);
            final change = ChangeMessage(
              table: table,
              operation: existsLocally ? 'update' : 'insert',
              rowId: rowId,
              data: row,
            );
            final sql = _generateSql(change);
            _db!.execBulkRemote(sql);
          }
          await _db!.endBulkRemote();
          // row_hash is now server-authoritative — included in row data from server via _generateSql
        } catch (e) {
          await _db!.endBulkRemote(rollback: true);
          rethrow;
        }
      }

      _logger.info('repairLww: $table block $blockIndex - '
          'client_wins: ${response.appliedFromClient}, '
          'server_wins: ${response.serverWins.length}');

    } catch (e) {
      _logger.severe('repairLww: Failed to resolve block $blockIndex for $table: $e');
      rethrow;
    }
  }

  // ==========================================================================
  // END MERKLE TREE INTEGRITY VERIFICATION
  // ==========================================================================

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
