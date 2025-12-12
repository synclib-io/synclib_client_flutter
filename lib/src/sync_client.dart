import 'dart:async';
import 'dart:convert';
import 'package:logging/logging.dart';
import 'package:synclib_flutter/synclib_flutter.dart';
import 'package:synchronized/synchronized.dart';
import 'connection/websocket_manager.dart';
import 'protocol/message.dart';
import 'protocol/codec.dart';

/// Sync readiness state
enum SyncReadyState {
  /// Waiting for initial hello reply from server
  waitingForHello,
  /// Applying schema migrations
  applyingMigrations,
  /// Ready to stream snapshots and sync data
  ready,
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

  /// Channel to use for broadcasting messages (e.g., "sync:user:123")
  /// If not specified, uses the first channel
  final String? broadcastChannel;

  /// Whether to pull remote changes periodically.
  /// If false, only pushes local changes. Defaults to true.
  final bool pullRemote;

  /// Whether to enable periodic sync at all.
  /// If false, no timers are started - sync is fully reactive (WebSocket events only).
  /// Defaults to true.
  final bool enablePeriodicSync;

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
    this.pullRemote = true,
    this.enablePeriodicSync = true,
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

  // Store channel subscription params for reconnection
  String? _token;
  bool _joinWorld = false;
  List<String>? _zones;
  List<String>? _guilds;
  List<String>? _parties;

  // Control whether periodic sync pulls from remote (initialized from config)
  late bool _pullRemoteEnabled;

  // Stream controller for remote change notifications
  final StreamController<ChangeMessage> _remoteChangeController = StreamController<ChangeMessage>.broadcast();

  // Stream controller for snapshot complete events
  final StreamController<SnapshotCompleteEvent> _snapshotCompleteController = StreamController<SnapshotCompleteEvent>.broadcast();

  // Stream controller for job update events
  final StreamController<JobUpdateMessage> _jobUpdateController = StreamController<JobUpdateMessage>.broadcast();

  // Stream controller for livestream events
  final StreamController<LivestreamMessage> _livestreamController = StreamController<LivestreamMessage>.broadcast();

  // Stream controller for conversation events
  final StreamController<ConversationMessage> _conversationController = StreamController<ConversationMessage>.broadcast();

  // Stream controller for sync ready state changes
  final StreamController<SyncReadyState> _syncReadyStateController = StreamController<SyncReadyState>.broadcast();

  // Lock to ensure only one batch operation happens at a time
  final Lock _batchLock = Lock();
  final List<SnapshotBatchMessage> _batchQueue = [];

  SyncReadyState _readyState = SyncReadyState.waitingForHello;

  SyncClient(this.config) {
    _pullRemoteEnabled = config.pullRemote;
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

    // Open database
    _db = await SynclibDatabase.open(config.dbPath);
    _logger.info('Database opened: ${config.dbPath}');

    // Subscribe to WebSocket messages
    _messageSubscription = _ws.messages.listen(_handleMessage);
    _stateSubscription = _ws.stateChanges.listen(_handleStateChange);

    _isInitialized = true;
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
    // Join user-specific channel (always required)

    for (final channel in config.initialChannels) {
      final topic = 'sync:${channel.channelName}:${channel.channelId}';
      _logger.info('Joining channel: $topic');
      await _ws.joinChannel(topic, {'client_id': config.clientId, ...?channel.params});
    }

    _logger.info('All channels joined successfully');
    _hasConnectedOnce = true;
    if (config.enablePeriodicSync) {
      startPeriodicSync(pullRemote: _pullRemoteEnabled);
    }
    await _sendHello();
  }

  /// Join an additional channel after initial connection
  ///
  /// Example:
  /// ```dart
  /// await syncClient.joinChannel(
  ///   SyncClientChannel(
  ///     channelName: 'user',
  ///     channelId: userId,
  ///   ),
  /// );
  /// ```
  Future<void> joinChannel(SyncClientChannel channel) async {
    if (!_ws.isConnected) {
      throw StateError('Not connected. Call connect() first.');
    }

    final topic = 'sync:${channel.channelName}:${channel.channelId}';
    _logger.info('Joining additional channel: $topic');

    await _ws.joinChannel(topic, {
      'client_id': config.clientId,
      ...?channel.params,
    });

    _logger.info('Successfully joined channel: $topic');
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

  /// Disconnect from sync server
  Future<void> disconnect() async {
    _stopPeriodicSync();
    await _ws.disconnect();
  }

  Future<void> syncOverTable(String table) async {
    await _pushLocalChanges();
    await _pullRemoteChangesForTable(table);
  }

  /// Manually trigger a sync cycle
  Future<void> sync() async {
    await _pushLocalChanges();
    await _pullRemoteChanges();
  }

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

      // Track pending acks
      for (final change in changes) {
        _pendingAcks.add(change.seqnum);
      }
    } catch (e, stack) {
      _logger.severe('Failed to push changes: $e', e, stack);
    }
  }

  /// Request remote changes from server
  Future<void> _pullRemoteChangesForTable(String table) async {
    if (!_ws.isConnected) return;

    if (_lastSyncedSeqnum == 0) {
      _lastSyncedSeqnum = await _getMaxSeqnumFromTable(table) ?? 0; // seqnum is global across all tables. we have anything under the max
    }

    try {
      final request = RequestChangesMessage(
        sinceSeqnum: _lastSyncedSeqnum,
        table: table
      );
      await _ws.send(request, channelTopic: config.broadcastChannel);
      _logger.fine('Requested remote changes since $_lastSyncedSeqnum');
    } catch (e) {
      _logger.severe('Failed to request changes: $e');
    }
  }

  /// Request remote changes from server
  Future<void> _pullRemoteChanges() async {
    if (!_ws.isConnected) return;

    try {
      final request = RequestChangesMessage(
        sinceSeqnum: _lastSyncedSeqnum ?? 0,
      );
      await _ws.send(request, channelTopic: config.broadcastChannel);
      _logger.fine('Requested remote changes since $_lastSyncedSeqnum');
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
      } else if (message is SchemaUpdateMessage) {
        await _handleSchemaUpdate(message);
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
      try {
        for (final change in changes) {
          final sql = _generateSql(change);
          await _db!.execBulkRemote(sql);
        }
        await _db!.endBulkRemote();
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
    _logger.fine('Received ack for seqnum ${ack.seqnum}: ${ack.success}');

    if (ack.success) {
      _pendingAcks.remove(ack.seqnum);
      // Mark as synced in local database
      _db!.markSynced(ack.seqnum).catchError((e) {
        _logger.severe('Failed to mark synced: $e');
      });
    } else {
      _logger.warning('Change ${ack.seqnum} failed: ${ack.error}');
      // TODO: Implement retry logic
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
        _logger.info('!!! SKIPPING batch for ${batch.table}: table does not exist');
        return;
      }
    } catch (e) {
      _logger.severe('Error checking if table ${batch.table} exists: $e');
      return;
    }

    // Apply each row to the database
    await _db!.beginBulkRemote();
    int processedCount = 0;
    try {
      for (final row in batch.rows) {
        // Create a change message for each row
        final change = ChangeMessage(
          table: batch.table,
          operation: 'insert',
          rowId: row['id'] as String,
          data: row,
        );

        final sql = _generateSql(change);
        await _db!.execBulkRemote(sql);
        processedCount++;
      }
      await _db!.endBulkRemote();
      final elapsed = DateTime.now().difference(startTime).inMilliseconds;
      _logger.info('Successfully applied snapshot batch for ${batch.table}: processed $processedCount/${batch.rows.length} rows in ${elapsed}ms');
    } catch (e, stackTrace) {
      final elapsed = DateTime.now().difference(startTime).inMilliseconds;
      _logger.info('!!! FAILED to apply snapshot batch for ${batch.table} after processing $processedCount/${batch.rows.length} rows (${elapsed}ms): $e');
      _logger.info('Stack trace: $stackTrace');
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
        // When reconnected (not initial connection), rejoin channels
        if (_hasConnectedOnce) {
          _logger.info('Reconnected - rejoining channels');
          _joinChannels().catchError((e) {
            _logger.severe('Failed to rejoin channels after reconnect: $e');
          });
        }
        break;
      case ConnectionState.disconnected:
      case ConnectionState.failed:
        _stopPeriodicSync();
        break;
      default:
        break;
    }
  }

  /// Start periodic sync timers
  ///
  /// [pullRemote] - If false, only pushes local changes periodically (no pull).
  /// Defaults to true.
  void startPeriodicSync({bool pullRemote = true}) {
    _pullRemoteEnabled = pullRemote;

    if (config.pushInterval != null) {
      _pushTimer?.cancel();
      _pushTimer = Timer.periodic(config.pushInterval!, (_) => _pushLocalChanges());
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

        // Handle special case mappings
        if (change.table == 'users' && normalizedKey == 'username') {
          normalizedKey = 'name';
        }

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
        final placeholders = ['?', ...List.generate(filteredData.length, (_) => '?')].join(', ');

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
  ///
  /// Example:
  /// ```dart
  /// // Full sync (all data)
  /// await syncClient.streamSnapshot(['users', 'journal_entries']);
  ///
  /// // Incremental sync (only changes since last sync)
  /// await syncClient.streamSnapshot(['users', 'journal_entries'], incremental: true);
  /// ```
  Future<void> streamSnapshot(
    List<String> tables, {
    bool incremental = false,
        String? channelTopic
  }) async {
    if (!_ws.isConnected) {
      throw Exception('Not connected to server');
    }

    int? sinceSeqnum;

    if (incremental) {
      sinceSeqnum = await _getMaxServerSeqnum(tables); // seqnum is global across all tables. we have anything under the max
      _logger.info('Requesting incremental snapshot since seqnum: $sinceSeqnum');
    } else {
      _logger.info('Requesting full snapshot');
    }

    final payload = {
      'tables': tables,
      if (sinceSeqnum != null) 'since_seqnum': sinceSeqnum,
    };

    await _ws.sendRaw('stream_snapshot', payload, channelTopic: channelTopic);
  }

  /// Get the max seqnum across multiple tables
  /// Returns null if any table has no data
  Future<int?> _getMaxServerSeqnum(List<String> tables) async {
    int? maxSeqnum;

    for (final table in tables) {
      final seqnum = await _getMaxSeqnumFromTable(table);
      if (maxSeqnum == null || seqnum > maxSeqnum) {
        maxSeqnum = seqnum;
      }
    }

    return maxSeqnum;
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
    {String? channelTopic}
  ) async {
    if (!_ws.isConnected) {
      throw Exception('Not connected to server');
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
  }) async {
    if (!_ws.isConnected) {
      throw Exception('Not connected to server');
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

  /// Stream of job update events (from ECS tasks via webhook)
  Stream<JobUpdateMessage> get jobUpdates => _jobUpdateController.stream;

  /// Stream of livestream events (started/stopped notifications)
  Stream<LivestreamMessage> get livestreamEvents => _livestreamController.stream;

  /// Stream of conversation events (user presence, message notifications, online count)
  Stream<ConversationMessage> get conversationEvents => _conversationController.stream;

  /// Stream of sync ready state changes
  /// Listen to this to know when the client is ready to stream snapshots
  /// States: waitingForHello -> applyingMigrations -> ready
  Stream<SyncReadyState> get syncReadyState => _syncReadyStateController.stream;

  /// Get current sync ready state
  SyncReadyState get readyState => _readyState;

  /// Check if client is ready to stream snapshots
  /// Returns true only when state is SyncReadyState.ready
  bool get isReady => _readyState == SyncReadyState.ready;

  /// Dispose resources
  Future<void> dispose() async {
    _stopPeriodicSync();
    await _messageSubscription?.cancel();
    await _stateSubscription?.cancel();
    await _remoteChangeController.close();
    await _snapshotCompleteController.close();
    await _jobUpdateController.close();
    await _livestreamController.close();
    await _conversationController.close();
    await _syncReadyStateController.close();
    await _ws.dispose();
    await _db?.close();
    _isInitialized = false;
  }
}
