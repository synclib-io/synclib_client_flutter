import 'dart:async';
import 'package:logging/logging.dart';
import 'package:synclib_flutter/synclib_flutter.dart';
import 'connection/websocket_manager.dart';
import 'protocol/message.dart';
import 'protocol/codec.dart';

/// Callback for conflict resolution
/// Returns the resolved change, or null to skip
typedef ConflictResolver = Future<ChangeMessage?> Function(
  ChangeMessage local,
  ChangeMessage remote,
);

/// Configuration for sync client
class SyncClientConfig {
  /// Path to local SQLite database
  final String dbPath;

  /// WebSocket server URL
  final String serverUrl;

  /// Unique client identifier
  final String clientId;

  /// User ID for user-specific channel subscription
  final String userId;

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

  const SyncClientConfig({
    required this.dbPath,
    required this.serverUrl,
    required this.clientId,
    required this.userId,
    this.codec = SyncCodecType.messagepack,
    this.pushInterval = const Duration(seconds: 5),
    this.pushBatchSize = 100,
    this.pullInterval,
    this.onConflict,
    this.metadata,
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
  int? _lastSyncedSeqnum;
  final Set<int> _pendingAcks = {};

  // Store channel subscription params for reconnection
  String? _token;
  bool _joinWorld = false;
  List<String>? _zones;
  List<String>? _guilds;
  List<String>? _parties;

  // Stream controller for remote change notifications
  final StreamController<ChangeMessage> _remoteChangeController = StreamController<ChangeMessage>.broadcast();

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
  /// - world: Global announcements
  /// - zones: List of zone IDs to subscribe to
  /// - guilds: List of guild IDs to subscribe to
  /// - parties: List of party IDs to subscribe to
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
  Future<void> _joinChannels() async {
    // Join user-specific channel (always required)
    final userTopic = 'sync:user:${config.userId}';
    _logger.info('Joining channel: $userTopic');
    await _ws.joinChannel(userTopic, {'client_id': config.clientId});

    // Join world channel if requested
    if (_joinWorld) {
      _logger.info('Joining channel: sync:world');
      await _ws.joinChannel('sync:world', {'client_id': config.clientId});
    }

    // Join zone channels
    if (_zones != null) {
      for (final zone in _zones!) {
        final topic = 'sync:zone:$zone';
        _logger.info('Joining channel: $topic');
        await _ws.joinChannel(topic, {'client_id': config.clientId});
      }
    }

    // Join guild channels
    if (_guilds != null) {
      for (final guild in _guilds!) {
        final topic = 'sync:guild:$guild';
        _logger.info('Joining channel: $topic');
        await _ws.joinChannel(topic, {'client_id': config.clientId});
      }
    }

    // Join party channels
    if (_parties != null) {
      for (final party in _parties!) {
        final topic = 'sync:party:$party';
        _logger.info('Joining channel: $topic');
        await _ws.joinChannel(topic, {'client_id': config.clientId});
      }
    }

    _logger.info('All channels joined successfully');
    _hasConnectedOnce = true;
    _startPeriodicSync();
    await _sendHello();
  }

  /// Disconnect from sync server
  Future<void> disconnect() async {
    _stopPeriodicSync();
    await _ws.disconnect();
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

      await _ws.send(batch);

      // Track pending acks
      for (final change in changes) {
        _pendingAcks.add(change.seqnum);
      }
    } catch (e, stack) {
      _logger.severe('Failed to push changes: $e', e, stack);
    }
  }

  /// Request remote changes from server
  Future<void> _pullRemoteChanges() async {
    if (!_ws.isConnected) return;

    try {
      final request = RequestChangesMessage(
        sinceSeqnum: _lastSyncedSeqnum,
      );
      await _ws.send(request);
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
    await _ws.send(hello);
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

      // Generate SQL for the operation
      final sql = _generateSql(change);

      // Apply to local database
      await _db!.applyRemote(
        tableName: change.table,
        rowId: change.rowId,
        operation: change.toSynclibOperation(),
        sql: sql,
        data: change.data?.toString(),
      );

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
        _lastSyncedSeqnum = changes.last.seqnum;
      }
    } catch (e) {
      _logger.severe('Failed to apply changes batch: $e');
      await _db!.endBulkRemote(rollback: true);
    }
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

  /// Handle Phoenix reply messages (especially hello response with migrations)
  Future<void> _handlePhoenixReply(PhoenixReplyMessage reply) async {
    final response = reply.response;

    // Check if this is a hello response with migrations
    if (response['status'] == 'upgrade_needed') {
      _logger.info('Schema upgrade needed');
      await _applyMigrations(response);
    } else if (response['status'] == 'up_to_date') {
      _logger.info('Schema is up to date');
    } else if (response['status'] == 'ok') {
      _logger.fine('Received OK response');
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
          _logger.fine('Executing: $sql');
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
    await _ws.send(confirm);
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
  void _startPeriodicSync() {
    if (config.pushInterval != null) {
      _pushTimer?.cancel();
      _pushTimer = Timer.periodic(config.pushInterval!, (_) => _pushLocalChanges());
    }

    if (config.pullInterval != null) {
      _pullTimer?.cancel();
      _pullTimer = Timer.periodic(config.pullInterval!, (_) => _pullRemoteChanges());
    }
  }

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

        // Skip server-only timestamp fields
        if (key == 'inserted_at') continue;

        // TODO, is this necessary? they should be the same.
        // do we need use case specific code here?
        // Map server field names to client field names
        String clientKey = key;
        if (change.table == 'users' && key == 'username') {
          clientKey = 'name';
        }

        filteredData[clientKey] = value;
      }
    }

    switch (change.operation) {
      case 'insert':
        final columns = filteredData.keys.join(', ');
        final values = filteredData.values
          .map((v) => v is String ? "'${_escapeSql(v)}'" : v.toString())
          .join(', ');
        return 'INSERT OR REPLACE INTO ${change.table} ($columns) VALUES ($values)';

      case 'update':
        final sets = filteredData.entries
          .map((e) => '${e.key} = ${e.value is String ? "'${_escapeSql(e.value)}'" : e.value}')
          .join(', ');
        return 'UPDATE ${change.table} SET $sets WHERE id = \'${_escapeSql(change.rowId)}\'';

      case 'delete':
        return 'DELETE FROM ${change.table} WHERE id = \'${_escapeSql(change.rowId)}\'';

      default:
        throw ArgumentError('Unknown operation: ${change.operation}');
    }
  }

  /// Escape single quotes in SQL strings
  String _escapeSql(String value) {
    return value.replaceAll("'", "''");
  }

  /// Get current connection state
  ConnectionState get connectionState => _ws.state;

  /// Stream of connection state changes
  Stream<ConnectionState> get stateChanges => _ws.stateChanges;

  /// Stream of remote changes as they are applied
  Stream<ChangeMessage> get remoteChanges => _remoteChangeController.stream;

  /// Dispose resources
  Future<void> dispose() async {
    _stopPeriodicSync();
    await _messageSubscription?.cancel();
    await _stateSubscription?.cancel();
    await _remoteChangeController.close();
    await _ws.dispose();
    await _db?.close();
    _isInitialized = false;
  }
}
