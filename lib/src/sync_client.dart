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
  int? _lastSyncedSeqnum;
  final Set<int> _pendingAcks = {};

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
  Future<void> connect() async {
    if (!_isInitialized) {
      throw StateError('Not initialized. Call initialize() first.');
    }

    await _ws.connect();

    // WebSocketManager.connect() ensures socket is connected
    // Now join the channel
    _logger.info('Attempting to join channel sync:lobby');
    await _ws.joinChannel('sync:lobby', {
      'client_id': config.clientId,
    });
    _logger.info('Channel joined successfully');

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
    final hello = HelloMessage(
      clientId: config.clientId,
      lastSeqnum: _lastSyncedSeqnum,
      metadata: config.metadata,
    );
    await _ws.send(hello);
    _logger.info('Sent hello message');
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

      // Send acknowledgment
      if (change.seqnum != null) {
        final ack = AckMessage(seqnum: change.seqnum!, success: true);
        await _ws.send(ack);
      }
    } catch (e, stack) {
      _logger.severe('Failed to apply remote change: $e', e, stack);
      if (change.seqnum != null) {
        final ack = AckMessage(
          seqnum: change.seqnum!,
          success: false,
          error: e.toString(),
        );
        await _ws.send(ack);
      }
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

  /// Handle connection state changes
  void _handleStateChange(ConnectionState state) {
    _logger.info('Connection state: $state');

    switch (state) {
      case ConnectionState.connected:
        // Note: _sendHello() and _startPeriodicSync() are called in connect() after joining channel
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
