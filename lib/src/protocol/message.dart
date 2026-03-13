import 'dart:convert';
import 'package:synclib_flutter/synclib_flutter.dart';

/// Base class for all sync protocol messages
abstract class SyncMessage {
  const SyncMessage();

  /// Convert message to map for encoding
  Map<String, dynamic> toMap();

  /// Create message from decoded map
  factory SyncMessage.fromMap(Map<String, dynamic> map) {
    final type = map['type'] as String?;

    switch (type) {
      case 'change':
        return ChangeMessage.fromMap(map);
      case 'ack':
        return AckMessage.fromMap(map);
      case 'request_changes':
        return RequestChangesMessage.fromMap(map);
      case 'changes_batch':
        return ChangesBatchMessage.fromMap(map);
      case 'hello':
        return HelloMessage.fromMap(map);
      case 'phx_reply':
        // Handle Phoenix reply messages (for hello response)
        return PhoenixReplyMessage.fromMap(map);
      case 'schema_migrated':
        return SchemaConfirmMessage.fromMap(map);
      case 'error':
        return ErrorMessage.fromMap(map);
      case 'snapshot_batch':
        return SnapshotBatchMessage.fromMap(map);
      case 'snapshot_complete':
        return SnapshotCompleteMessage.fromMap(map);
      case 'schema_update':
        return SchemaUpdateMessage.fromMap(map);
      // New simplified sync handshake messages
      case 'sync_request':
        return SyncRequestMessage.fromMap(map);
      case 'sync_schema': // Server event name
      case 'schema_migrations':
        return SchemaMigrationsMessage.fromMap(map);
      case 'sync_acks': // Server event name
      case 'change_acks':
        return ChangeAcksMessage.fromMap(map);
      case 'sync_batch': // Server event name
      case 'sync_data_batch':
        return SyncDataBatchMessage.fromMap(map);
      case 'sync_complete':
        return SyncCompleteMessage.fromMap(map);
      // Merkle tree integrity verification messages
      case 'merkle_verify':
        return MerkleVerifyMessage.fromMap(map);
      case 'merkle_verify_response':
        return MerkleVerifyResponse.fromMap(map);
      case 'merkle_block_hashes':
        return MerkleBlockHashesMessage.fromMap(map);
      case 'merkle_block_hashes_response':
        return MerkleBlockHashesResponse.fromMap(map);
      case 'merkle_fetch_blocks':
        return MerkleFetchBlocksMessage.fromMap(map);
      case 'merkle_fetch_blocks_response':
        return MerkleFetchBlocksResponse.fromMap(map);
      default:
        // Unknown message types are logged but not thrown - allows forward compatibility
        // and handles internal server messages that shouldn't be sent to clients
        return IgnoredMessage(type: type ?? 'unknown', payload: map);
    }
  }
}

/// Message type for unknown/ignored messages
/// Used for forward compatibility and internal server messages
class IgnoredMessage extends SyncMessage {
  final String type;
  final Map<String, dynamic> payload;

  const IgnoredMessage({required this.type, this.payload = const {}});

  @override
  Map<String, dynamic> toMap() => {'type': type, ...payload};
}

/// Initial handshake message from client to server
class HelloMessage extends SyncMessage {
  final String clientId;
  final int? lastSeqnum;
  final int schemaVersion;
  final Map<String, dynamic>? metadata;

  const HelloMessage({
    required this.clientId,
    this.lastSeqnum,
    this.schemaVersion = 0,
    this.metadata,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'hello',
    'client_id': clientId,
    'last_seqnum': lastSeqnum,
    'schema_version': schemaVersion,
    if (metadata != null) 'metadata': metadata,
  };

  factory HelloMessage.fromMap(Map<String, dynamic> map) => HelloMessage(
    clientId: map['client_id'] as String,
    lastSeqnum: map['last_seqnum'] as int?,
    schemaVersion: map['schema_version'] as int? ?? 0,
    metadata: map['metadata'] as Map<String, dynamic>?,
  );
}

/// Represents a single database change to sync
class ChangeMessage extends SyncMessage {
  final String table;
  final String operation; // 'insert', 'update', 'delete'
  final String rowId;
  final Map<String, dynamic>? data;
  final int? seqnum;
  final DateTime? timestamp;

  const ChangeMessage({
    required this.table,
    required this.operation,
    required this.rowId,
    this.data,
    this.seqnum,
    this.timestamp,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'change',
    'table': table,
    'operation': operation,
    'row_id': rowId,
    if (data != null) 'data': data,
    if (seqnum != null) 'seqnum': seqnum,
    if (timestamp != null) 'timestamp': timestamp!.toIso8601String(),
  };

  factory ChangeMessage.fromMap(Map<String, dynamic> map) {
    DateTime? timestamp;
    if (map['timestamp'] != null) {
      final ts = map['timestamp'];
      if (ts is String) {
        timestamp = DateTime.parse(ts);
      } else if (ts is num) {
        // Unix timestamp in seconds (possibly with fractional seconds)
        timestamp = DateTime.fromMillisecondsSinceEpoch((ts * 1000).toInt());
      }
    }

    return ChangeMessage(
      table: map['table'] as String,
      operation: map['operation'] as String,
      rowId: map['row_id'] as String,
      data: map['data'] as Map<String, dynamic>?,
      seqnum: map['seqnum'] as int?,
      timestamp: timestamp,
    );
  }

  /// Create from synclib Change object
  factory ChangeMessage.fromChange(Change change) => ChangeMessage(
    table: change.tableName,
    operation: change.operation.name,
    rowId: change.rowId,
    data: change.data != null
      ? _parseJson(change.data!)
      : null,
    seqnum: change.seqnum,
    timestamp: DateTime.now(),
  );

  /// Convert to SynclibOperation
  SynclibOperation toSynclibOperation() {
    switch (operation) {
      case 'insert':
        return SynclibOperation.insert;
      case 'update':
        return SynclibOperation.update;
      case 'delete':
        return SynclibOperation.delete;
      default:
        throw ArgumentError('Unknown operation: $operation');
    }
  }

  static Map<String, dynamic>? _parseJson(String jsonString) {
    try {
      return jsonDecode(jsonString) as Map<String, dynamic>;
    } catch (e) {
      return null;
    }
  }
}

/// Batch of changes (for efficient syncing)
class ChangesBatchMessage extends SyncMessage {
  final List<ChangeMessage> changes;
  final int? fromSeqnum;
  final int? toSeqnum;

  const ChangesBatchMessage({
    required this.changes,
    this.fromSeqnum,
    this.toSeqnum,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'changes_batch',
    'changes': changes.map((c) => c.toMap()).toList(),
    if (fromSeqnum != null) 'from_seqnum': fromSeqnum,
    if (toSeqnum != null) 'to_seqnum': toSeqnum,
  };

  factory ChangesBatchMessage.fromMap(Map<String, dynamic> map) {
    final changesData = map['changes'] as List;
    return ChangesBatchMessage(
      changes: changesData
        .map((c) => ChangeMessage.fromMap(c as Map<String, dynamic>))
        .toList(),
      fromSeqnum: map['from_seqnum'] as int?,
      toSeqnum: map['to_seqnum'] as int?,
    );
  }
}

/// Acknowledgment that changes were received and processed
class AckMessage extends SyncMessage {
  final int seqnum;
  final bool success;
  final String? error;
  /// Server-assigned seqnum for the row (set by Postgres trigger).
  /// Client should update the local row's seqnum column with this value.
  final int? serverSeqnum;

  const AckMessage({
    required this.seqnum,
    required this.success,
    this.error,
    this.serverSeqnum,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'ack',
    'seqnum': seqnum,
    'success': success,
    if (error != null) 'error': error,
    if (serverSeqnum != null) 'server_seqnum': serverSeqnum,
  };

  factory AckMessage.fromMap(Map<String, dynamic> map) => AckMessage(
    seqnum: map['seqnum'] as int,
    success: map['success'] as bool,
    error: map['error'] as String?,
    serverSeqnum: map['server_seqnum'] as int?,
  );
}

/// Request for changes from server
class RequestChangesMessage extends SyncMessage {
  final int? sinceSeqnum;
  final int? limit;
  final String? table;

  const RequestChangesMessage({
    this.sinceSeqnum,
    this.limit,
    this.table,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'request_changes',
    'since_seqnum': sinceSeqnum ?? 0,
    if (limit != null) 'limit': limit,
    if (table != null) 'table': table
  };

  factory RequestChangesMessage.fromMap(Map<String, dynamic> map) =>
    RequestChangesMessage(
      sinceSeqnum: map['since_seqnum'] as int?,
      limit: map['limit'] as int?,
      table: map['table'] as String?
    );
}

/// Error message
class ErrorMessage extends SyncMessage {
  final String code;
  final String message;
  final Map<String, dynamic>? details;

  const ErrorMessage({
    required this.code,
    required this.message,
    this.details,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'error',
    'code': code,
    'message': message,
    if (details != null) 'details': details,
  };

  factory ErrorMessage.fromMap(Map<String, dynamic> map) => ErrorMessage(
    code: map['code'] as String,
    message: map['message'] as String,
    details: map['details'] as Map<String, dynamic>?,
  );
}

/// Phoenix channel reply message (for hello response)
class PhoenixReplyMessage extends SyncMessage {
  final String status;
  final Map<String, dynamic> response;

  const PhoenixReplyMessage({
    required this.status,
    required this.response,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'phx_reply',
    'status': status,
    'response': response,
  };

  factory PhoenixReplyMessage.fromMap(Map<String, dynamic> map) {
    // Phoenix replies are already unwrapped by the WebSocketManager
    // Just return the response payload
    return PhoenixReplyMessage(
      status: map['status'] as String? ?? 'ok',
      response: map,
    );
  }
}

/// Schema migration confirmation message
class SchemaConfirmMessage extends SyncMessage {
  final int version;

  const SchemaConfirmMessage({
    required this.version,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'schema_migrated',
    'version': version,
  };

  factory SchemaConfirmMessage.fromMap(Map<String, dynamic> map) =>
    SchemaConfirmMessage(
      version: map['version'] as int,
    );
}

/// Snapshot batch message (streaming table data)
class SnapshotBatchMessage extends SyncMessage {
  final String streamId;
  final String table;
  final List<Map<String, dynamic>> rows;

  const SnapshotBatchMessage({
    required this.streamId,
    required this.table,
    required this.rows,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'snapshot_batch',
    'stream_id': streamId,
    'table': table,
    'rows': rows,
  };

  factory SnapshotBatchMessage.fromMap(Map<String, dynamic> map) {
    final rowsData = map['rows'] as List? ?? [];
    return SnapshotBatchMessage(
      streamId: map['stream_id'] as String,
      table: map['table'] as String,
      rows: rowsData.map((r) => Map<String, dynamic>.from(r as Map)).toList(),
    );
  }
}

/// Snapshot complete message (marks end of snapshot stream)
class SnapshotCompleteMessage extends SyncMessage {
  final String streamId;
  final String channelId;

  const SnapshotCompleteMessage({
    required this.streamId,
    required this.channelId,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'snapshot_complete',
    'stream_id': streamId,
    'channel_id': channelId,
  };

  factory SnapshotCompleteMessage.fromMap(Map<String, dynamic> map) =>
    SnapshotCompleteMessage(
      streamId: map['stream_id'] as String,
      channelId: map['channel_id'] as String,
    );
}

/// Schema update notification message
/// Notifies clients that a new schema version is available
class SchemaUpdateMessage extends SyncMessage {
  final int newVersion;
  final List<Map<String, dynamic>>? migrations;
  final int timestamp;

  const SchemaUpdateMessage({
    required this.newVersion,
    this.migrations,
    required this.timestamp,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'schema_update',
    'new_version': newVersion,
    if (migrations != null) 'migrations': migrations,
    'timestamp': timestamp,
  };

  factory SchemaUpdateMessage.fromMap(Map<String, dynamic> map) =>
    SchemaUpdateMessage(
      newVersion: map['new_version'] as int,
      migrations: map['migrations'] != null
        ? List<Map<String, dynamic>>.from(map['migrations'] as List)
        : null,
      timestamp: map['timestamp'] as int,
    );
}

// ============================================================================
// SIMPLIFIED SYNC HANDSHAKE PROTOCOL MESSAGES
// ============================================================================

/// Reference to a specific row in a table
class RowRef {
  final String table;
  final String rowId;

  const RowRef({required this.table, required this.rowId});

  Map<String, dynamic> toMap() => {'table': table, 'row_id': rowId};

  factory RowRef.fromMap(Map<String, dynamic> map) => RowRef(
    table: map['table'] as String,
    rowId: map['row_id'] as String,
  );
}

/// A pending local change to push to server
class PendingChange {
  final int localSeqnum;
  final String table;
  final String rowId;
  final String operation; // insert/update/delete
  final Map<String, dynamic>? data;

  const PendingChange({
    required this.localSeqnum,
    required this.table,
    required this.rowId,
    required this.operation,
    this.data,
  });

  Map<String, dynamic> toMap() => {
    'local_seqnum': localSeqnum,
    'table': table,
    'row_id': rowId,
    'operation': operation,
    if (data != null) 'data': data,
  };

  factory PendingChange.fromMap(Map<String, dynamic> map) => PendingChange(
    localSeqnum: map['local_seqnum'] as int,
    table: map['table'] as String,
    rowId: map['row_id'] as String,
    operation: map['operation'] as String,
    data: map['data'] as Map<String, dynamic>?,
  );
}

/// Unified sync request message
/// Handles push (pending changes), pull (table seqnums), schema, and stripped content
class SyncRequestMessage extends SyncMessage {
  final String clientId;
  final int schemaVersion;

  /// Per-table seqnums for incremental pull
  final Map<String, int> tableSeqnums;

  /// Specific tables to sync (null = all configured)
  final List<String>? tables;

  /// Force refresh these tables (ignore seqnums)
  final List<String>? forceRefreshTables;

  /// Rows that have _stripped=true locally - server will send fresh versions
  final List<RowRef>? strippedRows;

  /// Local changes to push to server
  final List<PendingChange>? pendingChanges;

  /// Channel role: "push", "pull", or "bidirectional".
  /// Tells the server whether to send data back, accept changes, or both.
  final String? role;

  const SyncRequestMessage({
    required this.clientId,
    required this.schemaVersion,
    required this.tableSeqnums,
    this.tables,
    this.forceRefreshTables,
    this.strippedRows,
    this.pendingChanges,
    this.role,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'sync_request',
    'client_id': clientId,
    'schema_version': schemaVersion,
    'table_seqnums': tableSeqnums,
    if (tables != null) 'tables': tables,
    if (forceRefreshTables != null) 'force_refresh_tables': forceRefreshTables,
    if (strippedRows != null) 'stripped_rows': strippedRows!.map((r) => r.toMap()).toList(),
    if (pendingChanges != null) 'pending_changes': pendingChanges!.map((c) => c.toMap()).toList(),
    if (role != null) 'role': role,
  };

  factory SyncRequestMessage.fromMap(Map<String, dynamic> map) {
    final tableSeqnums = Map<String, int>.from(map['table_seqnums'] as Map? ?? {});
    final strippedRowsRaw = map['stripped_rows'] as List?;
    final pendingChangesRaw = map['pending_changes'] as List?;

    return SyncRequestMessage(
      clientId: map['client_id'] as String,
      schemaVersion: map['schema_version'] as int? ?? 0,
      tableSeqnums: tableSeqnums,
      tables: (map['tables'] as List?)?.cast<String>(),
      forceRefreshTables: (map['force_refresh_tables'] as List?)?.cast<String>(),
      strippedRows: strippedRowsRaw?.map((r) => RowRef.fromMap(r as Map<String, dynamic>)).toList(),
      pendingChanges: pendingChangesRaw?.map((c) => PendingChange.fromMap(c as Map<String, dynamic>)).toList(),
      role: map['role'] as String?,
    );
  }
}

/// Schema migrations response (sent if schema upgrade needed)
class SchemaMigrationsMessage extends SyncMessage {
  final String? streamId;
  final int targetVersion;
  final List<Map<String, dynamic>> migrations;

  const SchemaMigrationsMessage({
    this.streamId,
    required this.targetVersion,
    required this.migrations,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'schema_migrations',
    if (streamId != null) 'stream_id': streamId,
    'target_version': targetVersion,
    'migrations': migrations,
  };

  factory SchemaMigrationsMessage.fromMap(Map<String, dynamic> map) {
    final migrations = (map['migrations'] as List? ?? map['statements'] as List? ?? [])
        .map((m) => m is Map ? Map<String, dynamic>.from(m) : {'sql': m.toString()})
        .toList();
    return SchemaMigrationsMessage(
      streamId: map['stream_id'] as String?,
      targetVersion: map['target_version'] as int? ?? 0,
      migrations: migrations,
    );
  }
}

/// Single change acknowledgment
class ChangeAck {
  final int localSeqnum;
  final bool success;
  final int? serverSeqnum;
  final String? error;

  const ChangeAck({
    required this.localSeqnum,
    required this.success,
    this.serverSeqnum,
    this.error,
  });

  Map<String, dynamic> toMap() => {
    'local_seqnum': localSeqnum,
    'success': success,
    if (serverSeqnum != null) 'server_seqnum': serverSeqnum,
    if (error != null) 'error': error,
  };

  factory ChangeAck.fromMap(Map<String, dynamic> map) => ChangeAck(
    localSeqnum: map['local_seqnum'] as int,
    success: map['success'] as bool,
    serverSeqnum: map['server_seqnum'] as int?,
    error: map['error'] as String?,
  );
}

/// Batch of change acknowledgments from server
class ChangeAcksMessage extends SyncMessage {
  final String? streamId;
  final List<ChangeAck> acks;

  const ChangeAcksMessage({this.streamId, required this.acks});

  @override
  Map<String, dynamic> toMap() => {
    'type': 'change_acks',
    if (streamId != null) 'stream_id': streamId,
    'acks': acks.map((a) => a.toMap()).toList(),
  };

  factory ChangeAcksMessage.fromMap(Map<String, dynamic> map) {
    final acksRaw = map['acks'] as List? ?? [];
    return ChangeAcksMessage(
      streamId: map['stream_id'] as String?,
      acks: acksRaw.map((a) => ChangeAck.fromMap(a as Map<String, dynamic>)).toList(),
    );
  }
}

/// Data batch for sync response (streamed)
class SyncDataBatchMessage extends SyncMessage {
  final String? streamId;
  final String table;
  final List<Map<String, dynamic>> rows;
  final bool isStrippedRefresh; // Indicates this batch contains refreshed stripped rows

  const SyncDataBatchMessage({
    this.streamId,
    required this.table,
    required this.rows,
    this.isStrippedRefresh = false,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'sync_data_batch',
    if (streamId != null) 'stream_id': streamId,
    'table': table,
    'rows': rows,
    if (isStrippedRefresh) 'is_stripped_refresh': isStrippedRefresh,
  };

  factory SyncDataBatchMessage.fromMap(Map<String, dynamic> map) {
    final rowsData = map['rows'] as List? ?? [];
    return SyncDataBatchMessage(
      streamId: map['stream_id'] as String?,
      table: map['table'] as String,
      rows: rowsData.map((r) => Map<String, dynamic>.from(r as Map)).toList(),
      isStrippedRefresh: map['is_stripped_refresh'] as bool? ?? false,
    );
  }
}

/// Sync completion signal with final state and stats
class SyncCompleteMessage extends SyncMessage {
  final String? streamId;
  final int schemaVersion;
  final Map<String, int> tableSeqnums;

  // Stats from server
  final bool schemaUpgraded;
  final int migrationsApplied;
  final int pushTotal;
  final int pushSuccess;
  final int pushFailed;
  final Map<String, Map<String, dynamic>> pushByTable;
  final int pullTotal;
  final Map<String, Map<String, dynamic>> pullByTable;
  final int strippedRefreshed;
  final int? elapsedMs;

  const SyncCompleteMessage({
    this.streamId,
    required this.schemaVersion,
    required this.tableSeqnums,
    this.schemaUpgraded = false,
    this.migrationsApplied = 0,
    this.pushTotal = 0,
    this.pushSuccess = 0,
    this.pushFailed = 0,
    this.pushByTable = const {},
    this.pullTotal = 0,
    this.pullByTable = const {},
    this.strippedRefreshed = 0,
    this.elapsedMs,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'sync_complete',
    if (streamId != null) 'stream_id': streamId,
    'schema_version': schemaVersion,
    'table_seqnums': tableSeqnums,
  };

  factory SyncCompleteMessage.fromMap(Map<String, dynamic> map) {
    final stats = map['stats'] as Map<String, dynamic>? ?? {};

    // Parse push_by_table
    final pushByTableRaw = stats['push_by_table'] as Map? ?? {};
    final pushByTable = pushByTableRaw.map((k, v) =>
        MapEntry(k.toString(), Map<String, dynamic>.from(v as Map)));

    // Parse pull_by_table
    final pullByTableRaw = stats['pull_by_table'] as Map? ?? {};
    final pullByTable = pullByTableRaw.map((k, v) =>
        MapEntry(k.toString(), Map<String, dynamic>.from(v as Map)));

    return SyncCompleteMessage(
      streamId: map['stream_id'] as String?,
      schemaVersion: map['schema_version'] as int? ?? 0,
      tableSeqnums: Map<String, int>.from(map['table_seqnums'] as Map? ?? {}),
      schemaUpgraded: stats['schema_upgraded'] as bool? ?? false,
      migrationsApplied: stats['migrations_applied'] as int? ?? 0,
      pushTotal: stats['push_total'] as int? ?? 0,
      pushSuccess: stats['push_success'] as int? ?? 0,
      pushFailed: stats['push_failed'] as int? ?? 0,
      pushByTable: pushByTable,
      pullTotal: stats['pull_total'] as int? ?? 0,
      pullByTable: pullByTable,
      strippedRefreshed: stats['stripped_refreshed'] as int? ?? 0,
      elapsedMs: stats['elapsed_ms'] as int?,
    );
  }
}

// ============================================================================
// END SIMPLIFIED SYNC HANDSHAKE PROTOCOL MESSAGES
// ============================================================================

// ============================================================================
// MERKLE TREE INTEGRITY VERIFICATION PROTOCOL MESSAGES
// ============================================================================

/// Information about a table's Merkle tree
class MerkleTableInfo {
  final String rootHash;
  final int blockCount;
  final int rowCount;

  const MerkleTableInfo({
    required this.rootHash,
    required this.blockCount,
    required this.rowCount,
  });

  Map<String, dynamic> toMap() => {
    'root_hash': rootHash,
    'block_count': blockCount,
    'row_count': rowCount,
  };

  factory MerkleTableInfo.fromMap(Map<String, dynamic> map) => MerkleTableInfo(
    rootHash: map['root_hash'] as String,
    blockCount: map['block_count'] as int,
    rowCount: map['row_count'] as int,
  );
}

/// Mismatch information returned by server
class MerkleMismatch {
  final String table;
  final String serverRootHash;
  final int serverBlockCount;
  final int serverRowCount;

  /// Server's scoped row IDs for this table on this channel.
  /// The client should use these to filter its merkle computation
  /// to the same set of rows the server sees.
  final List<String>? rowIds;

  const MerkleMismatch({
    required this.table,
    required this.serverRootHash,
    required this.serverBlockCount,
    required this.serverRowCount,
    this.rowIds,
  });

  factory MerkleMismatch.fromMap(Map<String, dynamic> map) => MerkleMismatch(
    table: map['table'] as String,
    serverRootHash: map['server_root_hash'] as String,
    serverBlockCount: map['server_block_count'] as int,
    serverRowCount: map['server_row_count'] as int,
    rowIds: (map['row_ids'] as List?)?.cast<String>(),
  );
}

/// Client sends Merkle roots for integrity verification
class MerkleVerifyMessage extends SyncMessage {
  final Map<String, MerkleTableInfo> tableHashes;
  final int? blockSize;

  const MerkleVerifyMessage({
    required this.tableHashes,
    this.blockSize,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'merkle_verify',
    'table_hashes': tableHashes.map((k, v) => MapEntry(k, v.toMap())),
    if (blockSize != null) 'block_size': blockSize,
  };

  factory MerkleVerifyMessage.fromMap(Map<String, dynamic> map) {
    final rawHashes = map['table_hashes'] as Map? ?? {};
    return MerkleVerifyMessage(
      tableHashes: rawHashes.map((k, v) => MapEntry(
        k.toString(),
        MerkleTableInfo.fromMap(v as Map<String, dynamic>),
      )),
      blockSize: map['block_size'] as int?,
    );
  }
}

/// Server response to Merkle verification
class MerkleVerifyResponse extends SyncMessage {
  final String status; // 'ok' or 'mismatch'
  final List<String>? verifiedTables;
  final List<MerkleMismatch>? mismatches;

  const MerkleVerifyResponse({
    required this.status,
    this.verifiedTables,
    this.mismatches,
  });

  bool get isOk => status == 'ok';
  bool get hasMismatches => status == 'mismatch' && mismatches != null && mismatches!.isNotEmpty;

  @override
  Map<String, dynamic> toMap() => {
    'type': 'merkle_verify_response',
    'status': status,
    if (verifiedTables != null) 'verified_tables': verifiedTables,
    if (mismatches != null) 'mismatches': mismatches!.map((m) => {
      'table': m.table,
      'server_root_hash': m.serverRootHash,
      'server_block_count': m.serverBlockCount,
      'server_row_count': m.serverRowCount,
    }).toList(),
  };

  factory MerkleVerifyResponse.fromMap(Map<String, dynamic> map) {
    final mismatchesRaw = map['mismatches'] as List?;
    // Status might be 'ok' or 'mismatch' - default to 'ok' if missing
    final status = (map['status'] as String?) ?? 'ok';
    return MerkleVerifyResponse(
      status: status,
      verifiedTables: (map['verified_tables'] as List?)?.cast<String>(),
      mismatches: mismatchesRaw?.map((m) =>
        MerkleMismatch.fromMap(m as Map<String, dynamic>)).toList(),
    );
  }
}

/// Client sends block-level hashes for a specific table
class MerkleBlockHashesMessage extends SyncMessage {
  final String table;
  final List<String> blockHashes;
  final int? blockSize;

  const MerkleBlockHashesMessage({
    required this.table,
    required this.blockHashes,
    this.blockSize,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'merkle_block_hashes',
    'table': table,
    'block_hashes': blockHashes,
    if (blockSize != null) 'block_size': blockSize,
  };

  factory MerkleBlockHashesMessage.fromMap(Map<String, dynamic> map) =>
    MerkleBlockHashesMessage(
      table: map['table'] as String,
      blockHashes: (map['block_hashes'] as List).cast<String>(),
      blockSize: map['block_size'] as int?,
    );
}

/// Server identifies which blocks differ
class MerkleBlockHashesResponse extends SyncMessage {
  final String table;
  final List<int> differingBlocks;

  const MerkleBlockHashesResponse({
    required this.table,
    required this.differingBlocks,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'merkle_block_hashes_response',
    'table': table,
    'differing_blocks': differingBlocks,
  };

  factory MerkleBlockHashesResponse.fromMap(Map<String, dynamic> map) =>
    MerkleBlockHashesResponse(
      table: map['table'] as String,
      differingBlocks: (map['differing_blocks'] as List).cast<int>(),
    );
}

/// Client requests data for specific blocks
class MerkleFetchBlocksMessage extends SyncMessage {
  final String table;
  final List<int> blocks;
  final int? blockSize;

  const MerkleFetchBlocksMessage({
    required this.table,
    required this.blocks,
    this.blockSize,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'merkle_fetch_blocks',
    'table': table,
    'blocks': blocks,
    if (blockSize != null) 'block_size': blockSize,
  };

  factory MerkleFetchBlocksMessage.fromMap(Map<String, dynamic> map) =>
    MerkleFetchBlocksMessage(
      table: map['table'] as String,
      blocks: (map['blocks'] as List).cast<int>(),
      blockSize: map['block_size'] as int?,
    );
}

/// Server sends data for a block
class MerkleFetchBlocksResponse extends SyncMessage {
  final String table;
  final int block;
  final List<Map<String, dynamic>> rows;

  const MerkleFetchBlocksResponse({
    required this.table,
    required this.block,
    required this.rows,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'merkle_fetch_blocks_response',
    'table': table,
    'block': block,
    'rows': rows,
  };

  factory MerkleFetchBlocksResponse.fromMap(Map<String, dynamic> map) {
    final rowsRaw = map['rows'] as List? ?? [];
    return MerkleFetchBlocksResponse(
      table: map['table'] as String,
      block: map['block'] as int,
      rows: rowsRaw.map((r) => Map<String, dynamic>.from(r as Map)).toList(),
    );
  }
}

/// Server response to merkle_push_blocks (client -> server push repair).
class MerklePushBlocksResponse extends SyncMessage {
  final String table;
  final int blockIndex;
  final int applied;
  final int rejected;
  final int deleted;
  final List<Map<String, dynamic>> errors;

  const MerklePushBlocksResponse({
    required this.table,
    required this.blockIndex,
    required this.applied,
    required this.rejected,
    required this.deleted,
    this.errors = const [],
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'merkle_push_blocks_response',
    'table': table,
    'block_index': blockIndex,
    'applied': applied,
    'rejected': rejected,
    'deleted': deleted,
    'errors': errors,
  };

  factory MerklePushBlocksResponse.fromMap(Map<String, dynamic> map) {
    final errorsRaw = map['errors'] as List? ?? [];
    return MerklePushBlocksResponse(
      table: map['table'] as String? ?? '',
      blockIndex: map['block_index'] as int? ?? 0,
      applied: map['applied'] as int? ?? 0,
      rejected: map['rejected'] as int? ?? 0,
      deleted: map['deleted'] as int? ?? 0,
      errors: errorsRaw.map((e) => Map<String, dynamic>.from(e as Map)).toList(),
    );
  }
}

/// Server response to merkle_lww_blocks (last-write-wins resolution).
class MerkleLwwBlocksResponse extends SyncMessage {
  final String table;
  final int blockIndex;

  /// Row IDs where the client had a newer last_modified_ms and server accepted.
  final List<String> clientWins;

  /// Full row data for rows where the server had a newer last_modified_ms.
  /// Client should overwrite local data with these.
  final List<Map<String, dynamic>> serverWins;

  final int appliedFromClient;
  final int sentToClient;

  const MerkleLwwBlocksResponse({
    required this.table,
    required this.blockIndex,
    this.clientWins = const [],
    this.serverWins = const [],
    this.appliedFromClient = 0,
    this.sentToClient = 0,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'merkle_lww_blocks_response',
    'table': table,
    'block_index': blockIndex,
    'client_wins': clientWins,
    'server_wins': serverWins,
    'applied_from_client': appliedFromClient,
    'sent_to_client': sentToClient,
  };

  factory MerkleLwwBlocksResponse.fromMap(Map<String, dynamic> map) {
    final serverWinsRaw = map['server_wins'] as List? ?? [];
    final clientWinsRaw = map['client_wins'] as List? ?? [];
    return MerkleLwwBlocksResponse(
      table: map['table'] as String? ?? '',
      blockIndex: map['block_index'] as int? ?? 0,
      clientWins: clientWinsRaw.map((e) => e.toString()).toList(),
      serverWins: serverWinsRaw.map((r) => Map<String, dynamic>.from(r as Map)).toList(),
      appliedFromClient: map['applied_from_client'] as int? ?? 0,
      sentToClient: map['sent_to_client'] as int? ?? 0,
    );
  }
}

// ============================================================================
// END MERKLE TREE INTEGRITY VERIFICATION PROTOCOL MESSAGES
// ============================================================================
