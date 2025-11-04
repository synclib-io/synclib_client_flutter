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
      case 'job_update':
        return JobUpdateMessage.fromMap(map);
      case 'schema_update':
        return SchemaUpdateMessage.fromMap(map);
      case 'livestream:started':
      case 'livestream:stopped':
        return LivestreamMessage.fromMap(map);
      default:
        throw UnsupportedError('Unknown message type: $type');
    }
  }
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

  const AckMessage({
    required this.seqnum,
    required this.success,
    this.error,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'ack',
    'seqnum': seqnum,
    'success': success,
    if (error != null) 'error': error,
  };

  factory AckMessage.fromMap(Map<String, dynamic> map) => AckMessage(
    seqnum: map['seqnum'] as int,
    success: map['success'] as bool,
    error: map['error'] as String?,
  );
}

/// Request for changes from server
class RequestChangesMessage extends SyncMessage {
  final int? sinceSeqnum;
  final int? limit;

  const RequestChangesMessage({
    this.sinceSeqnum,
    this.limit,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'request_changes',
    if (sinceSeqnum != null) 'since_seqnum': sinceSeqnum,
    if (limit != null) 'limit': limit,
  };

  factory RequestChangesMessage.fromMap(Map<String, dynamic> map) =>
    RequestChangesMessage(
      sinceSeqnum: map['since_seqnum'] as int?,
      limit: map['limit'] as int?,
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

  const SnapshotCompleteMessage({
    required this.streamId,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'snapshot_complete',
    'stream_id': streamId,
  };

  factory SnapshotCompleteMessage.fromMap(Map<String, dynamic> map) =>
    SnapshotCompleteMessage(
      streamId: map['stream_id'] as String,
    );
}

/// Job update message (from ECS tasks via webhook)
class JobUpdateMessage extends SyncMessage {
  final int currentStep;
  final int totalSteps;
  final String stepType;
  final String jobId;
  final String userId;
  final String phoenixChannelId;
  final String? filename;

  const JobUpdateMessage({
    required this.currentStep,
    required this.totalSteps,
    required this.stepType,
    required this.jobId,
    required this.userId,
    required this.phoenixChannelId,
    this.filename,
  });

  @override
  Map<String, dynamic> toMap() => {
    'type': 'job_update',
    'current_step': currentStep,
    'total_steps': totalSteps,
    'step_type': stepType,
    'job_id': jobId,
    'user_id': userId,
    'phoenix_channel_id': phoenixChannelId,
    if (filename != null) 'filename': filename,
  };

  factory JobUpdateMessage.fromMap(Map<String, dynamic> map) =>
    JobUpdateMessage(
      currentStep: map['current_step'] as int,
      totalSteps: map['total_steps'] as int,
      stepType: map['step_type'] as String,
      jobId: map['job_id'] as String,
      userId: map['user_id'] as String,
      phoenixChannelId: map['phoenix_channel_id'] as String,
      filename: map['filename'] as String?,
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

/// Livestream event message
/// Notifies tribe members when a livestream starts or stops
class LivestreamMessage extends SyncMessage {
  final String event; // 'livestream:started' or 'livestream:stopped'
  final String? streamId;
  final String? tribeId;
  final String? userId; // User who started/stopped the stream
  final String? hlsUrl; // HLS URL for playing the stream
  final int timestamp;

  const LivestreamMessage({
    required this.event,
    this.streamId,
    this.tribeId,
    this.userId,
    this.hlsUrl,
    required this.timestamp,
  });

  bool get isStarted => event == 'livestream:started';
  bool get isStopped => event == 'livestream:stopped';

  @override
  Map<String, dynamic> toMap() => {
    'type': event,
    if (streamId != null) 'stream_id': streamId,
    if (tribeId != null) 'tribe_id': tribeId,
    if (userId != null) 'user_id': userId,
    if (hlsUrl != null) 'hls_url': hlsUrl,
    'timestamp': timestamp,
  };

  factory LivestreamMessage.fromMap(Map<String, dynamic> map) {
    // The event type comes from the Phoenix channel event name
    final event = map['type'] as String? ?? map['event'] as String?;

    return LivestreamMessage(
      event: event ?? 'livestream:started',
      streamId: map['stream_id'] as String?,
      tribeId: map['tribe_id'] as String?,
      userId: map['user_id'] as String?,
      hlsUrl: map['hls_url'] as String?,
      timestamp: map['timestamp'] as int? ?? DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
  }
}
