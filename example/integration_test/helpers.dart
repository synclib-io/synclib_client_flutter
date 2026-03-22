import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dart_jsonwebtoken/dart_jsonwebtoken.dart';
import 'package:synclib_flutter/synclib_flutter.dart';
import 'package:synclib_sync/synclib_sync.dart';
import 'package:http/http.dart' as http;

const _serverUrl = 'ws://localhost:4444/socket/websocket';
const _jwtSecret = 'synclib_test_secret';

/// Wrapper holding a SyncClient, its database, and test metadata.
class TestClient {
  final SyncClient syncClient;
  final SynclibDatabase db;
  final String dbPath;
  final String roomId;
  final String userId;

  TestClient({
    required this.syncClient,
    required this.db,
    required this.dbPath,
    required this.roomId,
    required this.userId,
  });
}

/// Generate an HS256 JWT for the given [userId].
String generateTestToken(String userId) {
  final jwt = JWT({
    'sub': userId,
    'iat': DateTime.now().millisecondsSinceEpoch ~/ 1000,
    'exp':
        DateTime.now().add(const Duration(hours: 24)).millisecondsSinceEpoch ~/
            1000,
  });
  return jwt.sign(SecretKey(_jwtSecret), algorithm: JWTAlgorithm.HS256);
}

/// Create a test client connected to the org server.
///
/// Opens a temp database, creates a SyncClient, connects, and syncs.
Future<TestClient> createTestClient(
  String userId,
  String roomId, {
  Duration? merkleVerifyInterval,
}) async {
  final ts = DateTime.now().millisecondsSinceEpoch;
  final dbPath =
      '${Directory.systemTemp.path}/test_${userId}_${roomId}_$ts.db';
  final clientId = 'test_${userId}_${roomId}_$ts';

  final db = await SynclibDatabase.open(dbPath);

  final config = SyncClientConfig(
    dbPath: dbPath,
    serverUrl: _serverUrl,
    clientId: clientId,
    database: db,
    channels: [
      SyncChannel(
        topic: 'sync:room:$roomId',
        role: ChannelRole.bidirectional,
        tables: [
          SyncTable.lww('items', hashColumns: ['last_modified_ms']),
        ],
      ),
    ],
    codec: SyncCodecType.json,
    merkleVerifyInterval: merkleVerifyInterval,
    merkleSkipColumns: ['row_hash'],
  );

  final client = SyncClient(config);
  await client.initialize();

  final token = generateTestToken(userId);
  await client.connect(token: token);
  await client.sync();

  return TestClient(
    syncClient: client,
    db: db,
    dbPath: dbPath,
    roomId: roomId,
    userId: userId,
  );
}

/// Create a fresh SyncClient reusing an existing database file.
///
/// Disposes the old client's SyncClient (but not the DB file), then creates
/// a new SyncClient + SynclibDatabase pointing at the same path. This
/// simulates reconnection (data persists, new WebSocket connection).
Future<TestClient> reconnectClient(TestClient old) async {
  // Dispose old sync client only (keep DB file)
  try {
    await old.syncClient.disconnect();
  } catch (_) {}
  try {
    await old.db.close();
  } catch (_) {}

  final db = await SynclibDatabase.open(old.dbPath);
  final ts = DateTime.now().millisecondsSinceEpoch;
  final clientId = 'test_${old.userId}_${old.roomId}_$ts';

  final config = SyncClientConfig(
    dbPath: old.dbPath,
    serverUrl: _serverUrl,
    clientId: clientId,
    database: db,
    channels: [
      SyncChannel(
        topic: 'sync:room:${old.roomId}',
        role: ChannelRole.bidirectional,
        tables: [
          SyncTable.lww('items', hashColumns: ['last_modified_ms']),
        ],
      ),
    ],
    codec: SyncCodecType.json,
    merkleSkipColumns: ['row_hash'],
  );

  final client = SyncClient(config);
  await client.initialize();

  final token = generateTestToken(old.userId);
  await client.connect(token: token);
  await client.sync();

  return TestClient(
    syncClient: client,
    db: db,
    dbPath: old.dbPath,
    roomId: old.roomId,
    userId: old.userId,
  );
}

/// Reset the server's test data.
Future<void> resetServer() async {
  final response = await http.delete(
    Uri.parse('http://localhost:4444/api/test/items'),
  );
  if (response.statusCode >= 300) {
    // ignore: avoid_print
    print('resetServer returned ${response.statusCode}: ${response.body}');
  }
}

/// Add an item and return its ID.
Future<String> addItem(TestClient client, String name) async {
  final id = _uuid();
  final now = DateTime.now().millisecondsSinceEpoch;
  final doc = jsonEncode({'name': name});

  await client.db.writeWithParams(
    tableName: 'items',
    rowId: id,
    operation: SynclibOperation.insert,
    sql:
        'INSERT OR REPLACE INTO items (id, document, room_id, last_modified_ms) VALUES (?, jsonb(?), ?, ?)',
    params: [id, doc, client.roomId, now],
    data: jsonEncode({
      'id': id,
      'document': doc,
      'room_id': client.roomId,
      'last_modified_ms': now,
    }),
  );

  return id;
}

/// Add multiple items, return list of IDs.
Future<List<String>> addItems(TestClient client, List<String> names) async {
  final ids = <String>[];
  for (final name in names) {
    ids.add(await addItem(client, name));
  }
  return ids;
}

/// Soft-delete an item.
Future<void> deleteItem(TestClient client, String id) async {
  final now = DateTime.now().millisecondsSinceEpoch;

  await client.db.writeWithParams(
    tableName: 'items',
    rowId: id,
    operation: SynclibOperation.delete,
    sql: 'UPDATE items SET deleted_at = ?, last_modified_ms = ? WHERE id = ?',
    params: [now, now, id],
    data: jsonEncode({
      'id': id,
      'room_id': client.roomId,
      'deleted_at': now,
      'last_modified_ms': now,
    }),
  );
}

/// Update an item's name.
Future<void> updateItem(TestClient client, String id, String name) async {
  final now = DateTime.now().millisecondsSinceEpoch;
  final doc = jsonEncode({'name': name});

  await client.db.writeWithParams(
    tableName: 'items',
    rowId: id,
    operation: SynclibOperation.update,
    sql: 'UPDATE items SET document = jsonb(?), last_modified_ms = ? WHERE id = ?',
    params: [doc, now, id],
    data: jsonEncode({
      'id': id,
      'document': doc,
      'room_id': client.roomId,
      'last_modified_ms': now,
    }),
  );
}

/// Get active (non-deleted) items for the client's room.
Future<List<Map<String, dynamic>>> getItems(TestClient client) async {
  final roomId = client.roomId;
  return client.db.read(
    "SELECT id, json(document) as document, room_id, last_modified_ms "
    "FROM items WHERE room_id = '$roomId' AND deleted_at IS NULL "
    "ORDER BY last_modified_ms DESC",
  );
}

/// Get count of active items for the client's room.
Future<int> getItemCount(TestClient client) async {
  final roomId = client.roomId;
  try {
    final rows = await client.db.read(
      "SELECT COUNT(*) as cnt FROM items WHERE room_id = '$roomId' AND deleted_at IS NULL",
    );
    return (rows.first['cnt'] as int?) ?? 0;
  } catch (_) {
    return 0;
  }
}

/// Poll until item count >= [expected], or timeout.
Future<bool> waitForItemCount(
  TestClient client,
  int expected, {
  Duration timeout = const Duration(seconds: 15),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    final count = await getItemCount(client);
    if (count >= expected) return true;
    await Future.delayed(const Duration(milliseconds: 200));
  }
  return (await getItemCount(client)) >= expected;
}

/// Poll until item count == [expected] exactly, or timeout.
Future<bool> waitForItemCountExact(
  TestClient client,
  int expected, {
  Duration timeout = const Duration(seconds: 15),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    final count = await getItemCount(client);
    if (count == expected) return true;
    await Future.delayed(const Duration(milliseconds: 200));
  }
  return (await getItemCount(client)) == expected;
}

/// Get the number of pending (unsynced) changes.
Future<int> getPendingChangeCount(TestClient client) async {
  try {
    final changes = await client.db.getPendingChanges();
    return changes.length;
  } catch (_) {
    return 0;
  }
}

/// Get row_hash statistics for active items.
Future<Map<String, int>> getRowHashStats(TestClient client) async {
  final roomId = client.roomId;
  try {
    final rows = await client.db.read(
      "SELECT row_hash FROM items WHERE room_id = '$roomId' AND deleted_at IS NULL",
    );
    int withHash = 0, withoutHash = 0, sentinel = 0;
    for (final row in rows) {
      final hash = row['row_hash'];
      if (hash is String && hash.length == 64) {
        withHash++;
      } else if (hash == '') {
        sentinel++;
      } else {
        withoutHash++;
      }
    }
    return {
      'total': rows.length,
      'withHash': withHash,
      'withoutHash': withoutHash,
      'sentinel': sentinel,
    };
  } catch (_) {
    return {'total': 0, 'withHash': 0, 'withoutHash': 0, 'sentinel': 0};
  }
}

/// Get row_hash values for all active items (for detailed inspection).
Future<List<Map<String, dynamic>>> getRowHashes(TestClient client) async {
  final roomId = client.roomId;
  try {
    return client.db.read(
      "SELECT id, row_hash FROM items WHERE room_id = '$roomId' AND deleted_at IS NULL ORDER BY id",
    );
  } catch (_) {
    return [];
  }
}

/// Disconnect, close DB, and clean up temp file.
Future<void> disposeClient(TestClient client) async {
  try {
    await client.syncClient.disconnect();
  } catch (_) {}
  try {
    await client.db.close();
  } catch (_) {}
  try {
    File(client.dbPath).deleteSync();
  } catch (_) {}
}

/// Simple UUID v4 generator.
String _uuid() {
  final rng = DateTime.now().millisecondsSinceEpoch;
  // Use a combination of timestamp and hashCode for uniqueness
  final bytes = List<int>.generate(16, (i) {
    final val = (rng + i * 31 + DateTime.now().microsecondsSinceEpoch) & 0xFF;
    return val;
  });
  bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
  bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant
  final hex = bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
  return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}';
}
