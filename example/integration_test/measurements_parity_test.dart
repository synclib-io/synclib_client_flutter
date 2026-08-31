import 'dart:convert';
import 'dart:io';

import 'package:dart_jsonwebtoken/dart_jsonwebtoken.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:synclib_flutter/synclib_flutter.dart';
import 'package:synclib_sync/synclib_sync.dart';

/// Tier 2 (cross-language) of the HTTP-fallback parity suite.
///
/// Drives the REAL Dart client HTTP fallback (syncUnifiedOverHttp + fetchPage)
/// against the same live `sync_server` the JS Tier-1 harness (e2e-parity/)
/// runs against — USE_CASE=app_builder, port 4242, `measurements` model. No
/// WebSocket: exactly the degraded path. If the Dart client's HTTP push+pull
/// round-trips correctly here, the Dart implementation agrees with the server
/// (and transitively with the JS client, which Tier 1 proved equals the server).
///
/// Boot the server first (from sync_server/):
///   export USE_CASE=app_builder
///   ENV_FILE=.env.app_builder iex -S mix phx.server
///
/// Run (from synclib_sync/example/):
///   flutter test integration_test/measurements_parity_test.dart -d macos
const _serverUrl = 'ws://localhost:4242/socket/websocket';
// Matches APP_BUILDER_JWT_SECRET in sync_server/.env.app_builder.
const _appBuilderSecret = 'local_dev_jwt_secret_stable_across_restarts';
const _userId = 'e2e-dart-measure';

String _memberToken() {
  final jwt = JWT({
    'sub': _userId,
    'permissions': ['write:own_data', 'admin:all'],
    'iat': DateTime.now().millisecondsSinceEpoch ~/ 1000,
    'exp': DateTime.now().add(const Duration(hours: 1)).millisecondsSinceEpoch ~/ 1000,
  });
  return jwt.sign(SecretKey(_appBuilderSecret), algorithm: JWTAlgorithm.HS256);
}

class _Harness {
  final SyncClient client;
  final SynclibDatabase db;
  final String dbPath;
  _Harness(this.client, this.db, this.dbPath);
}

Future<_Harness> _makeClient() async {
  final ts = DateTime.now().microsecondsSinceEpoch;
  final dbPath = '${Directory.systemTemp.path}/measurements_parity_$ts.db';
  final db = await SynclibDatabase.open(dbPath);

  // Pre-create measurements with the server-shaped sync columns so
  // applyServerRows has somewhere to write (the WS handshake normally does this).
  await db.exec('''
    CREATE TABLE IF NOT EXISTS measurements (
      "id" TEXT PRIMARY KEY,
      "document" BLOB,
      "owner_id" TEXT,
      "availability" TEXT,
      "product_id" TEXT,
      "locale" TEXT,
      "last_modified_ms" INTEGER,
      "seqnum" INTEGER,
      "deleted_at" TEXT,
      "row_hash" TEXT
    )
  ''');

  final config = SyncClientConfig(
    dbPath: dbPath,
    serverUrl: _serverUrl,
    clientId: 'app_e2e_dart',
    database: db,
    channels: [
      SyncChannel(
        topic: 'sync:user:$_userId',
        role: ChannelRole.bidirectional,
        tables: [SyncTable.lww('measurements')],
      ),
    ],
    codec: SyncCodecType.json,
    merkleSkipColumns: ['row_hash'],
  );

  final client = SyncClient(config);
  await client.initialize();
  // Feed the HTTP path a token WITHOUT opening a socket.
  client.updateToken(_memberToken());
  return _Harness(client, db, dbPath);
}

Future<void> _dispose(_Harness h) async {
  try {
    await h.db.close();
  } catch (_) {}
  try {
    File(h.dbPath).deleteSync();
  } catch (_) {}
}

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('measurements HTTP fallback parity (Dart, no WebSocket)', () {
    test('syncUnifiedOverHttp pushes a local change; ack stamps the server seqnum', () async {
      final h = await _makeClient();
      try {
        final id = 'e2e-dart-${DateTime.now().microsecondsSinceEpoch}';
        final now = DateTime.now().millisecondsSinceEpoch;

        await h.db.writeWithParams(
          tableName: 'measurements',
          rowId: id,
          operation: SynclibOperation.insert,
          sql:
              'INSERT OR REPLACE INTO measurements ("id","owner_id","document","last_modified_ms") '
              'VALUES (?, ?, jsonb(?), ?)',
          params: [id, _userId, jsonEncode({'weight': 181}), now],
          data: jsonEncode({
            'id': id,
            'owner_id': _userId,
            'document': {'weight': 181},
            'last_modified_ms': now,
          }),
        );

        await h.client.syncUnifiedOverHttp();

        final rows = await h.db.read("SELECT seqnum FROM measurements WHERE id = '$id'");
        expect(rows, isNotEmpty, reason: 'the pushed row should still exist locally');
        expect((rows.first['seqnum'] as int?) ?? 0, greaterThan(0),
            reason: 'the server-assigned seqnum should have been applied from the push ack');

        final pending = await h.db.getPendingChanges();
        expect(pending.where((c) => c.rowId == id), isEmpty,
            reason: 'the acked change should have been removed from the outbound queue');
      } finally {
        await _dispose(h);
      }
    });

    test('fetchPage pulls the user scope and applies rows locally', () async {
      final h = await _makeClient();
      try {
        final id = 'e2e-dart-pull-${DateTime.now().microsecondsSinceEpoch}';
        final now = DateTime.now().millisecondsSinceEpoch;
        await h.db.writeWithParams(
          tableName: 'measurements',
          rowId: id,
          operation: SynclibOperation.insert,
          sql:
              'INSERT OR REPLACE INTO measurements ("id","owner_id","document","last_modified_ms") '
              'VALUES (?, ?, jsonb(?), ?)',
          params: [id, _userId, jsonEncode({'reps': 12}), now],
          data: jsonEncode({
            'id': id,
            'owner_id': _userId,
            'document': {'reps': 12},
            'last_modified_ms': now,
          }),
        );
        await h.client.syncUnifiedOverHttp();

        // Wipe the local copy, then pull it back purely over HTTP.
        await h.db.exec("DELETE FROM measurements WHERE id = '$id'");

        final page = await h.client.fetchPage('measurements', scope: 'user', since: 0, limit: 200);

        expect(page.received, greaterThan(0), reason: 'server should return rows for this user');
        expect(page.applied, greaterThan(0), reason: 'fetched rows should apply into local SQLite');
        expect(page.nextSeqnum, greaterThan(0));

        final rows = await h.db.read("SELECT id FROM measurements WHERE id = '$id'");
        expect(rows, isNotEmpty, reason: 'the pulled row should have landed back in local SQLite');
      } finally {
        await _dispose(h);
      }
    });
  });
}
