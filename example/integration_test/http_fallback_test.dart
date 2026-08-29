import 'dart:convert';
import 'dart:io';

import 'package:dart_jsonwebtoken/dart_jsonwebtoken.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:synclib_flutter/synclib_flutter.dart';
import 'package:synclib_sync/synclib_sync.dart';

/// End-to-end client test for the HTTP member sync fallback
/// (docs/http_fallback_sync_plan.md).
///
/// Drives the real client HTTP code (fetchPage + syncUnifiedOverHttp) against a
/// running TG sync_server, with no WebSocket connection — exactly the degraded
/// path. Start the server first:
///
///   cd sync_server && USE_CASE=tg APP_BUILDER_JWT_SECRET=e2e_test_secret \
///     mix phx.server        # binds :4242, DB app_builder_dev
///
/// Auth: an HS256 member token via the `app_` verifier path (clientId starts
/// with `app_`, signed with APP_BUILDER_JWT_SECRET). `admin:all` bypasses the
/// server RowSanitizer so pulled rows aren't stripped.
const _serverUrl = 'ws://localhost:4242/socket/websocket';
const _appBuilderSecret = 'e2e_test_secret';
const _userId = 'e2e-dart-user';

String _memberToken() {
  final jwt = JWT({
    'sub': _userId,
    'tribe': 'e2e-tribe',
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
  final dbPath = '${Directory.systemTemp.path}/http_fallback_$ts.db';
  final db = await SynclibDatabase.open(dbPath);

  // Pre-create storage_files (server-shaped columns) so applyServerRows has
  // somewhere to write — the example app creates its tables the same way.
  await db.exec('''
    CREATE TABLE IF NOT EXISTS storage_files (
      "id" TEXT PRIMARY KEY,
      "document" BLOB,
      "parent_id" TEXT,
      "last_modified_ms" INTEGER,
      "seqnum" INTEGER,
      "deleted_at" TEXT,
      "row_hash" TEXT,
      "locale" TEXT
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
        tables: [SyncTable.lww('storage_files')],
      ),
    ],
    codec: SyncCodecType.json,
    merkleSkipColumns: ['row_hash'],
  );

  final client = SyncClient(config);
  await client.initialize();
  // Set the token for the HTTP path without opening a WebSocket.
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

  group('HTTP member sync fallback (no WebSocket)', () {
    test('syncUnifiedOverHttp pushes a local change; ack updates the local seqnum', () async {
      final h = await _makeClient();
      try {
        final id = 'e2e-dart-${DateTime.now().microsecondsSinceEpoch}';
        final now = DateTime.now().millisecondsSinceEpoch;

        // Local write -> enqueues a pending change (seqnum starts at 0 locally).
        await h.db.writeWithParams(
          tableName: 'storage_files',
          rowId: id,
          operation: SynclibOperation.insert,
          sql:
              'INSERT OR REPLACE INTO storage_files ("id","parent_id","document","last_modified_ms") '
              'VALUES (?, ?, jsonb(?), ?)',
          params: [id, _userId, jsonEncode({'from': 'dart'}), now],
          data: jsonEncode({
            'id': id,
            'parent_id': _userId,
            'document': {'from': 'dart'},
            'last_modified_ms': now,
          }),
        );

        // Push over HTTP (+ pull). The ack should delete the queue entry and
        // stamp the server seqnum onto the local row.
        await h.client.syncUnifiedOverHttp();

        final rows = await h.db.read(
          "SELECT seqnum FROM storage_files WHERE id = '$id'",
        );
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
        // Ensure there is at least one row for this user by pushing one first.
        final id = 'e2e-dart-pull-${DateTime.now().microsecondsSinceEpoch}';
        final now = DateTime.now().millisecondsSinceEpoch;
        await h.db.writeWithParams(
          tableName: 'storage_files',
          rowId: id,
          operation: SynclibOperation.insert,
          sql:
              'INSERT OR REPLACE INTO storage_files ("id","parent_id","document","last_modified_ms") '
              'VALUES (?, ?, jsonb(?), ?)',
          params: [id, _userId, jsonEncode({'k': 'v'}), now],
          data: jsonEncode({
            'id': id,
            'parent_id': _userId,
            'document': {'k': 'v'},
            'last_modified_ms': now,
          }),
        );
        await h.client.syncUnifiedOverHttp();

        // Wipe the local copy, then pull it back purely over HTTP.
        await h.db.exec("DELETE FROM storage_files WHERE id = '$id'");

        final page = await h.client.fetchPage('storage_files',
            scope: 'user', since: 0, limit: 200);

        expect(page.received, greaterThan(0), reason: 'server should return rows for this user');
        expect(page.applied, greaterThan(0), reason: 'fetched rows should apply into local SQLite');
        expect(page.nextSeqnum, greaterThan(0));

        final rows = await h.db.read("SELECT id FROM storage_files WHERE id = '$id'");
        expect(rows, isNotEmpty, reason: 'the pulled row should have landed back in local SQLite');
      } finally {
        await _dispose(h);
      }
    });
  });
}
