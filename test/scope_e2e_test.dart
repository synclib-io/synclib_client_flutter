// Live end-to-end proof of the Dart scoped-sync fan-out (SYNCLIB-SCOPES.md).
//
// A real SyncClient with a channel declaring `activeScopes: [scopeA]` syncs
// against the running sync_server. It must pull ONLY scope A's rows into local
// SQLite and record a per-scope cursor keyed by the scope id in
// `_synclib_cursors` — never scope B's row, never a per-table cursor.
//
// The pg seeding + JWT come from ../e2e-parity/dart-scope-seed.mjs; its JSON
// handshake path is passed in SCOPE_TEST_FILE. Requires the sync_server up on
// :4242 (app_builder) and the libsynclib dylib on disk.

import 'dart:convert';
import 'dart:ffi';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:synclib_flutter/synclib_flutter.dart';
import 'package:synclib_sync/synclib_sync.dart';

void main() {
  late String token, sub, topic, scopeA;

  setUpAll(() {
    final handshakePath = Platform.environment['SCOPE_TEST_FILE'];
    expect(handshakePath, isNotNull, reason: 'SCOPE_TEST_FILE env not set');
    final hs = jsonDecode(File(handshakePath!).readAsStringSync()) as Map<String, dynamic>;
    token = hs['token'] as String;
    sub = hs['sub'] as String;
    topic = hs['topic'] as String;
    scopeA = hs['scopeA'] as String;
    // Load the native SQLite/synclib lib into the process once.
    DynamicLibrary.open('/Users/phil/projects/sync/synclibc/libsynclib.dylib');
  });

  // Fresh client + local DB, scoped to A. `channelRoleParams` lets each test
  // drive the same config over its transport of choice.
  Future<({SyncClient client, SynclibDatabase db})> freshClient() async {
    final dbPath =
        '${Directory.systemTemp.path}/synclib_scope_e2e_${DateTime.now().microsecondsSinceEpoch}.db';
    final db = await SynclibDatabase.open(dbPath);
    await db.setSchemaVersion(25);
    // The client skips any table it doesn't hold locally, so declare it with a
    // superset of the columns the server streams.
    await db.exec('''
      CREATE TABLE IF NOT EXISTS measurements (
        id TEXT PRIMARY KEY, document TEXT, owner_id TEXT, availability TEXT,
        product_id TEXT, last_modified_ms INTEGER, seqnum INTEGER,
        deleted_at TEXT, row_hash TEXT, locale TEXT, scope_ids TEXT
      )
    ''');
    final client = SyncClient(
      SyncClientConfig(
        dbPath: dbPath,
        serverUrl: 'ws://localhost:4242/socket/websocket',
        clientId: 'app_test', // HS256 local-secret path (other ids hit auth-server RS256)
        database: db,
        autoSyncOnConnect: false,
        channels: [
          SyncChannel(
            topic: topic,
            role: ChannelRole.pull,
            tables: const [SyncTable('measurements')],
            activeScopes: [scopeA], // <- only scope A is "in view"
            scopeKind: 'user_scope',
          ),
        ],
      ),
    );
    await client.initialize();
    return (client: client, db: db);
  }

  Future<({List<String> ids, List<String> units, int cursorSeqnum})> inspect(
      SynclibDatabase db) async {
    final rows = await db.read("SELECT id FROM measurements WHERE id LIKE '$sub%' ORDER BY id");
    final ids = rows.map((r) => (r['id'] as String).substring(sub.length + 1)).toList();
    final cursors = await db.read('SELECT unit, seqnum FROM _synclib_cursors');
    final units = cursors.map((r) => r['unit'] as String).toList();
    final scopeCursor = cursors.firstWhere((r) => r['unit'] == scopeA, orElse: () => <String, dynamic>{});
    return (ids: ids, units: units, cursorSeqnum: (scopeCursor['seqnum'] as int?) ?? 0);
  }

  void assertScopeA(({List<String> ids, List<String> units, int cursorSeqnum}) r, String label) {
    expect(r.ids, ['a1', 'a2'], reason: '$label: only scope A landed, b1 excluded');
    expect(r.units, contains(scopeA), reason: '$label: cursor keyed by scope id');
    expect(r.units, isNot(contains('measurements')), reason: '$label: no per-table cursor');
    expect(r.cursorSeqnum, greaterThan(0), reason: '$label: scope cursor advanced');
  }

  test('scoped SyncClient over WebSocket pulls only its scope', () async {
    final c = await freshClient();
    await c.client.connect(token: token);
    await c.client.syncUnified(skipMerkle: true);
    final r = await inspect(c.db);
    await c.client.dispose();
    assertScopeA(r, 'WS');
  }, timeout: const Timeout(Duration(seconds: 90)));

  test('scoped SyncClient over HTTP degrade pulls only its scope', () async {
    final c = await freshClient();
    // HTTP degrade: set the token for the fallback surface, never open the WS.
    c.client.updateToken(token);
    await c.client.syncUnifiedOverHttp();
    final r = await inspect(c.db);
    await c.client.dispose();
    assertScopeA(r, 'HTTP');
  }, timeout: const Timeout(Duration(seconds: 90)));
}
