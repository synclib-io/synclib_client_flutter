import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:logging/logging.dart';
import 'package:synclib_flutter/synclib_flutter.dart';
import 'package:synclib_sync/synclib_sync.dart';

/// Merkle + write-path end to end for the native Dart client, built the way
/// the Flutter scaffold builds it (member channel `sync:user:<id>`, LWW
/// tables, syncOnWrite, tables created by the server's schema migrations).
///
/// Needs a live app_builder sync_server and seeded rows. Run through
/// `e2e-parity/flutter-merkle.sh`, which seeds the database, mints the member
/// token, passes it in with --dart-define, and checks the server side after.
const _serverUrl = String.fromEnvironment('SERVER_URL', defaultValue: 'ws://localhost:4242/socket/websocket');
const _token = String.fromEnvironment('TOKEN');
const _sub = String.fromEnvironment('SUB');

const _docs = [
  {'weight': 81.5, 'unit': 'kg'},
  {
    'note': 'café ✓ 日本 \u0001',
    'nested': {
      'b': [1, {'d': 2, 'c': 3}],
      'a': null
    },
    'flag': true
  },
  {'big': 1234567890123, 'small': 0.000123, 'neg': -42, 'price': 19.99},
  {'empty': '', 'list': [], 'obj': {}},
];

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  late SyncClient client;
  late SynclibDatabase db;
  final events = <MerkleVerificationEvent>[];

  Future<List<Map<String, dynamic>>> mine(String table) =>
      db.read("SELECT id, json(document) AS document, row_hash FROM $table WHERE id LIKE '$_sub%' AND deleted_at IS NULL ORDER BY id");

  Map<String, dynamic> doc(Map<String, dynamic> row) => jsonDecode(row['document'] as String) as Map<String, dynamic>;

  // syncUnified returns at once while another sync runs; wait it out.
  Future<void> sync() async {
    while (client.isSyncing) {
      await Future.delayed(const Duration(milliseconds: 50));
    }
    await client.syncUnified();
    while (client.isSyncing) {
      await Future.delayed(const Duration(milliseconds: 50));
    }
    // Merkle events arrive on a stream; let them be delivered.
    await Future.delayed(const Duration(milliseconds: 50));
  }

  setUpAll(() async {
    expect(_token, isNotEmpty, reason: 'pass --dart-define=TOKEN=...');
    Logger.root.level = Level.INFO;
    Logger.root.onRecord.listen((r) {
      // ignore: avoid_print
      if (RegExp('merkle|verifyRoots|repair|mismatch|Already syncing|Deferred', caseSensitive: false).hasMatch(r.message)) print('[${r.loggerName}] ${r.message}');
    });
    final dbPath = '${Directory.systemTemp.path}/merkle_e2e_${DateTime.now().microsecondsSinceEpoch}.db';
    db = await SynclibDatabase.open(dbPath);
    client = SyncClient(SyncClientConfig(
      dbPath: dbPath,
      database: db,
      serverUrl: _serverUrl,
      clientId: 'app_$_sub',
      codec: SyncCodecType.json,
      channels: [
        SyncChannel(
          topic: 'sync:user:$_sub',
          role: ChannelRole.bidirectional,
          tables: const [SyncTable.lww('measurements'), SyncTable.lww('blog_posts')],
          params: {'token': _token},
        ),
      ],
      syncOnWrite: true,
      autoSyncOnConnect: false,
      merkleVerifyInterval: const Duration(milliseconds: 1),
    ));
    await client.initialize();
    client.merkleVerificationEvents.listen(events.add);
    await client.connect(token: _token, extra: {'env': 'dev'});
  });

  testWidgets('merkle verifies, repairs drift, and converges', (tester) async {
    // Pull until the seeded rows are local.
    for (var i = 0; i < 60; i++) {
      await sync();
      if ((await mine('measurements')).length == _docs.length && (await mine('blog_posts')).length == _docs.length + 1) break;
      await Future.delayed(const Duration(seconds: 1));
    }
    expect((await mine('measurements')).length, _docs.length);
    expect((await mine('blog_posts')).length, _docs.length + 1);

    // The paywalled row arrives stripped.
    final premium = (await mine('blog_posts')).firstWhere((r) => r['id'] == '$_sub-premium');
    expect(premium['document'], anyOf(isNull, contains('_stripped')));

    // A synced client verifies clean (first run only stamps the timestamp).
    await sync();
    await sync();
    expect(events.last.incomplete, isFalse);
    expect(events.last.repairedTables.where((t) => t == 'measurements' || t == 'blog_posts'), isEmpty);

    // Drift: a missing row, a stale row, a stale copy with a fast clock, and
    // a ghost of a row deleted on the server. Documents are written as jsonb,
    // as the client stores them (text there reads as corruption and forces a
    // full re-pull, which would heal the drift before merkle sees it).
    await db.exec("DELETE FROM measurements WHERE id = '$_sub-m0'");
    await db.exec("UPDATE measurements SET document = jsonb('{\"weight\":1}'), row_hash = 'stale' WHERE id = '$_sub-m1'");
    await db.exec("UPDATE measurements SET document = jsonb('{\"fast\":\"clock\"}'), last_modified_ms = 9999999999999, row_hash = 'old-version' WHERE id = '$_sub-m2'");
    await db.exec("INSERT INTO blog_posts (id, document, owner_id, availability, last_modified_ms, row_hash) "
        "VALUES ('$_sub-gone', jsonb('{\"title\":\"ghost\"}'), NULL, 'free', 1, 'ghost')");

    await sync();
    expect(events.last.incomplete, isFalse);
    expect(events.last.repairedTables.toSet(), containsAll({'measurements', 'blog_posts'}));

    final measurements = await mine('measurements');
    expect(measurements.map((r) => r['id']), contains('$_sub-m0'));
    expect(doc(measurements.firstWhere((r) => r['id'] == '$_sub-m1')), _docs[1]);
    expect(doc(measurements.firstWhere((r) => r['id'] == '$_sub-m2')), _docs[2]);
    expect((await mine('blog_posts')).map((r) => r['id']), isNot(contains('$_sub-gone')));

    // Converges.
    await sync();
    expect(events.last.incomplete, isFalse);
    expect(events.last.repairedTables.where((t) => t == 'measurements' || t == 'blog_posts'), isEmpty);
  });

  testWidgets('a write made while the same row is being pushed is not lost', (tester) async {
    final id = '$_sub-coalesce';
    Future<void> put(String rowId, String version) => db.write(
          tableName: 'measurements',
          rowId: rowId,
          operation: SynclibOperation.insert,
          sql: "INSERT OR REPLACE INTO measurements (id, document, owner_id) VALUES ('$rowId', jsonb('{\"version\":\"$version\"}'), '$_sub')",
          data: jsonEncode({'id': rowId, 'document': {'version': version}}),
        );

    // v1 first, then 99 filler writes so the server spends a while applying
    // the batch before it acks.
    await put(id, 'v1');
    for (var i = 0; i < 99; i++) {
      await put('$_sub-fill-$i', 'f');
    }
    // Read v1's queue entry and push at once: the filler writes armed the
    // syncOnWrite timer, which must not push v1 first (when it fires during
    // this sync it becomes the rerun).
    final before = await db.read("SELECT seqnum FROM _sync_changes WHERE row_id = '$id'");
    final v1Seqnum = before.first['seqnum'] as int;

    // Push the batch, then write v2 to the same row while the server is still
    // applying it. v2 merges into v1's queue entry (same seqnum).
    final pushing = client.syncUnified();
    await Future.delayed(const Duration(milliseconds: 30));
    await put(id, 'v2');
    await pushing;

    // Right after the push: v1's ack must have re-queued the merged entry
    // under a new seqnum, not deleted it. (The deferred rerun hasn't run yet.)
    final requeued = await db.read("SELECT seqnum, data FROM _sync_changes WHERE row_id = '$id'");
    expect(requeued, hasLength(1), reason: 'the v2 edit must still be queued after v1 was acked');
    expect(requeued.first['seqnum'], isNot(v1Seqnum));
    expect(requeued.first['data'] as String, contains('v2'));

    // The rerun (v2's syncOnWrite came in mid-sync) pushes it.
    for (var i = 0; i < 40; i++) {
      await Future.delayed(const Duration(milliseconds: 250));
      final pending = await db.read("SELECT count(*) AS n FROM _sync_changes WHERE row_id = '$id'");
      if (!client.isSyncing && (pending.first['n'] as int) == 0) break;
    }
    final pending = await db.read("SELECT count(*) AS n FROM _sync_changes WHERE row_id = '$id'");
    expect(pending.first['n'], 0, reason: 'v2 should have been pushed and acked');
    // The server side (document.version == "v2") is checked by the wrapper script.
  });
}
