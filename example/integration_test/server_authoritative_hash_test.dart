import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

import 'helpers.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Server-authoritative row_hash round-trip', () {
    setUp(() async {
      await resetServer();
    });

    testWidgets('pushed items receive row_hash from server ACK',
        (tester) async {
      final client = await createTestClient('alice', 'room-hash');

      await addItems(client, ['hash-item-1', 'hash-item-2', 'hash-item-3']);
      await client.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      final stats = await getRowHashStats(client);
      expect(stats['total'], equals(3));
      expect(stats['withHash'], equals(3));
      expect(stats['withoutHash'], equals(0));

      // Verify each hash is a valid 64-char hex string
      final hashes = await getRowHashes(client);
      final hexPattern = RegExp(r'^[0-9a-f]{64}$');
      for (final row in hashes) {
        expect(row['row_hash'], matches(hexPattern));
      }

      await disposeClient(client);
    });

    testWidgets('pulled items from another client have row_hash',
        (tester) async {
      final alice = await createTestClient('alice', 'room-hash2');
      await addItems(alice, ['pull-item-1', 'pull-item-2']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      final bob = await createTestClient('bob', 'room-hash2');
      final synced = await waitForItemCount(bob, 2);
      expect(synced, isTrue);

      final stats = await getRowHashStats(bob);
      expect(stats['total'], equals(2));
      expect(stats['withHash'], equals(2));
      expect(stats['sentinel'], equals(0));

      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets(
        'merkle verification passes after sync (no spurious mismatches)',
        (tester) async {
      final client = await createTestClient(
        'alice',
        'room-hash3',
        merkleVerifyInterval: const Duration(seconds: 3),
      );

      // Collect merkle verification events
      final merkleEvents = <dynamic>[];
      final sub = client.syncClient.merkleVerificationEvents.listen((event) {
        merkleEvents.add(event);
      });

      await addItems(
          client, ['merkle-1', 'merkle-2', 'merkle-3', 'merkle-4', 'merkle-5']);
      await client.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // Verify all items have row_hash, no sentinels
      final stats = await getRowHashStats(client);
      expect(stats['withHash'], equals(5));
      expect(stats['sentinel'], equals(0));

      // Wait for merkle verification to run (interval is 3s)
      await Future.delayed(const Duration(seconds: 5));

      // If merkle ran, none should have had mismatches
      for (final event in merkleEvents) {
        expect(event.hadMismatches, isFalse,
            reason: 'Merkle verification should not find mismatches');
      }

      await sub.cancel();
      await disposeClient(client);
    });

    testWidgets('no null row_hash values after full sync cycle',
        (tester) async {
      final alice = await createTestClient('alice', 'room-hash4');
      await addItems(alice, ['null-check-1', 'null-check-2']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      final bob = await createTestClient('bob', 'room-hash4');
      await waitForItemCount(bob, 2);
      await addItems(bob, ['null-check-3']);
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // Alice syncs to get Bob's item
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Both should have 3 items, all with hashes, no nulls
      for (final entry in [
        ('alice', alice),
        ('bob', bob),
      ]) {
        final stats = await getRowHashStats(entry.$2);
        expect(stats['total'], equals(3), reason: '${entry.$1} total');
        expect(stats['withHash'], equals(3), reason: '${entry.$1} withHash');
        expect(stats['withoutHash'], equals(0),
            reason: '${entry.$1} withoutHash');
        expect(stats['sentinel'], equals(0), reason: '${entry.$1} sentinel');
      }

      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets('client row_hash exactly matches server row_hash',
        (tester) async {
      final client = await createTestClient('alice', 'room-hash6');

      final ids = await addItems(client, ['match-1', 'match-2', 'match-3']);
      await client.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // Compare each client hash directly against the server's Postgres value
      final clientRows = await client.db.read(
        'SELECT id, row_hash FROM items ORDER BY id',
      );

      for (final clientRow in clientRows) {
        final id = clientRow['id'] as String;
        final clientHash = clientRow['row_hash'] as String?;
        expect(clientHash, isNotNull, reason: 'client hash null for $id');

        final serverRow = await getItemOnServer(id);
        expect(serverRow, isNotNull, reason: 'server row missing for $id');
        final serverHash = serverRow!['row_hash'] as String?;
        expect(serverHash, isNotNull, reason: 'server hash null for $id');

        expect(clientHash, equals(serverHash),
            reason: 'hash mismatch for $id: client=$clientHash server=$serverHash');
      }

      // Also verify after an update
      await updateItem(client, ids[0], 'updated-match-1');
      await client.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      final updatedClientRow = await client.db.read(
        "SELECT row_hash FROM items WHERE id = '${ids[0]}'",
      );
      final updatedServerRow = await getItemOnServer(ids[0]);
      expect(updatedClientRow.first['row_hash'],
          equals(updatedServerRow!['row_hash']),
          reason: 'hash mismatch after update');

      await disposeClient(client);
    });

    testWidgets('both clients agree on hash after bidirectional sync',
        (tester) async {
      final alice = await createTestClient('alice', 'room-hash7');
      final ids = await addItems(alice, ['bidi-1', 'bidi-2']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // Bob joins, gets Alice's items
      final bob = await createTestClient('bob', 'room-hash7');
      await waitForItemCount(bob, 2);

      // Bob adds an item, both sync
      await addItems(bob, ['bidi-3']);
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // All 3 items should have identical hashes on alice, bob, and server
      final aliceRows = await alice.db.read(
        'SELECT id, row_hash FROM items ORDER BY id',
      );
      final bobRows = await bob.db.read(
        'SELECT id, row_hash FROM items ORDER BY id',
      );

      expect(aliceRows.length, equals(3));
      expect(bobRows.length, equals(3));

      for (int i = 0; i < aliceRows.length; i++) {
        final id = aliceRows[i]['id'] as String;
        final aliceHash = aliceRows[i]['row_hash'] as String?;
        final bobHash = bobRows[i]['row_hash'] as String?;
        final serverRow = await getItemOnServer(id);
        final serverHash = serverRow!['row_hash'] as String?;

        expect(aliceHash, isNotNull, reason: 'alice hash null for $id');
        expect(bobHash, isNotNull, reason: 'bob hash null for $id');
        expect(serverHash, isNotNull, reason: 'server hash null for $id');
        expect(aliceHash, equals(serverHash),
            reason: 'alice != server for $id');
        expect(bobHash, equals(serverHash),
            reason: 'bob != server for $id');
      }

      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets('updated items get new row_hash from server',
        (tester) async {
      final client = await createTestClient('alice', 'room-hash5');

      final ids = await addItems(client, ['will-update']);
      await client.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // Get the initial hash
      final hashesBefore = await getRowHashes(client);
      final hashBefore = hashesBefore[0]['row_hash'] as String;
      expect(hashBefore, matches(RegExp(r'^[0-9a-f]{64}$')));

      // Update the item
      await updateItem(client, ids[0], 'updated-name');
      await client.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // Hash should have changed
      final hashesAfter = await getRowHashes(client);
      final hashAfter = hashesAfter[0]['row_hash'] as String;
      expect(hashAfter, matches(RegExp(r'^[0-9a-f]{64}$')));
      expect(hashAfter, isNot(equals(hashBefore)));

      await disposeClient(client);
    });
  });
}
