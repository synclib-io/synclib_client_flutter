import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

import 'helpers.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Merkle repair and stability', () {
    setUp(() async {
      await resetServer();
    });

    testWidgets(
        'merkle detects server-side drift and client wins via LWW repair',
        (tester) async {
      final alice = await createTestClient(
        'alice',
        'room-merkle-drift-1',
        merkleVerifyInterval: const Duration(seconds: 3),
      );

      // Subscribe to merkle events
      final merkleEvents = <dynamic>[];
      final sub = alice.syncClient.merkleVerificationEvents.listen((event) {
        merkleEvents.add(event);
      });

      // Add items, push, then pull to update tracked seqnum.
      // The first sync pushes the items; the second sync pulls them back
      // so the client's per-table seqnum is up-to-date. Without this,
      // the next sync would pull ALL items (seqnum=0) and self-heal the
      // corruption before merkle gets a chance to detect it.
      final ids = await addItems(alice, ['item-1', 'item-2', 'item-3']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Verify all 3 items synced and have valid hashes
      final statsBefore = await getRowHashStats(alice);
      expect(statsBefore['total'], equals(3));
      expect(statsBefore['withHash'], equals(3));

      // Verify client and server hashes match (important for merkle)
      final hashBefore =
          (await getRowHashes(alice)).firstWhere((r) => r['id'] == ids[0]);
      final originalHash = hashBefore['row_hash'] as String;
      final serverBefore = await getItemOnServer(ids[0]);
      expect(originalHash, equals(serverBefore!['row_hash']),
          reason: 'Client and server hashes should match before corruption');

      // Modify a row directly on the server, bypassing only the seqnum
      // trigger. The row_hash trigger still fires, so row_hash is correctly
      // recomputed for the new data. seqnum stays unchanged, so normal
      // sync won't detect this — only merkle can.
      await updateItemOnServer(ids[0], {'last_modified_ms': 0});

      // Verify the server has different data AND a different row_hash
      final serverItem = await getItemOnServer(ids[0]);
      expect(serverItem, isNotNull);
      expect(serverItem!['last_modified_ms'], equals(0));
      expect(serverItem!['row_hash'], isNot(equals(originalHash)),
          reason: 'Server row_hash should differ after modification');

      // Wait for merkle interval to elapse, then trigger via sync
      await Future.delayed(const Duration(seconds: 4));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // Merkle should have detected a mismatch and repaired
      expect(merkleEvents.isNotEmpty, isTrue,
          reason: 'Merkle verification should have run');
      expect(merkleEvents.any((e) => e.hadMismatches), isTrue,
          reason: 'Merkle should detect server-side data drift');
      expect(
          merkleEvents
              .where((e) => e.hadMismatches)
              .first
              .repairedTables
              .contains('items'),
          isTrue,
          reason: 'items table should be in repairedTables');

      // LWW repair: client's last_modified_ms is higher than server's (0),
      // so client wins. Client data should be unchanged.
      expect(await getItemCount(alice), equals(3));

      // After repair, run another merkle cycle — it should be clean now.
      merkleEvents.clear();
      await Future.delayed(const Duration(seconds: 4));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      expect(merkleEvents.isNotEmpty, isTrue,
          reason: 'Second merkle cycle should have run');
      for (final event in merkleEvents) {
        expect(event.hadMismatches, isFalse,
            reason: 'No mismatches expected after repair');
      }

      await sub.cancel();
      await disposeClient(alice);
    });

    testWidgets(
        'merkle detects server-side drift and server wins via LWW repair',
        (tester) async {
      final alice = await createTestClient(
        'alice',
        'room-merkle-drift-2',
        merkleVerifyInterval: const Duration(seconds: 3),
      );

      // Subscribe to merkle events
      final merkleEvents = <dynamic>[];
      final sub = alice.syncClient.merkleVerificationEvents.listen((event) {
        merkleEvents.add(event);
      });

      // Add items, push, then pull to update tracked seqnum.
      final ids = await addItems(alice, ['item-1', 'item-2']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Verify hashes are populated
      final statsBefore = await getRowHashStats(alice);
      expect(statsBefore['withHash'], equals(2));

      // Modify a row on the server with a FUTURE timestamp so server wins LWW.
      // Only seqnum trigger is disabled; row_hash trigger still fires.
      final futureMs = DateTime.now().millisecondsSinceEpoch + 1000000;
      await updateItemOnServer(ids[0], {'last_modified_ms': futureMs});

      // Wait for merkle interval, then trigger
      await Future.delayed(const Duration(seconds: 4));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // Merkle should detect and repair
      expect(merkleEvents.any((e) => e.hadMismatches), isTrue,
          reason: 'Merkle should detect server-side drift');

      // Server wins LWW because its last_modified_ms is higher.
      // Client should now have the server's last_modified_ms value.
      final items = await getItems(alice);
      final repairedItem = items.firstWhere((i) => i['id'] == ids[0]);
      expect(repairedItem['last_modified_ms'], equals(futureMs),
          reason:
              'Client should have server data after server-wins LWW repair');

      // Second merkle cycle should be clean
      merkleEvents.clear();
      await Future.delayed(const Duration(seconds: 4));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      expect(merkleEvents.isNotEmpty, isTrue,
          reason: 'Second merkle cycle should have run');
      for (final event in merkleEvents) {
        expect(event.hadMismatches, isFalse,
            reason: 'No mismatches expected after LWW repair');
      }

      await sub.cancel();
      await disposeClient(alice);
    });

    testWidgets(
        'merkle stays stable after complex multi-client operations',
        (tester) async {
      // Use a long interval so merkle doesn't fire mid-setup.
      // We trigger it explicitly after all operations complete.
      final alice = await createTestClient(
        'alice',
        'room-merkle-complex-1',
        merkleVerifyInterval: const Duration(seconds: 15),
      );
      final bob = await createTestClient(
        'bob',
        'room-merkle-complex-1',
        merkleVerifyInterval: const Duration(seconds: 15),
      );

      // Alice adds 3 items
      final aliceIds = await addItems(alice, ['a1', 'a2', 'a3']);
      await alice.syncClient.sync();
      await waitForItemCount(bob, 3);

      // Bob adds 2 items
      await addItems(bob, ['b1', 'b2']);
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Both should have 5
      expect(await getItemCount(alice), equals(5));
      expect(await getItemCount(bob), equals(5));

      // Alice updates an item
      await updateItem(alice, aliceIds[0], 'a1-updated');
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Alice deletes an item
      await deleteItem(alice, aliceIds[1]);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Bob syncs to get the update + delete
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Both should have 4 active items
      expect(await getItemCount(alice), equals(4));
      expect(await getItemCount(bob), equals(4));

      // Subscribe to merkle events AFTER all operations are synced,
      // so we only capture the final verification results.
      final aliceMerkle = <dynamic>[];
      final bobMerkle = <dynamic>[];
      final sub1 = alice.syncClient.merkleVerificationEvents.listen((e) {
        aliceMerkle.add(e);
      });
      final sub2 = bob.syncClient.merkleVerificationEvents.listen((e) {
        bobMerkle.add(e);
      });

      // Wait for merkle interval, then trigger on both
      await Future.delayed(const Duration(seconds: 16));
      await alice.syncClient.sync();
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Merkle should not find mismatches after properly synced operations
      expect(aliceMerkle.isNotEmpty, isTrue,
          reason: 'Alice should have merkle events');
      for (final event in aliceMerkle) {
        expect(event.hadMismatches, isFalse,
            reason: 'Alice merkle should not find mismatches');
      }
      expect(bobMerkle.isNotEmpty, isTrue,
          reason: 'Bob should have merkle events');
      for (final event in bobMerkle) {
        expect(event.hadMismatches, isFalse,
            reason: 'Bob merkle should not find mismatches');
      }

      // Counts should be stable
      expect(await getItemCount(alice), equals(4));
      expect(await getItemCount(bob), equals(4));

      await sub1.cancel();
      await sub2.cancel();
      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets('multiple merkle cycles remain stable with no changes',
        (tester) async {
      final alice = await createTestClient(
        'alice',
        'room-merkle-stable-1',
        merkleVerifyInterval: const Duration(seconds: 3),
      );

      // Subscribe to merkle events immediately
      final merkleEvents = <dynamic>[];
      final sub = alice.syncClient.merkleVerificationEvents.listen((event) {
        merkleEvents.add(event);
      });

      // Add items and sync
      await addItems(alice, ['stable-1', 'stable-2', 'stable-3']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Trigger multiple merkle cycles by calling sync() after each interval.
      // Merkle runs at the end of syncUnified(), so we need explicit calls.
      for (var i = 0; i < 3; i++) {
        await Future.delayed(const Duration(seconds: 4));
        await alice.syncClient.sync();
      }
      await Future.delayed(const Duration(seconds: 1));

      // Should have seen at least 2 merkle events, none with mismatches
      expect(merkleEvents.length, greaterThanOrEqualTo(2),
          reason: 'Should have multiple merkle verification events');
      for (final event in merkleEvents) {
        expect(event.hadMismatches, isFalse,
            reason: 'No mismatches expected in stable state');
      }

      // Item count should not change
      expect(await getItemCount(alice), equals(3));

      await sub.cancel();
      await disposeClient(alice);
    });

    testWidgets('merkle remains stable after updates', (tester) async {
      final alice = await createTestClient(
        'alice',
        'room-merkle-update-1',
        merkleVerifyInterval: const Duration(seconds: 3),
      );

      // Subscribe to merkle events immediately
      final merkleEvents = <dynamic>[];
      final sub = alice.syncClient.merkleVerificationEvents.listen((event) {
        merkleEvents.add(event);
      });

      // Add items and sync
      final ids = await addItems(alice, ['before-update-1', 'before-update-2']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Update an item and sync
      await updateItem(alice, ids[0], 'after-update-1');
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Wait for interval, then trigger merkle via sync
      await Future.delayed(const Duration(seconds: 4));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Merkle should not find mismatches after a properly synced update
      expect(merkleEvents.isNotEmpty, isTrue,
          reason: 'Merkle should have run at least once');
      for (final event in merkleEvents) {
        expect(event.hadMismatches, isFalse,
            reason: 'Merkle should be stable after synced update');
      }

      // Items should still be correct
      expect(await getItemCount(alice), equals(2));

      await sub.cancel();
      await disposeClient(alice);
    });

    testWidgets('merkle handles empty table without errors', (tester) async {
      final alice = await createTestClient(
        'alice',
        'room-merkle-empty-1',
        merkleVerifyInterval: const Duration(seconds: 3),
      );

      // Subscribe to merkle events immediately
      final merkleEvents = <dynamic>[];
      final sub = alice.syncClient.merkleVerificationEvents.listen((event) {
        merkleEvents.add(event);
      });

      // Don't add any items — table is empty. Wait for interval then trigger.
      await Future.delayed(const Duration(seconds: 4));
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Merkle should handle empty table gracefully — no mismatches
      for (final event in merkleEvents) {
        expect(event.hadMismatches, isFalse,
            reason: 'Empty table should not cause merkle mismatches');
      }

      // Count should still be 0
      expect(await getItemCount(alice), equals(0));

      await sub.cancel();
      await disposeClient(alice);
    });
  });
}
