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
        'merkle stays stable after complex multi-client operations',
        (tester) async {
      final alice = await createTestClient(
        'alice',
        'room-merkle-complex-1',
        merkleVerifyInterval: const Duration(seconds: 3),
      );
      final bob = await createTestClient(
        'bob',
        'room-merkle-complex-1',
        merkleVerifyInterval: const Duration(seconds: 3),
      );

      // Subscribe to merkle events on both clients
      final aliceMerkle = <dynamic>[];
      final bobMerkle = <dynamic>[];
      final sub1 = alice.syncClient.merkleVerificationEvents.listen((e) {
        aliceMerkle.add(e);
      });
      final sub2 = bob.syncClient.merkleVerificationEvents.listen((e) {
        bobMerkle.add(e);
      });

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

      // Wait for merkle interval, then trigger on both
      await Future.delayed(const Duration(seconds: 4));
      await alice.syncClient.sync();
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Merkle should not find mismatches after properly synced operations
      for (final event in aliceMerkle) {
        expect(event.hadMismatches, isFalse,
            reason: 'Alice merkle should not find mismatches');
      }
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
