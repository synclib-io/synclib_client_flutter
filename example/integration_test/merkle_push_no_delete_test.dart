import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

import 'helpers.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Merkle push without spurious deletes', () {
    setUp(() async {
      await resetServer();
    });

    testWidgets(
        'items are not deleted after multiple sync cycles with merkle enabled',
        (tester) async {
      final alice = await createTestClient(
        'alice',
        'room-merkle-push-1',
        merkleVerifyInterval: const Duration(seconds: 5),
      );
      final bob = await createTestClient(
        'bob',
        'room-merkle-push-1',
        merkleVerifyInterval: const Duration(seconds: 5),
      );

      // Alice adds 2 items
      await addItems(alice, ['alice-1', 'alice-2']);
      await alice.syncClient.sync();
      final synced1 = await waitForItemCount(bob, 2);
      expect(synced1, isTrue);

      // Bob adds 1 item
      await addItems(bob, ['bob-1']);
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Alice syncs to see Bob's item
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Both should have 3 items
      expect(await getItemCount(alice), equals(3));
      expect(await getItemCount(bob), equals(3));

      // Collect merkle events
      final aliceMerkleEvents = <dynamic>[];
      final bobMerkleEvents = <dynamic>[];
      final sub1 = alice.syncClient.merkleVerificationEvents.listen((e) {
        aliceMerkleEvents.add(e);
      });
      final sub2 = bob.syncClient.merkleVerificationEvents.listen((e) {
        bobMerkleEvents.add(e);
      });

      // Run multiple sync cycles and wait for merkle verification
      for (var i = 0; i < 3; i++) {
        await alice.syncClient.sync();
        await bob.syncClient.sync();
        await Future.delayed(const Duration(seconds: 2));
      }

      // Wait for at least one merkle verification cycle
      await Future.delayed(const Duration(seconds: 5));

      // Verify counts are still 3 — no items were spuriously deleted
      expect(await getItemCount(alice), equals(3),
          reason: 'Alice should still have 3 items');
      expect(await getItemCount(bob), equals(3),
          reason: 'Bob should still have 3 items');

      // Merkle should not have found mismatches
      for (final event in aliceMerkleEvents) {
        expect(event.hadMismatches, isFalse,
            reason: 'Alice merkle should not find mismatches');
      }
      for (final event in bobMerkleEvents) {
        expect(event.hadMismatches, isFalse,
            reason: 'Bob merkle should not find mismatches');
      }

      await sub1.cancel();
      await sub2.cancel();
      await disposeClient(alice);
      await disposeClient(bob);
    });
  });
}
