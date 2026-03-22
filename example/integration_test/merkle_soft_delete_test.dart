import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

import 'helpers.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Merkle verification with soft-deleted items', () {
    setUp(() async {
      await resetServer();
    });

    testWidgets(
        'soft-deleted items sync correctly and merkle stays stable',
        (tester) async {
      final alice = await createTestClient(
        'alice',
        'room-merkle-del',
        merkleVerifyInterval: const Duration(seconds: 5),
      );
      final bob = await createTestClient('bob', 'room-merkle-del');

      // Alice adds 2 items, bob sees them
      final ids = await addItems(alice, ['keep-me', 'delete-me']);
      await alice.syncClient.sync();
      final synced = await waitForItemCount(bob, 2);
      expect(synced, isTrue);

      // Alice deletes one item
      await deleteItem(alice, ids[1]);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Bob syncs and should see 1 active item
      await bob.syncClient.sync();
      final exactCount = await waitForItemCountExact(bob, 1);
      expect(exactCount, isTrue);

      // Collect merkle events
      final merkleEvents = <dynamic>[];
      final sub = alice.syncClient.merkleVerificationEvents.listen((event) {
        merkleEvents.add(event);
      });

      // Wait for merkle verification to run
      await Future.delayed(const Duration(seconds: 7));

      // Re-sync both and verify counts are still stable
      await alice.syncClient.sync();
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      final aliceCount = await getItemCount(alice);
      final bobCount = await getItemCount(bob);
      expect(aliceCount, equals(1));
      expect(bobCount, equals(1));

      // Merkle should not have triggered spurious repairs
      for (final event in merkleEvents) {
        expect(event.hadMismatches, isFalse,
            reason: 'Merkle should not find mismatches after soft delete sync');
      }

      await sub.cancel();
      await disposeClient(alice);
      await disposeClient(bob);
    });
  });
}
