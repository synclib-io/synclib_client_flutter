import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

import 'helpers.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Sync after reconnection (Bug #7)', () {
    setUp(() async {
      await resetServer();
    });

    testWidgets('client auto-syncs after reconnect and receives new items',
        (tester) async {
      final clientA = await createTestClient('alice', 'room-reconn');
      final clientB = await createTestClient('bob', 'room-reconn');

      // A adds an item, both should see it
      await addItems(clientA, ['item-before-disconnect']);
      await clientA.syncClient.sync();
      final syncedB = await waitForItemCount(clientB, 1);
      expect(syncedB, isTrue);

      // A disconnects and reconnects (fresh SyncClient, same DB)
      final clientA2 = await reconnectClient(clientA);

      // B adds an item while A was reconnecting
      await addItems(clientB, ['item-while-offline']);
      await clientB.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // A syncs and should see both items
      await clientA2.syncClient.sync();
      final syncedA = await waitForItemCount(clientA2, 2);
      expect(syncedA, isTrue);

      final countA = await getItemCount(clientA2);
      final countB = await getItemCount(clientB);
      expect(countA, equals(2));
      expect(countB, equals(2));

      await disposeClient(clientA2);
      await disposeClient(clientB);
    });
  });
}
