import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:synclib_sync/synclib_sync.dart';

import 'helpers.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('General sync', () {
    setUp(() async {
      await resetServer();
    });

    testWidgets('alice creates items, bob joins and sees them',
        (tester) async {
      final alice = await createTestClient('alice', 'room-general-1');

      await addItems(alice, ['apple', 'banana', 'cherry']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      final bob = await createTestClient('bob', 'room-general-1');
      final synced = await waitForItemCount(bob, 3);
      expect(synced, isTrue);

      final bobItems = await getItems(bob);
      expect(bobItems, hasLength(3));
      final names = bobItems
          .map((i) => jsonDecode(i['document'] as String)['name'] as String)
          .toList()
        ..sort();
      expect(names, equals(['apple', 'banana', 'cherry']));

      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets('bob deletes an item, alice sees the deletion',
        (tester) async {
      final alice = await createTestClient('alice', 'room-general-2');

      final ids = await addItems(alice, ['item-a', 'item-b', 'item-c']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      final bob = await createTestClient('bob', 'room-general-2');
      final synced = await waitForItemCount(bob, 3);
      expect(synced, isTrue);

      // Bob deletes item-b
      await deleteItem(bob, ids[1]);
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Alice syncs and should now have 2 items
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      final aliceCount = await getItemCount(alice);
      expect(aliceCount, equals(2));

      final aliceItems = await getItems(alice);
      final names = aliceItems
          .map((i) => jsonDecode(i['document'] as String)['name'] as String)
          .toList()
        ..sort();
      expect(names, equals(['item-a', 'item-c']));

      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets('alice updates an item, bob sees the updated value',
        (tester) async {
      final alice = await createTestClient('alice', 'room-general-3');

      final ids = await addItems(alice, ['old-name']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      final bob = await createTestClient('bob', 'room-general-3');
      final synced = await waitForItemCount(bob, 1);
      expect(synced, isTrue);

      // Alice updates the item
      await updateItem(alice, ids[0], 'new-name');
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Bob syncs and should see the updated name
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      final bobItems = await getItems(bob);
      expect(bobItems, hasLength(1));
      expect(
        jsonDecode(bobItems[0]['document'] as String)['name'],
        equals('new-name'),
      );

      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets('both clients add items concurrently, both see all items',
        (tester) async {
      final alice = await createTestClient('alice', 'room-general-4');
      final bob = await createTestClient('bob', 'room-general-4');

      await addItems(alice, ['alice-item-1', 'alice-item-2']);
      await addItems(bob, ['bob-item-1', 'bob-item-2']);

      await alice.syncClient.sync();
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      // Sync again to ensure all items propagated
      await alice.syncClient.sync();
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      final aliceCount = await getItemCount(alice);
      final bobCount = await getItemCount(bob);
      expect(aliceCount, equals(4));
      expect(bobCount, equals(4));

      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets('items are scoped to their room', (tester) async {
      final alice = await createTestClient('alice', 'room-scoped-a');
      await addItems(alice, ['room-a-item']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Bob in room-b should see 0 items
      final bob = await createTestClient('bob', 'room-scoped-b');
      await Future.delayed(const Duration(seconds: 1));
      final bobCount = await getItemCount(bob);
      expect(bobCount, equals(0));

      // Carol in room-a should see 1 item
      final carol = await createTestClient('carol', 'room-scoped-a');
      final synced = await waitForItemCount(carol, 1);
      expect(synced, isTrue);

      await disposeClient(alice);
      await disposeClient(bob);
      await disposeClient(carol);
    });

    testWidgets('reconnect preserves data and syncs new items',
        (tester) async {
      final alice = await createTestClient('alice', 'room-general-5');

      await addItems(alice, ['before-disconnect']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Bob adds items while alice will be away
      final bob = await createTestClient('bob', 'room-general-5');
      await waitForItemCount(bob, 1);
      await addItems(bob, ['while-away']);
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Alice reconnects (fresh SyncClient, same DB file)
      final alice2 = await reconnectClient(alice);
      await Future.delayed(const Duration(seconds: 1));

      final aliceCount = await getItemCount(alice2);
      expect(aliceCount, equals(2));

      await disposeClient(alice2);
      await disposeClient(bob);
    });

    testWidgets('delete then re-add with same name works correctly',
        (tester) async {
      final alice = await createTestClient('alice', 'room-general-6');
      final bob = await createTestClient('bob', 'room-general-6');

      // Alice adds an item
      final ids = await addItems(alice, ['ephemeral']);
      await alice.syncClient.sync();
      await waitForItemCount(bob, 1);
      await Future.delayed(const Duration(seconds: 1));

      // Alice deletes it
      await deleteItem(alice, ids[0]);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Bob syncs and sees 0 active items
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));
      final bobCountAfterDelete = await getItemCount(bob);
      expect(bobCountAfterDelete, equals(0));

      // Alice re-adds an item with the same name (different id)
      await addItems(alice, ['ephemeral']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Bob syncs and sees 1 active item again
      await bob.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));
      final bobCountAfterReAdd = await getItemCount(bob);
      expect(bobCountAfterReAdd, equals(1));

      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets('many items sync correctly', (tester) async {
      final alice = await createTestClient('alice', 'room-general-7');

      final names = List.generate(
        20,
        (i) => 'item-${i.toString().padLeft(3, '0')}',
      );
      await addItems(alice, names);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 2));

      final bob = await createTestClient('bob', 'room-general-7');
      final synced =
          await waitForItemCount(bob, 20, timeout: const Duration(seconds: 30));
      expect(synced, isTrue);

      final bobCount = await getItemCount(bob);
      expect(bobCount, equals(20));

      await disposeClient(alice);
      await disposeClient(bob);
    });

    testWidgets('sync state transitions correctly', (tester) async {
      final alice = await createTestClient('alice', 'room-general-8');

      // After createTestClient, should be ready
      expect(alice.syncClient.syncState, equals(SyncState.ready));

      // Add items and sync — should go through syncing → ready
      final states = <SyncState>[];
      final sub = alice.syncClient.syncStateChanges.listen((state) {
        states.add(state);
      });

      await addItems(alice, ['state-test']);
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      // Should have seen syncing then ready
      expect(states, contains(SyncState.syncing));
      expect(states, contains(SyncState.ready));

      await sub.cancel();
      await disposeClient(alice);
    });

    testWidgets('pending changes are cleared after successful sync',
        (tester) async {
      final alice = await createTestClient('alice', 'room-general-9');

      // Add items — should create pending changes
      await addItems(alice, ['pending-1', 'pending-2']);

      final pendingBefore = await getPendingChangeCount(alice);
      expect(pendingBefore, greaterThan(0));

      // Sync — pending changes should be cleared
      await alice.syncClient.sync();
      await Future.delayed(const Duration(seconds: 1));

      final pendingAfter = await getPendingChangeCount(alice);
      expect(pendingAfter, equals(0));

      await disposeClient(alice);
    });
  });
}
