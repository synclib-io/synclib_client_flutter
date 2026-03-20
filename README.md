# synclib-client-flutter

Flutter sync client for bidirectional database synchronization over Phoenix channels.

Connects a local SQLite database (via [synclib_flutter](https://github.com/synclib-io/synclib-flutter)) to a [synclib server](https://github.com/synclib-io/synclib-server), keeping data in sync using seqnum-based incremental pull, batched push, and Merkle tree integrity verification.

## How it works

```
Local SQLite                          synclib server (Postgres)
┌───────────┐    WebSocket / Phoenix   ┌──────────────────┐
│ your app   │◄═══════════════════════►│ sync channel      │
│            │                         │                    │
│ push ─────►│── pending changes ────► │ apply + broadcast │
│            │                         │                    │
│ ◄── pull ──│◄── newer rows ──────── │ seqnum triggers   │
│            │                         │                    │
│ verify ───►│◄── merkle compare ───► │ hash triggers     │
└───────────┘                          └──────────────────┘
```

1. Client sends a `sync_request` with its per-table seqnums and any pending local changes
2. Server applies changes, streams back newer rows, and returns a `sync_complete` with stats
3. Optionally, periodic Merkle verification detects and repairs any drift

## Quick start

```dart
import 'package:synclib_sync/synclib_sync.dart';
import 'package:synclib_flutter/synclib_flutter.dart';

// 1. Open the local database
final db = await SynclibDatabase.open('path/to/local.db');

// 2. Configure the sync client
final client = SyncClient(SyncClientConfig(
  dbPath: 'path/to/local.db',
  serverUrl: 'wss://your-server.com/socket',
  clientId: 'device-abc',
  database: db,  // share the db instance with your app
  channels: [
    SyncChannel(
      topic: 'sync:user:user-123',
      role: ChannelRole.push,
      tables: [
        SyncTable('journal_entries'),
        SyncTable('measurements'),
      ],
    ),
    SyncChannel(
      topic: 'sync:tribe:tribe-456',
      role: ChannelRole.pull,
      tables: [
        SyncTable('exercises'),
        SyncTable('workouts'),
      ],
    ),
  ],
  syncOnWrite: true,              // auto-push on local writes
  autoSyncOnConnect: true,        // sync stale tables on connect
  merkleVerifyInterval: Duration(hours: 1),
));

// 3. Initialize and connect
await client.initialize();
await client.connect(token: jwtToken);

// 4. Sync is now automatic — or call manually:
await client.syncUnified();
```

## Channels and roles

Every sync relationship is modeled as a **channel** with a **role**:

| Role | Direction | Use case |
|------|-----------|----------|
| `ChannelRole.push` | Client → Server | User-owned data (journal entries, settings) |
| `ChannelRole.pull` | Server → Client | Shared/read-only data (exercises, content) |
| `ChannelRole.bidirectional` | Both ways | Collaborative data (chat, shared docs) |

The role determines the default **repair direction** when Merkle verification finds mismatches:

- `push` → client is authoritative, sends its rows to server
- `pull` → server is authoritative, client overwrites local data
- `bidirectional` → last-write-wins (`lww`) using `last_modified_ms`

Individual tables can override the channel default:

```dart
SyncChannel(
  topic: 'sync:user:user-123',
  role: ChannelRole.push,
  tables: [
    SyncTable('journal_entries'),           // inherits push
    SyncTable.pull('notifications'),        // override: server-authoritative
    SyncTable.lww('shared_preferences'),    // override: last-write-wins
  ],
)
```

## Sync modes

### syncOnWrite

When `syncOnWrite: true`, the client subscribes to local database changes and automatically pushes after a debounce period (default 100ms). Rapid writes are batched together.

### Periodic sync

Set `periodicSyncInterval` to run `syncUnified()` on a timer:

```dart
SyncClientConfig(
  // ...
  periodicSyncInterval: Duration(minutes: 5),
)
```

### Manual sync

Call `syncUnified()` directly for on-demand sync:

```dart
await client.syncUnified();

// Force refresh specific tables (ignores seqnums, pulls all rows)
await client.syncUnified(forceRefresh: ['exercises']);
```

## Observing state

The client exposes streams for every stage of the sync lifecycle:

```dart
// Connection state (disconnected, connecting, connected, reconnecting, failed)
client.stateChanges.listen((state) => print('Connection: $state'));

// Sync state (disconnected, connecting, syncing, ready, error)
client.syncStateChanges.listen((state) => print('Sync: $state'));

// Sync progress (phase: pushing, pulling, migrating, complete)
client.syncProgress.listen((p) => print('${p.phase}: ${p.table}'));

// Sync complete with stats
client.syncComplete.listen((event) {
  print('Pulled ${event.totalRowsPulled} rows');
  print('Pushed ${event.totalChangesPushed} changes');
  if (event.schemaUpgraded) {
    print('Schema upgraded to v${event.schemaVersion}');
  }
});

// Remote changes as they arrive
client.remoteChanges.listen((change) {
  print('${change.operation} on ${change.table}: ${change.rowId}');
});

// Auto-sync events (stale table detection + progress)
client.autoSyncEvents.listen((event) {
  print('Auto-sync ${event.state}: ${event.tableNames}');
});

// Merkle verification results
client.merkleVerificationEvents.listen((event) {
  if (event.hadMismatches) {
    print('Repaired: ${event.repairedTables}');
    // Invalidate caches, refresh UI
  }
});
```

### Readiness

```dart
// Check if client is fully ready (connected + all channels synced)
if (client.isReady) {
  // Safe to read from local database
}

// Wait for all channels to complete initial sync
await client.waitForAutoSyncComplete();
```

## Schema migrations

The server pushes SQLite DDL migrations when the client's schema version is behind. The client applies them automatically during `syncUnified()` and reports the result in `SyncCompleteEvent.schemaUpgraded`.

No client-side migration code is needed — the server is the single source of truth for schema.

## Merkle tree verification

When `merkleVerifyInterval` is set, the client periodically builds SHA256-based Merkle trees from stored `row_hash` values and compares them with the server. If blocks differ, only the mismatched rows are fetched and repaired.

The server is the single source of truth for `row_hash` — computed by a Postgres trigger at write time. Clients receive and store these values during sync, snapshots, ACK responses, and merkle repair. No local hash computation is needed for sync to work.

For optional client-side data integrity (e.g., detecting local corruption), the `synclib_hash` library is available on all platforms but is not required.

```dart
SyncClientConfig(
  // ...
  merkleVerifyInterval: Duration(hours: 1),
  merkleSkipColumns: ['row_hash'],  // columns to exclude from hash
  channels: [
    SyncChannel(
      topic: 'sync:tribe:tribe-456',
      role: ChannelRole.pull,
      tables: [
        SyncTable('exercises', hashColumns: ['last_modified_ms']),
      ],
    ),
  ],
)
```

`hashColumns` lets you hash only specific columns (e.g., a timestamp updated on every write) instead of the full row, which is faster for tables with large JSONB documents.

## Message codecs

Two codecs are available for the WebSocket wire format:

```dart
SyncClientConfig(
  // ...
  codec: SyncCodecType.json,         // default — human-readable
  // codec: SyncCodecType.messagepack, // more compact, faster
)
```

## API reference

### SyncClient

| Method | Description |
|--------|-------------|
| `initialize()` | Open database, set up WebSocket listeners |
| `connect(token:)` | Connect to server with JWT auth |
| `disconnect()` | Close the WebSocket connection |
| `syncUnified()` | Run a full push + pull sync cycle |
| `streamSnapshot(tables)` | Stream table data from server |
| `fetchRow(table, rowId)` | Fetch a single row from server |
| `sendMessage(event, payload)` | Send a custom message to server |
| `waitForAutoSyncComplete()` | Wait for all channels to finish initial sync |
| `dispose()` | Clean up all resources |

### SyncClientConfig

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dbPath` | `String` | required | Path to local SQLite database |
| `serverUrl` | `String` | required | WebSocket server URL |
| `clientId` | `String` | required | Unique client identifier |
| `channels` | `List<SyncChannel>` | required | Channels to sync on |
| `database` | `SynclibDatabase?` | `null` | Pre-opened database instance |
| `codec` | `SyncCodecType` | `json` | Wire format codec |
| `syncOnWrite` | `bool` | `false` | Auto-push on local writes |
| `syncOnWriteDebounce` | `Duration` | `100ms` | Debounce for syncOnWrite |
| `autoSyncOnConnect` | `bool` | `true` | Sync stale tables on connect |
| `periodicSyncInterval` | `Duration?` | `null` | Background sync interval |
| `merkleVerifyInterval` | `Duration?` | `null` | Merkle verification interval |
| `merkleSkipColumns` | `List<String>` | `['row_hash']` | Columns to exclude from hash |
| `pushBatchSize` | `int` | `100` | Max changes per push batch |
| `onConflict` | `ConflictResolver?` | `null` | Custom conflict resolution |
| `metadata` | `Map?` | `null` | Extra data sent in hello |

## Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  synclib_sync:
    git:
      url: git@github.com:synclib-io/synclib-client-flutter.git
      ref: main
```

## License

MIT
