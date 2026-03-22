# synclib_sync

High-level sync package for synclib - WebSocket-based bidirectional synchronization with Elixir/Phoenix backends.

## Features

- 🔄 **Bidirectional Sync**: Automatic push of local changes and pull of remote changes
- 🔌 **WebSocket Connection**: Real-time sync over WebSocket with automatic reconnection
- 📦 **Multiple Codecs**: Support for JSON and MessagePack encoding
- ⚡ **Batch Operations**: Efficient batching of changes for network transfer
- 🔀 **Conflict Resolution**: Pluggable conflict resolution strategies
- 📡 **Reactive & Periodic**: Choose between server-push or polling modes
- 🛡️ **Security**: Uses structured messages (not raw SQL) for safe syncing

## Architecture

```
┌─────────────────┐         WebSocket          ┌──────────────────┐
│  Flutter App    │◄───────────────────────────►│  Elixir/Phoenix  │
│                 │                              │     Server       │
│  ┌───────────┐  │   ChangeMessage/Batch       │                  │
│  │ SyncClient│──┼─────────────────────────────┤  Phoenix Channel │
│  └─────┬─────┘  │   AckMessage/Error          │                  │
│        │        │                              └──────────────────┘
│  ┌─────▼─────┐  │
│  │  Synclib  │  │
│  │  Database │  │
│  └───────────┘  │
└─────────────────┘
```

## Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  synclib_sync:
    path: ../synclib_sync
```

## Usage

### Basic Setup

```dart
import 'package:synclib_sync/synclib_sync.dart';

// Configure sync client
final config = SyncClientConfig(
  dbPath: '/path/to/database.db',
  serverUrl: 'ws://localhost:4000/socket/websocket',
  clientId: 'unique-client-id',
  codec: SyncCodecType.messagepack,
  pushInterval: const Duration(seconds: 5), // Auto-push every 5s
  pullInterval: null, // Server pushes changes (reactive)
  onConflict: resolveConflict,
);

// Create and initialize client
final syncClient = SyncClient(config);
await syncClient.initialize();

// Connect to server
await syncClient.connect();

// Client automatically syncs in background!
```

### Conflict Resolution

```dart
Future<ChangeMessage?> resolveConflict(
  ChangeMessage local,
  ChangeMessage remote,
) async {
  // Last-write-wins based on timestamp
  final localTime = local.data?['updated_at'] as int? ?? 0;
  final remoteTime = remote.data?['updated_at'] as int? ?? 0;

  return remoteTime > localTime ? remote : local;
}
```

### Manual Sync

```dart
// Trigger sync manually
await syncClient.sync();
```

### Monitor Connection State

```dart
syncClient.stateChanges.listen((state) {
  print('Connection state: $state');
  // ConnectionState: disconnected, connecting, connected, reconnecting, failed
});
```

## Protocol Messages

### Client → Server

#### HelloMessage
```json
{
  "type": "hello",
  "client_id": "flutter-client-123",
  "last_seqnum": 42,
  "metadata": {"device": "mobile"}
}
```

#### ChangeMessage
```json
{
  "type": "change",
  "table": "users",
  "operation": "insert",
  "row_id": "123",
  "data": {"id": "123", "name": "Alice"},
  "seqnum": 42
}
```

#### ChangesBatchMessage
```json
{
  "type": "changes_batch",
  "changes": [/* ChangeMessages */],
  "from_seqnum": 40,
  "to_seqnum": 45
}
```

### Server → Client

#### AckMessage
```json
{
  "type": "ack",
  "seqnum": 42,
  "success": true
}
```

## Elixir/Phoenix Server Example

```elixir
defmodule MyAppWeb.SyncChannel do
  use Phoenix.Channel

  def join("sync:lobby", %{"client_id" => client_id}, socket) do
    {:ok, assign(socket, :client_id, client_id)}
  end

  def handle_in("hello", payload, socket) do
    # Send pending changes
    {:noreply, socket}
  end

  def handle_in("changes_batch", %{"changes" => changes}, socket) do
    Enum.each(changes, fn change ->
      apply_change(change)
      push(socket, "ack", %{seqnum: change["seqnum"], success: true})
      broadcast_from(socket, "change", change)
    end)
    {:noreply, socket}
  end
end
```

## Example

See [example/main.dart](example/main.dart) for a complete working example.



## LOGGING
The _logger in sync_client.dart uses Dart's logging package. To see the logs in your app, you need to configure a logger listener. Let me show you how: In your Flutter app (e.g., main.dart):
import 'package:logging/logging.dart';

void main() {
  // Configure logging - do this BEFORE initializing SyncClient
  Logger.root.level = Level.ALL; // Set minimum log level
  
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    
    // Or use developer.log for better formatting in Flutter
    // developer.log(
    //   record.message,
    //   time: record.time,
    //   level: record.level.value,
    //   name: record.loggerName,
    // );
  });
  
  runApp(MyApp());
}
Available log levels:
Logger.root.level = Level.ALL;      // See everything (FINEST to SHOUT)
Logger.root.level = Level.FINE;     // Debug and above
Logger.root.level = Level.INFO;     // Info and above (recommended)
Logger.root.level = Level.WARNING;  // Warnings and errors only
Logger.root.level = Level.SEVERE;   // Errors only
For better formatting in Flutter DevTools:
import 'dart:developer' as developer;
import 'package:logging/logging.dart';

void setupLogging() {
  Logger.root.level = Level.INFO;
  
  Logger.root.onRecord.listen((record) {
    developer.log(
      record.message,
      time: record.time,
      level: record.level.value,
      name: record.loggerName,
      error: record.error,
      stackTrace: record.stackTrace,
    );
  });
}
Filter by specific logger:
// Only show SyncClient logs
Logger.root.onRecord.listen((record) {
  if (record.loggerName == 'SyncClient' || record.loggerName.startsWith('Sync')) {
    print('[${record.level.name}] ${record.loggerName}: ${record.message}');
  }
});
Make sure you have the logging package:
# pubspec.yaml
dependencies:
  logging: ^1.2.0  # Add this if not already there
Once configured, you'll see all the _logger.info(), _logger.warning(), etc. messages from SyncClient in your console

## Integration Tests

There are 17 integration tests in `example/integration_test/` that verify the Dart sync client against the org server. They mirror the 17 Playwright tests in the JS test harness.

### Prerequisites

1. Start the org server with the test JWT secret:

```bash
cd /path/to/org/synclib_server
JWT_SECRET=synclib_test_secret mix phx.server
```

2. The server must be running on `localhost:4444`.

### Running Tests

Run each test file individually from the example directory (macOS desktop):

```bash
cd example
flutter test integration_test/general_sync_test.dart -d macos
flutter test integration_test/server_authoritative_hash_test.dart -d macos
flutter test integration_test/reconnect_sync_test.dart -d macos
flutter test integration_test/merkle_soft_delete_test.dart -d macos
```

**Note:** Running all files at once (`flutter test integration_test/ -d macos`) may fail due to a macOS Flutter integration test runner limitation where the app can't reliably restart between test files.

### Test Files

| File | Tests | What it covers |
|------|-------|----------------|
| `general_sync_test.dart` | 10 | Two-client sync, delete sync, update sync, concurrent adds, room scoping, reconnect persistence, delete/re-add, large batch (20 items), sync state transitions, pending changes cleared |
| `server_authoritative_hash_test.dart` | 5 | Push gets row_hash, pull gets row_hash, merkle verification (no spurious mismatches), no null hashes after full cycle, update changes hash |
| `reconnect_sync_test.dart` | 1 | Reconnect auto-syncs and receives items added while offline |
| `merkle_soft_delete_test.dart` | 1 | Soft-deleted items sync correctly, merkle verification stays stable (no spurious repairs) |

### Test Helpers

`helpers.dart` provides the test infrastructure:

- `createTestClient(userId, roomId)` — Opens a temp DB, creates a `SyncClient`, connects with an HS256 JWT, and syncs
- `reconnectClient(old)` — Creates a fresh `SyncClient` reusing the same DB file (simulates reconnection)
- `addItem / deleteItem / updateItem` — CRUD via `db.writeWithParams()` on the `items` table
- `getItems / getItemCount` — Query non-deleted items filtered by room_id
- `waitForItemCount(client, n)` — Polls until count >= n or timeout
- `getRowHashStats / getRowHashes` — Inspect server-computed `row_hash` values
- `resetServer()` — HTTP DELETE to `/api/test/items`