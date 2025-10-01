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
