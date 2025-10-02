import 'dart:async';
import 'package:flutter/material.dart';
import 'package:synclib_sync/synclib_sync.dart' as sync;
import 'package:synclib_flutter/synclib_flutter.dart';
import 'package:path_provider/path_provider.dart';
import 'package:logging/logging.dart';
import 'package:sqlite3/sqlite3.dart' as sqlite;

void main() {
  // Enable logging
  Logger.root.level = Level.ALL;
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
  });

  runApp(const MyApp());
}

class MyApp extends StatefulWidget {
  const MyApp({super.key});

  @override
  State<MyApp> createState() => _MyAppState();
}

class _MyAppState extends State<MyApp> {
  sync.SyncClient? _syncClient;
  String _status = 'Not initialized';
  sync.ConnectionState _connectionState = sync.ConnectionState.disconnected;
  List<Change> _changes = [];
  List<Map<String, dynamic>> _users = [];
  StreamSubscription<sync.ChangeMessage>? _remoteChangeSubscription;

  @override
  void initState() {
    super.initState();
    _initializeSync();
  }

  Future<void> _initializeSync() async {
    try {
      // Get application documents directory
      final directory = await getApplicationDocumentsDirectory();
      final dbPath = '${directory.path}/sync_example.db';

      setState(() => _status = 'Initializing sync client...');

      // Create sync client configuration
      final config = sync.SyncClientConfig(
        dbPath: dbPath,
        serverUrl: 'ws://localhost:4000/socket/websocket', // Your Elixir server
        clientId: 'flutter-client-123',
        codec: sync.SyncCodecType.messagepack, // Or sync.SyncCodecType.json
        pushInterval: const Duration(seconds: 5),
        pullInterval: null, // Reactive mode - server pushes changes
        onConflict: _resolveConflict,
        metadata: {
          'device': 'mobile',
          'platform': 'flutter',
        },
      );

      _syncClient = sync.SyncClient(config);

      // Initialize
      await _syncClient!.initialize();

      // Get database instance and set up schema
      final db = await SynclibDatabase.open(dbPath);
      await db.exec('''
        CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT,
          updated_at INTEGER
        )
      ''');
      await db.close();

      // Listen to connection state changes
      _syncClient!.stateChanges.listen((state) {
        setState(() {
          _connectionState = state;
          _status = 'Connection: ${state.name}';
        });
      });

      // Listen to remote changes and auto-refresh UI
      _remoteChangeSubscription = _syncClient!.remoteChanges.listen((change) {
        print('Remote change received: ${change.operation} on ${change.table}');
        setState(() {
          _status = 'Received server change: ${change.operation} on ${change.table}';
        });
        _refreshAll();
      });

      setState(() => _status = 'Initialized. Ready to connect.');
    } catch (e) {
      setState(() => _status = 'Error: $e');
    }
  }

  Future<void> _connect() async {
    if (_syncClient == null) return;

    try {
      setState(() => _status = 'Connecting...');
      await _syncClient!.connect();
      setState(() => _status = 'Connected!');
    } catch (e) {
      setState(() => _status = 'Connection error: $e');
    }
  }

  Future<void> _disconnect() async {
    if (_syncClient == null) return;

    try {
      await _syncClient!.disconnect();
      setState(() => _status = 'Disconnected');
    } catch (e) {
      setState(() => _status = 'Disconnect error: $e');
    }
  }

  Future<void> _insertUser() async {
    if (_syncClient == null) return;

    try {
      final directory = await getApplicationDocumentsDirectory();
      final dbPath = '${directory.path}/sync_example.db';
      final db = await SynclibDatabase.open(dbPath);

      final id = DateTime.now().millisecondsSinceEpoch.toString();
      final name = 'User $id';
      final email = 'user$id@example.com';
      final timestamp = DateTime.now().millisecondsSinceEpoch;

      await db.write(
        tableName: 'users',
        rowId: id,
        operation: SynclibOperation.insert,
        sql: "INSERT INTO users (id, name, email, updated_at) VALUES ('$id', '$name', '$email', $timestamp)",
        data: '{"id":"$id","name":"$name","email":"$email","updated_at":$timestamp}',
      );

      await db.close();
      setState(() => _status = 'Inserted user: $name (will sync automatically)');
      await _loadChanges();
    } catch (e) {
      setState(() => _status = 'Insert error: $e');
    }
  }

  Future<void> _syncNow() async {
    if (_syncClient == null) return;

    try {
      setState(() => _status = 'Syncing...');
      await _syncClient!.sync();
      setState(() => _status = 'Sync complete');
      await _loadChanges();
    } catch (e) {
      setState(() => _status = 'Sync error: $e');
    }
  }

  Future<void> _loadChanges() async {
    try {
      final directory = await getApplicationDocumentsDirectory();
      final dbPath = '${directory.path}/sync_example.db';
      final db = await SynclibDatabase.open(dbPath);

      final changes = await db.getPendingChanges(limit: 20);
      await db.close();

      setState(() {
        _changes = changes;
      });
    } catch (e) {
      setState(() => _status = 'Load changes error: $e');
    }
  }

  Future<void> _loadUsers() async {
    try {
      final directory = await getApplicationDocumentsDirectory();
      final dbPath = '${directory.path}/sync_example.db';

      // Use sqlite3 package to query
      final db = sqlite.sqlite3.open(dbPath);
      final result = db.select('SELECT id, name, email, updated_at FROM users ORDER BY updated_at DESC');
      db.dispose();

      setState(() {
        _users = result.map((row) => {
          'id': row['id'],
          'name': row['name'],
          'email': row['email'],
          'updated_at': row['updated_at'],
        }).toList();
      });
    } catch (e) {
      setState(() => _status = 'Load users error: $e');
    }
  }

  Future<void> _refreshAll() async {
    await _loadChanges();
    await _loadUsers();
  }

  /// Example conflict resolution: last-write-wins based on timestamp
  Future<sync.ChangeMessage?> _resolveConflict(
    sync.ChangeMessage local,
    sync.ChangeMessage remote,
  ) async {
    print('Conflict detected: ${local.table}:${local.rowId}');

    // Compare timestamps
    final localTime = local.data?['updated_at'] as int? ?? 0;
    final remoteTime = remote.data?['updated_at'] as int? ?? 0;

    if (remoteTime > localTime) {
      print('Choosing remote (newer)');
      return remote;
    } else {
      print('Choosing local (newer)');
      return local;
    }
  }

  @override
  void dispose() {
    _remoteChangeSubscription?.cancel();
    _syncClient?.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(
          title: const Text('Synclib Sync Example'),
          backgroundColor: _connectionStateColor,
        ),
        body: Padding(
          padding: const EdgeInsets.all(16.0),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              Text(
                'Status: $_status',
                style: Theme.of(context).textTheme.titleMedium,
              ),
              const SizedBox(height: 10),
              Text(
                'Connection: ${_connectionState.name}',
                style: TextStyle(
                  color: _connectionStateColor,
                  fontWeight: FontWeight.bold,
                ),
              ),
              const SizedBox(height: 20),
              Row(
                children: [
                  Expanded(
                    child: ElevatedButton(
                      onPressed: _connectionState == sync.ConnectionState.disconnected
                        ? _connect
                        : null,
                      child: const Text('Connect'),
                    ),
                  ),
                  const SizedBox(width: 10),
                  Expanded(
                    child: ElevatedButton(
                      onPressed: _connectionState == sync.ConnectionState.connected
                        ? _disconnect
                        : null,
                      child: const Text('Disconnect'),
                    ),
                  ),
                ],
              ),
              const SizedBox(height: 10),
              ElevatedButton(
                onPressed: _insertUser,
                child: const Text('Insert User (Local)'),
              ),
              const SizedBox(height: 10),
              ElevatedButton(
                onPressed: _connectionState == sync.ConnectionState.connected
                  ? _syncNow
                  : null,
                child: const Text('Sync Now'),
              ),
              const SizedBox(height: 10),
              ElevatedButton(
                onPressed: _refreshAll,
                child: const Text('Refresh All Data'),
              ),
              const SizedBox(height: 20),
              Text(
                'Users (${_users.length}):',
                style: Theme.of(context).textTheme.titleMedium,
              ),
              const SizedBox(height: 10),
              Expanded(
                child: _users.isEmpty
                  ? const Center(child: Text('No users yet. Insert one or wait for server changes!'))
                  : ListView.builder(
                      itemCount: _users.length,
                      itemBuilder: (context, index) {
                        final user = _users[index];
                        DateTime? timestamp;
                        if (user['updated_at'] != null) {
                          final ts = user['updated_at'];
                          if (ts is int) {
                            timestamp = DateTime.fromMillisecondsSinceEpoch(ts);
                          } else if (ts is String) {
                            try {
                              timestamp = DateTime.parse(ts);
                            } catch (e) {
                              // Invalid timestamp format
                            }
                          }
                        }
                        return Card(
                          child: ListTile(
                            leading: const CircleAvatar(
                              child: Icon(Icons.person),
                            ),
                            title: Text(user['name'] ?? 'Unknown'),
                            subtitle: Text(
                              '${user['email'] ?? 'No email'}\n'
                              'ID: ${user['id']}\n'
                              'Updated: ${timestamp?.toString() ?? 'Unknown'}',
                            ),
                            isThreeLine: true,
                          ),
                        );
                      },
                    ),
              ),
              const SizedBox(height: 10),
              Text(
                'Pending Changes: ${_changes.length}',
                style: Theme.of(context).textTheme.bodySmall,
              ),
            ],
          ),
        ),
      ),
    );
  }

  Color get _connectionStateColor {
    switch (_connectionState) {
      case sync.ConnectionState.connected:
        return Colors.green;
      case sync.ConnectionState.connecting:
      case sync.ConnectionState.reconnecting:
        return Colors.orange;
      case sync.ConnectionState.disconnected:
        return Colors.grey;
      case sync.ConnectionState.failed:
        return Colors.red;
    }
  }

  IconData _operationIcon(SynclibOperation op) {
    switch (op) {
      case SynclibOperation.insert:
        return Icons.add;
      case SynclibOperation.update:
        return Icons.edit;
      case SynclibOperation.delete:
        return Icons.delete;
    }
  }
}
