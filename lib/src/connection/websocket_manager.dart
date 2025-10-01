import 'dart:async';
import 'package:phoenix_socket/phoenix_socket.dart';
import 'package:logging/logging.dart';
import '../protocol/message.dart';
import '../protocol/codec.dart';

/// Connection state
enum ConnectionState {
  disconnected,
  connecting,
  connected,
  reconnecting,
  failed,
}

/// WebSocket connection manager using Phoenix Channels
class WebSocketManager {
  final String url;
  final SyncCodec codec;
  final Duration reconnectDelay;
  final int maxReconnectAttempts;
  final Logger _logger = Logger('WebSocketManager');

  PhoenixSocket? _socket;
  PhoenixChannel? _channel;
  ConnectionState _state = ConnectionState.disconnected;
  StreamController<SyncMessage>? _messageController;
  StreamController<ConnectionState>? _stateController;
  int _reconnectAttempts = 0;

  WebSocketManager({
    required this.url,
    required this.codec,
    this.reconnectDelay = const Duration(seconds: 2),
    this.maxReconnectAttempts = 5,
  }) {
    _messageController = StreamController<SyncMessage>.broadcast();
    _stateController = StreamController<ConnectionState>.broadcast();
  }

  /// Stream of incoming messages
  Stream<SyncMessage> get messages => _messageController!.stream;

  /// Stream of connection state changes
  Stream<ConnectionState> get stateChanges => _stateController!.stream;

  /// Current connection state
  ConnectionState get state => _state;

  /// Whether currently connected
  bool get isConnected => _state == ConnectionState.connected;

  /// Connect to WebSocket server
  Future<void> connect() async {
    if (_state == ConnectionState.connected || _state == ConnectionState.connecting) {
      _logger.info('Already connected or connecting');
      return;
    }

    _updateState(ConnectionState.connecting);
    _logger.info('Connecting to $url');

    try {
      // Create Phoenix socket
      _socket = PhoenixSocket(
        url,
        socketOptions: PhoenixSocketOptions(
          timeout: const Duration(seconds: 10),
        ),
      );

      // Set up listeners before connecting
      _socket!.closeStream.listen((event) {
        _logger.warning('Socket closed');
        _onDone();
      });

      _socket!.errorStream.listen((error) {
        _logger.severe('Socket error: $error');
        _onError(error);
      });

      _socket!.openStream.listen((event) {
        _logger.info('Socket opened');
        _updateState(ConnectionState.connected);
        _reconnectAttempts = 0;
      });

      // Connect socket
      _logger.info('Connecting to $url');
      await _socket!.connect();

      _logger.info('Connect call completed');

    } catch (e) {
      _logger.severe('Connection failed: $e');
      _updateState(ConnectionState.failed);
      _scheduleReconnect();
    }
  }

  /// Join a Phoenix channel
  Future<void> joinChannel(String topic, Map<String, dynamic> params) async {
    if (!isConnected) {
      throw StateError('Not connected to server');
    }

    try {
      _channel = _socket!.addChannel(topic: topic, parameters: params);

      // Listen for messages
      _channel!.messages.listen((message) {
        _onMessage(message);
      });

      // Join the channel
      final pushResponse = await _channel!.join().future;

      if (pushResponse.isOk) {
        _logger.info('Joined channel: $topic');
      } else {
        _logger.severe('Failed to join channel: ${pushResponse.response}');
        throw Exception('Failed to join channel: ${pushResponse.response}');
      }
    } catch (e) {
      _logger.severe('Failed to join channel: $e');
      rethrow;
    }
  }

  /// Disconnect from WebSocket server
  Future<void> disconnect() async {
    _logger.info('Disconnecting');
    _channel?.leave();
    _channel = null;
    _socket?.dispose();
    _socket = null;
    _updateState(ConnectionState.disconnected);
  }

  /// Send a message to the server
  Future<void> send(SyncMessage message) async {
    if (!isConnected || _channel == null) {
      throw StateError('Not connected to channel');
    }

    try {
      final map = message.toMap();
      final event = map['type'] as String;
      map.remove('type');

      _logger.fine('Sending event: $event with payload: $map');

      final push = _channel!.push(event, map);
      await push.future;

      _logger.fine('Sent message: $event');
    } catch (e) {
      _logger.severe('Failed to send message: $e');
      rethrow;
    }
  }

  /// Handle incoming message
  void _onMessage(Message message) {
    try {
      _logger.fine('Received Phoenix message: event=${message.event}, payload=${message.payload}');

      // Convert Phoenix message to SyncMessage
      final payload = message.payload as Map<String, dynamic>? ?? {};
      final eventType = message.event?.value ?? 'unknown';

      // Add type to payload for SyncMessage decoding
      payload['type'] = eventType;

      final syncMessage = SyncMessage.fromMap(payload);
      _messageController!.add(syncMessage);
    } catch (e, stack) {
      _logger.severe('Failed to process message: $e', e, stack);
    }
  }

  /// Handle WebSocket error
  void _onError(error) {
    _logger.severe('WebSocket error: $error');
    _updateState(ConnectionState.failed);
  }

  /// Handle WebSocket close
  void _onDone() {
    _logger.warning('WebSocket connection closed');
    _updateState(ConnectionState.disconnected);
    _scheduleReconnect();
  }

  /// Schedule automatic reconnection
  void _scheduleReconnect() {
    if (_reconnectAttempts >= maxReconnectAttempts) {
      _logger.severe('Max reconnect attempts reached');
      _updateState(ConnectionState.failed);
      return;
    }

    _reconnectAttempts++;
    _updateState(ConnectionState.reconnecting);

    final delay = reconnectDelay * _reconnectAttempts;
    _logger.info('Reconnecting in ${delay.inSeconds}s (attempt $_reconnectAttempts/$maxReconnectAttempts)');

    Future.delayed(delay, () {
      connect();
    });
  }

  /// Update connection state and notify listeners
  void _updateState(ConnectionState newState) {
    if (_state == newState) return;
    _state = newState;
    _stateController!.add(newState);
  }

  /// Dispose resources
  Future<void> dispose() async {
    await disconnect();
    await _messageController?.close();
    await _stateController?.close();
    _messageController = null;
    _stateController = null;
  }
}
