import 'dart:async';
import 'package:phoenix_socket/phoenix_socket.dart';
import 'package:logging/logging.dart';
import '../protocol/message.dart';
import '../protocol/codec.dart';
import 'package:collection/collection.dart' show IterableExtension;

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
  final Map<String, PhoenixChannel> _channels = {};
  ConnectionState _state = ConnectionState.disconnected;
  StreamController<SyncMessage>? _messageController;
  StreamController<ConnectionState>? _stateController;
  int _reconnectAttempts = 0;
  Map<String, dynamic>? _connectionParams;

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
  Future<void> connect({Map<String, dynamic>? params}) async {
    if (_state == ConnectionState.connected || _state == ConnectionState.connecting) {
      _logger.info('Already connected or connecting');
      return;
    }

    // Store params for reconnection
    if (params != null) {
      _connectionParams = params;
    }

    _updateState(ConnectionState.connecting);
    _logger.info('Connecting to $url');

    try {
      // Create Phoenix socket
      // Convert params to Map<String, String> as required by PhoenixSocketOptions
      final stringParams = _connectionParams?.map(
        (key, value) => MapEntry(key, value.toString()),
      ) ?? <String, String>{};

      _socket = PhoenixSocket(
        url,
        socketOptions: PhoenixSocketOptions(
          timeout: const Duration(seconds: 10),
          params: stringParams,
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
      // Leave existing channel with this topic if present
      if (_channels.containsKey(topic)) {
        _logger.info('Channel $topic already exists, leaving old one');
        _channels[topic]?.leave();
        _channels.remove(topic);
      }

      final channel = _socket!.addChannel(topic: topic, parameters: params);

      // Listen for messages
      channel.messages.listen((message) {
        _onMessage(message);
      });

      // Join the channel
      final pushResponse = await channel.join().future;

      if (pushResponse.isOk) {
        _channels[topic] = channel;
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

  /// Check if a channel is currently joined
  bool isChannelJoined(String topic) {
    return _channels.containsKey(topic);
  }

  /// Get all currently joined channel topics
  List<String> get joinedChannels => _channels.keys.toList();

  /// Leave a specific channel by topic
  Future<void> leaveChannel(String topic) async {
    final channel = _channels[topic];
    if (channel == null) {
      _logger.warning('Channel $topic not found');
      return;
    }

    _logger.info('Leaving channel: $topic');
    await channel.leave().future;
    _channels.remove(topic);
    _logger.info('Left channel: $topic');
  }

  /// Disconnect from WebSocket server
  Future<void> disconnect() async {
    _logger.info('Disconnecting');

    // Leave all channels
    for (final channel in _channels.values) {
      channel.leave();
    }
    _channels.clear();

    _socket?.dispose();
    _socket = null;
    _updateState(ConnectionState.disconnected);
  }

  /// Send a message to the server
  /// Sends to the first channel (user channel) by default
  Future<void> send(SyncMessage message, {String? channelTopic}) async {
    if (!isConnected || _channels.isEmpty) {
      throw StateError('Not connected to channel');
    }

    try {
      final map = message.toMap();
      final event = map['type'] as String;
      map.remove('type');

      _logger.fine('Sending event: $event with payload: $map');

      PhoenixChannel? channel = _channels.values.first;
      if (channelTopic != null) {
        final existingChannelKey = _channels.keys.firstWhereOrNull((k) =>
            k.contains(channelTopic));
        channel = existingChannelKey != null
            ? _channels[existingChannelKey]
            : _channels.values.first;
      }

      final push = channel!.push(event, map);
      await push.future;

      _logger.fine('Sent message: $event');
    } catch (e) {
      _logger.severe('Failed to send message: $e');
      rethrow;
    }
  }

  /// Send a raw message with custom event and payload, returns server response
  Future<Map<String, dynamic>> sendRaw(
    String event,
    Map<String, dynamic> payload,
    {String? channelTopic}
  ) async {
    if (!isConnected || _channels.isEmpty) {
      throw StateError('Not connected to channel');
    }

    try {
      _logger.fine('Sending raw event: $event with payload: $payload');

      PhoenixChannel? channel = _channels.values.first;
      if (channelTopic != null) {
        final existingChannelKey = _channels.keys.firstWhereOrNull((k) =>
            k.contains(channelTopic));
        channel = existingChannelKey != null
            ? _channels[existingChannelKey]
            : _channels.values.first;
      }

      final push = channel!.push(event, payload);
      final response = await push.future;

      _logger.fine('Sent on channel ${channelTopic ?? 'default'} raw message: $event, received response');

      // Phoenix response structure: {status: "ok", response: {...}}
      if (response.isOk) {
        return Map<String, dynamic>.from(response.response as Map);
      } else {
        throw Exception('Server error: ${response.response}');
      }
    } catch (e) {
      _logger.severe('Failed to send raw message: $e');
      rethrow;
    }
  }

  /// Handle incoming message
  void _onMessage(Message message) {
    try {
      final eventType = message.event?.value ?? 'unknown';

      _logger.info('Received Phoenix message: event=$eventType, payload=${message.payload}');

      // Skip Phoenix system events (but not channel replies)
      if (eventType.startsWith('phx_') && eventType != 'phx_reply') {
        _logger.fine('Skipping Phoenix system event: $eventType');
        return;
      }

      // Handle channel reply messages (chan_reply_N)
      if (eventType.startsWith('chan_reply_')) {
        _handleChannelReply(message);
        return;
      }

      // Convert Phoenix message to SyncMessage
      final payload = Map<String, dynamic>.from(message.payload as Map<String, dynamic>? ?? {});

      // Add type to payload for SyncMessage decoding
      payload['type'] = eventType;

      final syncMessage = SyncMessage.fromMap(payload);
      _messageController!.add(syncMessage);
      _logger.info('Successfully decoded $eventType message and added to stream');
    } catch (e, stack) {
      _logger.severe('Failed to process message: $e', e, stack);
    }
  }

  /// Handle channel reply messages
  void _handleChannelReply(Message message) {
    try {
      final payload = message.payload != null
          ? Map<String, dynamic>.from(message.payload as Map)
          : <String, dynamic>{};

      // Extract the response from the Phoenix reply structure
      final responseData = payload['response'];
      final response = responseData != null
          ? Map<String, dynamic>.from(responseData as Map)
          : <String, dynamic>{};

      _logger.fine('Channel reply response: $response');

      // Create a PhoenixReplyMessage with the response
      final replyPayload = Map<String, dynamic>.from(response);
      replyPayload['type'] = 'phx_reply';

      final syncMessage = SyncMessage.fromMap(replyPayload);
      _messageController!.add(syncMessage);
      _logger.info('Successfully processed channel reply');
    } catch (e, stack) {
      _logger.severe('Failed to process channel reply: $e', e, stack);
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
