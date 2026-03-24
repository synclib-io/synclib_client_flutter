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
  authFailed,
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
  DateTime? _connectStartTime;
  bool _intentionalDisconnect = false;
  int _quickFailureCount = 0; // Track consecutive quick failures for auth detection
  static const int _quickFailureThreshold = 1; // Trigger authFailed after 1 quick failure to refresh token faster

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

    // Reset intentional disconnect flag when connecting
    _intentionalDisconnect = false;

    // Store params for reconnection
    if (params != null) {
      _connectionParams = params;
    }

    // Clean up any existing socket and channels before reconnecting
    // This ensures we don't have stale channel objects with _joinedOnce = true
    if (_socket != null) {
      _logger.info('Cleaning up existing socket before reconnect');
      for (final channel in _channels.values) {
        try {
          channel.leave();
        } catch (e) {
          _logger.fine('Error leaving channel during cleanup: $e');
        }
      }
      _channels.clear();
      _socket?.dispose();
      _socket = null;
    }

    _updateState(ConnectionState.connecting);
    _connectStartTime = DateTime.now();
    _logger.info('Connecting to $url');

    // Completer to wait for actual connection (not just initiation)
    final connectionCompleter = Completer<void>();

    try {
      // Create Phoenix socket
      // Convert params to Map<String, String> as required by PhoenixSocketOptions
      final stringParams = _connectionParams?.map(
        (key, value) => MapEntry(key, value.toString()),
      ) ?? <String, String>{};

      _socket = PhoenixSocket(
        url,
        socketOptions: PhoenixSocketOptions(
          timeout: const Duration(seconds: 30),
          params: stringParams,
        ),
      );

      // Set up listeners before connecting
      _socket!.closeStream.listen((event) {
        _logger.warning('Socket closed');
        _onDone();
        // Complete with error if we're still waiting for connection
        if (!connectionCompleter.isCompleted) {
          connectionCompleter.completeError(StateError('Socket closed before connection established'));
        }
      });

      _socket!.errorStream.listen((error) {
        _logger.severe('Socket error: $error');
        _onError(error);
        // Complete with error if we're still waiting for connection
        if (!connectionCompleter.isCompleted) {
          connectionCompleter.completeError(error);
        }
      });

      _socket!.openStream.listen((event) {
        _logger.info('Socket opened');
        _updateState(ConnectionState.connected);
        _reconnectAttempts = 0;
        _quickFailureCount = 0; // Reset quick failure counter on successful connection
        // Signal that connection is ready
        if (!connectionCompleter.isCompleted) {
          connectionCompleter.complete();
        }
      });

      // Connect socket
      _logger.info('Connecting to $url');
      await _socket!.connect();

      // Wait for the socket to actually open (or fail)
      await connectionCompleter.future.timeout(
        const Duration(seconds: 15),
        onTimeout: () {
          throw TimeoutException('Connection timed out waiting for socket to open');
        },
      );

      _logger.info('Connect call completed - socket is open');

    } catch (e) {
      _logger.severe('Connection failed: $e');
      _updateState(ConnectionState.failed);
      _scheduleReconnect();
      rethrow; // Re-throw so caller knows connection failed
    }
  }

  /// Join a Phoenix channel
  /// Returns the server's join response (e.g., status, stale_tables, etc.)
  Future<Map<String, dynamic>> joinChannel(String topic, Map<String, dynamic> params) async {
    if (!isConnected) {
      throw StateError('Not connected to server');
    }

    try {
      // Leave existing channel with this topic if present in our map
      if (_channels.containsKey(topic)) {
        _logger.info('Channel $topic already exists in our map, leaving old one');
        _channels[topic]?.leave();
        _channels.remove(topic);
      }

      // Also check if the socket has this channel cached and remove it
      // This prevents the "_joinedOnce" assertion error when reconnecting
      final existingChannel = _socket!.channels[topic];
      if (existingChannel != null) {
        _logger.info('Channel $topic exists in socket cache, removing it');
        existingChannel.leave();
        _socket!.removeChannel(existingChannel);
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
        // Return the server's response data
        final response = pushResponse.response;
        if (response is Map<String, dynamic>) {
          return response;
        }
        return {'status': 'connected'};
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

    // Mark as intentional to prevent auto-reconnect
    _intentionalDisconnect = true;

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
            k == channelTopic);
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
            k == channelTopic);
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

      // Preserve the inner type (e.g., "viewers_list", "online_count") before overwriting
      // This is used by presence and feed_status messages to distinguish event subtypes
      if (payload['type'] != null) {
        payload['inner_type'] = payload['type'];
      }

      // Add type to payload for SyncMessage decoding (this is the Phoenix event type)
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

    // If error occurs very quickly after connection attempt (< 2 seconds),
    // it's likely an auth failure (token expired/invalid)
    final timeSinceConnect = _connectStartTime != null
        ? DateTime.now().difference(_connectStartTime!).inMilliseconds
        : 999999;
    if (timeSinceConnect < 2000 && _state == ConnectionState.connecting) {
      _logger.warning('Connection rejected quickly - likely auth failure (token expired?)');
      _updateState(ConnectionState.authFailed);
    } else {
      _updateState(ConnectionState.failed);
    }
  }

  /// Handle WebSocket close
  void _onDone() {
    _logger.warning('WebSocket connection closed');

    // Don't auto-reconnect on auth failures - client needs to refresh token first
    if (_state == ConnectionState.authFailed) {
      _logger.warning('Not reconnecting due to auth failure - token refresh required');
      return;
    }

    // Track quick failures - if connection closes quickly after connect attempt,
    // it's likely an auth rejection (expired token)
    final timeSinceConnect = _connectStartTime != null
        ? DateTime.now().difference(_connectStartTime!).inMilliseconds
        : 999999;

    if (timeSinceConnect < 2000) {
      _quickFailureCount++;
      _logger.warning('Quick connection failure detected ($_quickFailureCount/$_quickFailureThreshold)');

      if (_quickFailureCount >= _quickFailureThreshold) {
        _logger.warning('Multiple quick failures - likely auth failure (token expired?)');
        _updateState(ConnectionState.authFailed);
        return;
      }
    } else {
      // Reset counter on successful connection that lasted > 2s
      _quickFailureCount = 0;
    }

    _updateState(ConnectionState.disconnected);
    _scheduleReconnect();
  }

  /// Schedule automatic reconnection
  void _scheduleReconnect() {
    // Don't reconnect if disconnect was intentional
    if (_intentionalDisconnect) {
      _logger.info('Skipping reconnect - disconnect was intentional');
      return;
    }

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
