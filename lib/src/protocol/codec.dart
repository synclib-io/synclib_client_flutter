import 'dart:convert';
import 'dart:typed_data';
import 'package:msgpack_dart/msgpack_dart.dart';
import 'message.dart';

/// Codec for encoding/decoding sync messages
abstract class SyncCodec {
  /// Encode a message to bytes
  Uint8List encode(SyncMessage message);

  /// Decode bytes to a message
  SyncMessage decode(Uint8List bytes);
}

/// JSON codec implementation
class JsonSyncCodec implements SyncCodec {
  const JsonSyncCodec();

  @override
  Uint8List encode(SyncMessage message) {
    final map = message.toMap();
    final jsonString = json.encode(map);
    return Uint8List.fromList(utf8.encode(jsonString));
  }

  @override
  SyncMessage decode(Uint8List bytes) {
    final jsonString = utf8.decode(bytes);
    final map = json.decode(jsonString) as Map<String, dynamic>;
    return SyncMessage.fromMap(map);
  }
}

/// MessagePack codec implementation (more efficient than JSON)
class MessagePackSyncCodec implements SyncCodec {
  const MessagePackSyncCodec();

  @override
  Uint8List encode(SyncMessage message) {
    final map = message.toMap();
    final bytes = serialize(map);
    return Uint8List.fromList(bytes);
  }

  @override
  SyncMessage decode(Uint8List bytes) {
    final decoded = deserialize(bytes);
    final map = Map<String, dynamic>.from(decoded as Map);
    return SyncMessage.fromMap(map);
  }
}

/// Factory for creating codecs
class SyncCodecFactory {
  static SyncCodec create(SyncCodecType type) {
    switch (type) {
      case SyncCodecType.json:
        return const JsonSyncCodec();
      case SyncCodecType.messagepack:
        return const MessagePackSyncCodec();
    }
  }
}

/// Available codec types
enum SyncCodecType {
  json,
  messagepack,
}
