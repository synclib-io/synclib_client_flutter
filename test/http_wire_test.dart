import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:synclib_sync/synclib_sync.dart';

/// Host-runnable unit tests for the HTTP member sync WIRE logic (no server, no
/// native DB): URI construction, request method/headers/body, and response +
/// ack parsing. The DB apply path and the live round-trip are covered by the
/// integration test (integration_test/http_fallback_test.dart), which needs a
/// device + a running server.
SyncClientConfig _config({
  String serverUrl = 'ws://localhost:4242/socket/websocket',
  String clientId = 'app_test',
}) =>
    SyncClientConfig(
      dbPath: '/tmp/unused_wire_test.db',
      serverUrl: serverUrl,
      clientId: clientId,
      channels: const [],
      codec: SyncCodecType.json,
    );

void main() {
  group('memberSyncUri', () {
    test('maps ws://host:port/socket to http://host:port/api/sync/member/...', () {
      final client = SyncClient(_config());
      final uri = client.memberSyncUri('/storage_files', {'topic': 'sync:user:x', 'since': '0'});

      expect(uri.scheme, 'http');
      expect(uri.host, 'localhost');
      expect(uri.port, 4242);
      expect(uri.path, '/api/sync/member/storage_files');
      expect(uri.queryParameters['topic'], 'sync:user:x');
      expect(uri.queryParameters['since'], '0');
    });

    test('maps wss to https', () {
      final client = SyncClient(_config(serverUrl: 'wss://example.com/socket'));
      final uri = client.memberSyncUri('/push');
      expect(uri.scheme, 'https');
      expect(uri.host, 'example.com');
      expect(uri.path, '/api/sync/member/push');
    });
  });

  group('fetchPageRaw (pull wire)', () {
    test('sends a GET with auth + client-id headers and parses the page', () async {
      http.Request? captured;
      final mock = MockClient((req) async {
        captured = req;
        return http.Response(
          jsonEncode({
            'rows': [
              {'id': 'a', 'seqnum': 5, 'document': {'k': 'v'}},
              {'id': 'b', 'seqnum': 9, 'document': {}},
            ],
            'next_seqnum': 9,
            'has_more': true,
          }),
          200,
          headers: {'content-type': 'application/json'},
        );
      });

      final client = SyncClient(_config(), httpClient: mock);
      final page = await client.fetchPageRaw('storage_files',
          topic: 'sync:user:x', since: 3, limit: 50, token: 'tok-123');

      // request shape
      expect(captured!.method, 'GET');
      expect(captured!.url.path, '/api/sync/member/storage_files');
      expect(captured!.url.queryParameters['topic'], 'sync:user:x');
      expect(captured!.url.queryParameters['since'], '3');
      expect(captured!.url.queryParameters['limit'], '50');
      expect(captured!.headers['authorization'], 'Bearer tok-123');
      expect(captured!.headers['x-client-id'], 'app_test');

      // parsed page
      expect(page.rows.map((r) => r['id']).toList(), ['a', 'b']);
      expect(page.nextSeqnum, 9);
      expect(page.hasMore, isTrue);
    });

    test('empty page parses to no rows, has_more false, next_seqnum falls back to since', () async {
      final mock = MockClient((_) async => http.Response(
            jsonEncode({'rows': [], 'has_more': false, 'next_seqnum': 0}),
            200,
          ));
      final client = SyncClient(_config(), httpClient: mock);
      final page =
          await client.fetchPageRaw('storage_files', topic: 'sync:user:x', since: 7, token: 't');
      expect(page.rows, isEmpty);
      expect(page.hasMore, isFalse);
      // server sent 0; parser takes it as-is (server is authoritative)
      expect(page.nextSeqnum, 0);
    });

    test('non-200 throws HttpSyncException carrying the status', () async {
      final mock = MockClient((_) async => http.Response('nope', 403));
      final client = SyncClient(_config(), httpClient: mock);
      expect(
        () => client.fetchPageRaw('storage_files', topic: 'sync:user:x', token: 't'),
        throwsA(isA<HttpSyncException>().having((e) => e.statusCode, 'statusCode', 403)),
      );
    });

    test('missing token throws StateError', () async {
      final client = SyncClient(_config(), httpClient: MockClient((_) async => http.Response('{}', 200)));
      expect(
        () => client.fetchPageRaw('storage_files', topic: 'sync:user:x'),
        throwsA(isA<StateError>()),
      );
    });
  });

  group('httpPush (push wire)', () {
    test('POSTs scope + pending_changes and parses acks', () async {
      http.Request? captured;
      final mock = MockClient((req) async {
        captured = req;
        return http.Response(
          jsonEncode({
            'acks': [
              {'local_seqnum': 1, 'success': true, 'server_seqnum': 42},
              {'local_seqnum': 2, 'success': false, 'error': 'nope'},
            ]
          }),
          200,
        );
      });

      final client = SyncClient(_config(), httpClient: mock);
      final acks = await client.httpPush(
        'sync:user:x',
        [
          {'local_seqnum': 1, 'table': 'storage_files', 'row_id': 'a', 'operation': 'insert', 'data': {'x': 1}},
          {'local_seqnum': 2, 'table': 'storage_files', 'row_id': 'b', 'operation': 'delete'},
        ],
        token: 'tok-9',
      );

      // request shape
      expect(captured!.method, 'POST');
      expect(captured!.url.path, '/api/sync/member/push');
      expect(captured!.headers['authorization'], 'Bearer tok-9');
      expect(captured!.headers['x-client-id'], 'app_test');
      final body = jsonDecode(captured!.body) as Map<String, dynamic>;
      expect(body['topic'], 'sync:user:x');
      expect((body['pending_changes'] as List).length, 2);
      expect((body['pending_changes'] as List).first['row_id'], 'a');

      // parsed acks
      expect(acks.length, 2);
      expect(acks[0].success, isTrue);
      expect(acks[0].serverSeqnum, 42);
      expect(acks[1].success, isFalse);
      expect(acks[1].error, 'nope');
    });

    test('non-200 throws HttpSyncException', () async {
      final mock = MockClient((_) async => http.Response('boom', 500));
      final client = SyncClient(_config(), httpClient: mock);
      expect(
        () => client.httpPush('sync:user:x', const [], token: 't'),
        throwsA(isA<HttpSyncException>().having((e) => e.statusCode, 'statusCode', 500)),
      );
    });
  });

  group('by-id / by-ids wire', () {
    test('fetchByIdsRaw POSTs {ids} and parses rows', () async {
      http.Request? captured;
      final mock = MockClient((req) async {
        captured = req;
        return http.Response(jsonEncode({'rows': [{'id': 'a'}, {'id': 'b'}]}), 200);
      });
      final client = SyncClient(_config(), httpClient: mock);
      final rows =
          await client.fetchByIdsRaw('storage_files', ['a', 'b'], topic: 'sync:user:x', token: 't');

      expect(captured!.method, 'POST');
      expect(captured!.url.path, '/api/sync/member/storage_files/by-ids');
      expect(captured!.url.queryParameters['topic'], 'sync:user:x');
      expect((jsonDecode(captured!.body) as Map)['ids'], ['a', 'b']);
      expect(rows.map((r) => r['id']).toList(), ['a', 'b']);
    });

    test('fetchByIdsRaw with empty ids makes no request', () async {
      var called = false;
      final mock = MockClient((_) async {
        called = true;
        return http.Response('{}', 200);
      });
      final client = SyncClient(_config(), httpClient: mock);
      final rows = await client.fetchByIdsRaw('storage_files', const [], topic: 'sync:user:x', token: 't');
      expect(rows, isEmpty);
      expect(called, isFalse);
    });

    test('fetchByIdRaw GETs /:table/:id and parses the row', () async {
      http.Request? captured;
      final mock = MockClient((req) async {
        captured = req;
        return http.Response(jsonEncode({'row': {'id': 'x', 'seqnum': 7}}), 200);
      });
      final client = SyncClient(_config(), httpClient: mock);
      final row = await client.fetchByIdRaw('storage_files', 'x', topic: 'sync:user:x', token: 't');

      expect(captured!.method, 'GET');
      expect(captured!.url.path, '/api/sync/member/storage_files/x');
      expect(captured!.url.queryParameters['topic'], 'sync:user:x');
      expect(row!['id'], 'x');
    });

    test('fetchByIdRaw returns null on 404', () async {
      final mock = MockClient((_) async => http.Response('{}', 404));
      final client = SyncClient(_config(), httpClient: mock);
      final row = await client.fetchByIdRaw('storage_files', 'missing', topic: 'sync:user:x', token: 't');
      expect(row, isNull);
    });

    test('fetchByIdRaw throws on a non-200 non-404', () async {
      final mock = MockClient((_) async => http.Response('err', 500));
      final client = SyncClient(_config(), httpClient: mock);
      expect(
        () => client.fetchByIdRaw('storage_files', 'x', topic: 'sync:user:x', token: 't'),
        throwsA(isA<HttpSyncException>()),
      );
    });
  });
}
