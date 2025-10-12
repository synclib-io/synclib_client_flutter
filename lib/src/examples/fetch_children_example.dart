/// Example of using sendMessage to fetch children by parent_id
///
/// This demonstrates how to use the generic sendMessage API to call
/// TG-specific custom queries on the server.

import 'package:synclib_sync/synclib_sync.dart';

Future<void> fetchChildrenExample(SyncClient syncClient) async {
  // Example 1: Fetch stages for a specific parent
  final stagesResponse = await syncClient.sendMessage('fetch_children', {
    'table': 'stages',
    'parent_id': 'parent_123',
    'limit': 50, // optional, defaults to 100
  });

  final stages = stagesResponse['rows'] as List;
  print('Fetched ${stages.length} stages');

  for (final stage in stages) {
    final stageMap = stage as Map<String, dynamic>;
    print('Stage ${stageMap['id']}: seqnum=${stageMap['seqnum']}');
    print('  Document: ${stageMap['document']}');
  }

  // Example 2: Fetch journal entries for a specific parent
  final journalResponse = await syncClient.sendMessage('fetch_children', {
    'table': 'journal_entries',
    'parent_id': 'parent_456',
  });

  final entries = journalResponse['rows'] as List;
  print('Fetched ${entries.length} journal entries');
}

/// Error handling example
Future<void> fetchChildrenWithErrorHandling(
  SyncClient syncClient,
  String parentId,
) async {
  try {
    final response = await syncClient.sendMessage('fetch_children', {
      'table': 'stages',
      'parent_id': parentId,
    });

    final rows = response['rows'] as List;
    // Process rows...
  } catch (e) {
    print('Error fetching children: $e');
    // Handle error (e.g., show user message, retry, etc.)
  }
}
