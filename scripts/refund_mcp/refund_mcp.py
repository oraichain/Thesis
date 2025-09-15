#!/usr/bin/env python3
"""
Script to analyze MCP tool call usage for refund calculations.
"""

import json
import os
import pickle
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import lmdb


@dataclass
class Conversation:
    """Conversation record structure"""

    id: int
    user_id: str
    conversation_id: str
    published: bool
    configs: Dict[str, Any]
    title: str
    short_description: str
    created_at: datetime
    status: str
    metadata: Dict[str, Any]
    final_result: Optional[str]
    updated_at: datetime


@dataclass
class ConversationEvent:
    """Conversation event record structure"""

    id: int
    conversation_id: str
    event_id: int
    metadata: Dict[str, Any]
    created_at: datetime


@dataclass
class RefundCount:
    """Refund counting structure"""

    total_occurrence_count: int
    refund_count: int


class RefundDatabase:
    """LMDB-backed storage for refund count mapping"""

    def __init__(self, db_path: str = './scripts/refund_mcp/refund_data'):
        self.db_path = db_path
        os.makedirs(db_path, exist_ok=True)
        self.env = lmdb.open(db_path, max_dbs=0, map_size=10**10)  # 10GB

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the LMDB environment"""
        if self.env:
            self.env.close()

    def put_refund_count(self, conversation_id: str, refund_count: RefundCount):
        """Store refund count for a conversation"""
        with self.env.begin(write=True) as txn:
            key = conversation_id.encode('utf-8')
            value = pickle.dumps(refund_count)
            txn.put(key, value)

    def get_refund_count(self, conversation_id: str) -> Optional[RefundCount]:
        """Retrieve refund count for a conversation"""
        with self.env.begin() as txn:
            key = conversation_id.encode('utf-8')
            value = txn.get(key)
            if value:
                return pickle.loads(value)
            return None

    def update_refund_count(self, conversation_id: str, increment: int = 1):
        """Update refund count with upsert logic"""
        existing = self.get_refund_count(conversation_id)
        if existing:
            existing.total_occurrence_count += increment
        else:
            existing = RefundCount(total_occurrence_count=increment, refund_count=0)

        self.put_refund_count(conversation_id, existing)

    def get_all_refund_counts(self) -> Dict[str, RefundCount]:
        """Get all refund counts as a dictionary"""
        result = {}
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                conversation_id = key.decode('utf-8')
                refund_count = pickle.loads(value)
                result[conversation_id] = refund_count
        return result

    def clear_all(self):
        """Clear all data from the database"""
        with self.env.begin(write=True) as txn:
            txn.drop(self.env.open_db())


def create_mock_conversations(count: int = 50) -> List[Conversation]:
    """Create mock conversation records"""
    conversations = []
    base_time = datetime.now()

    titles = [
        'Stablecoin Yield Farming Strategy Consultation',
        'DeFi Arbitrage Opportunity Analysis',
        'NFT Market Trend Research',
        'Crypto Portfolio Optimization',
        'Smart Contract Security Audit',
        'Layer 2 Scaling Solutions Comparison',
        'DAO Governance Token Analysis',
        'Cross-chain Bridge Strategy',
        'Liquidity Mining Strategy Review',
        'DEX Trading Bot Configuration',
    ]

    statuses = ['active', 'completed', 'deleted', 'paused']

    for i in range(count):
        conv = Conversation(
            id=i + 1,
            user_id=f'0x{uuid.uuid4().hex[:40]}',
            conversation_id=uuid.uuid4().hex,
            published=i % 3 == 0,  # Every 3rd conversation is published
            configs={'hidden_prompt': True} if i % 2 == 0 else {},
            title=titles[i % len(titles)],
            short_description='',
            created_at=base_time - timedelta(hours=i),
            status=statuses[i % len(statuses)],
            metadata={},
            final_result=None if i % 4 == 0 else 'Task completed successfully',
            updated_at=base_time - timedelta(minutes=i * 30),
        )
        conversations.append(conv)

    return conversations


def get_conversations_paginated(
    conversations: List[Conversation],
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 20,
    page: int = 0,
) -> List[Conversation]:
    """Get conversations within time range with pagination

    Args:
        conversations: List of all conversations
        start_time: Start of time range (defaults to yesterday)
        end_time: End of time range (defaults to today)
        limit: Number of records per page (default 20)
        page: Page number (0-based)

    Returns:
        List of conversations for the specified page
    """
    if start_time is None:
        end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
    elif end_time is None:
        end_time = start_time + timedelta(days=1)

    # Filter conversations by time range
    filtered = [
        conv for conv in conversations if start_time <= conv.created_at <= end_time
    ]

    # Apply pagination
    start_idx = page * limit
    end_idx = start_idx + limit

    return filtered[start_idx:end_idx]


def get_all_conversations_paginated(
    conversations: List[Conversation],
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 20,
) -> List[Conversation]:
    """Get all conversations within time range using pagination

    This function demonstrates how to fetch all records by iterating through pages
    """
    all_conversations = []
    page = 0

    while True:
        page_conversations = get_conversations_paginated(
            conversations, start_time, end_time, limit, page
        )

        if not page_conversations:  # No more records
            break

        all_conversations.extend(page_conversations)
        page += 1

    return all_conversations


def create_mock_conversation_events(
    conversation_ids: List[str],
) -> List[ConversationEvent]:
    """Create mock conversation events with MCP tool calls"""
    events = []
    base_time = datetime.now()

    # Create MCP events naturally based on probability
    event_id = 1

    for conv_id in conversation_ids:
        # Add some regular events
        for i in range(2, 5):  # 2-4 regular events per conversation
            event = ConversationEvent(
                id=event_id,
                conversation_id=conv_id,
                event_id=i,
                metadata={
                    'id': i,
                    'timestamp': (base_time - timedelta(minutes=i * 10)).isoformat(),
                    'source': 'agent',
                    'message': f'Regular event {i}',
                    'action': 'message',
                },
                created_at=base_time - timedelta(minutes=i * 10),
            )
            events.append(event)
            event_id += 1

        # Add MCP tool call events (50% chance per conversation, 1-3 calls per conversation)
        if hash(conv_id) % 2 == 0:  # 50% chance for this conversation to have MCP calls
            # Determine number of MCP calls for this conversation (1-3)
            num_calls = 1 + (hash(conv_id + 'calls') % 3)  # 1, 2, or 3 calls

            for call_idx in range(num_calls):
                mcp_event = ConversationEvent(
                    id=event_id,
                    conversation_id=conv_id,
                    event_id=event_id,
                    metadata={
                        'id': event_id,
                        'timestamp': (
                            base_time - timedelta(minutes=event_id * 5)
                        ).isoformat(),
                        'source': 'agent',
                        'message': f'I am interacting with the MCP server with name: crypto_insights_service_tool (call {call_idx + 1})',
                        'action': 'call_tool_mcp',
                        'tool_call_metadata': {
                            'function_name': 'crypto_insights_service_tool_mcp_tool_call',
                            'tool_call_id': f'tooluse_{uuid.uuid4().hex[:20]}',
                            'model_response': {
                                'id': f'chatcmpl-{uuid.uuid4().hex}',
                                'created': int(base_time.timestamp()),
                                'model': None,
                                'object': 'chat.completion',
                                'choices': [
                                    {
                                        'finish_reason': 'tool_calls',
                                        'index': 0,
                                        'message': {
                                            'content': f'Analyzing crypto market data... (call {call_idx + 1})',
                                            'role': 'assistant',
                                            'tool_calls': [
                                                {
                                                    'function': {
                                                        'arguments': json.dumps(
                                                            {
                                                                'user_prompt': f'Find top whale positions for BTC - analysis {call_idx + 1}'
                                                            }
                                                        ),
                                                        'name': 'crypto_insights_service_tool_mcp_tool_call',
                                                    },
                                                    'id': f'tooluse_{uuid.uuid4().hex[:20]}',
                                                    'type': 'function',
                                                }
                                            ],
                                        },
                                    }
                                ],
                            },
                            'total_calls_in_response': 1,
                            'enable_show_thought': False,
                        },
                        'args': {
                            'name': 'crypto_insights_service_tool',
                            'arguments': json.dumps(
                                {
                                    'user_prompt': f'Market analysis request {call_idx + 1}'
                                }
                            ),
                            'thought': '',
                            'sid': None,
                        },
                    },
                    created_at=base_time - timedelta(minutes=event_id * 5),
                )
                events.append(mcp_event)
                event_id += 1

    return events


def filter_mcp_tool_events(events: List[ConversationEvent]) -> List[ConversationEvent]:
    """Filter events that contain crypto_insights_service_tool_mcp_tool_call"""
    filtered_events = []

    for event in events:
        metadata = event.metadata
        if 'tool_call_metadata' in metadata:
            tool_metadata = metadata['tool_call_metadata']
            if (
                isinstance(tool_metadata, dict)
                and 'function_name' in tool_metadata
                and tool_metadata['function_name']
                == 'crypto_insights_service_tool_mcp_tool_call'
            ):
                filtered_events.append(event)

    return filtered_events


def populate_refund_database(
    mcp_events: List[ConversationEvent], refund_db: RefundDatabase
) -> None:
    """Populate LMDB database with refund counts from MCP events"""
    print(f'Processing {len(mcp_events)} MCP events...')

    for i, event in enumerate(mcp_events):
        conv_id = event.conversation_id
        refund_db.update_refund_count(conv_id, increment=1)

        if (i + 1) % 100 == 0:  # Progress indicator for large datasets
            print(f'  Processed {i + 1}/{len(mcp_events)} events')

    print('✅ All events processed and stored in LMDB')


def calculate_refunds(
    refund_db: RefundDatabase, conversations: List[Conversation]
) -> Dict[str, int]:
    """Calculate refunds using LMDB database and update refund_count to prevent double-refunding

    Returns:
        Dict mapping user_id to total refund amount
    """
    user_refunds = {}

    # Create conversation_id to user_id mapping
    conv_to_user = {conv.conversation_id: conv.user_id for conv in conversations}

    # Get all refund counts from LMDB
    refund_map = refund_db.get_all_refund_counts()
    print(f'Retrieved {len(refund_map)} conversations from LMDB database')

    for conv_id, refund_data in refund_map.items():
        # Calculate refund amount (total_occurrence_count - refund_count)
        refund_amount = refund_data.total_occurrence_count - refund_data.refund_count

        if refund_amount > 0:
            # Update refund_count to prevent double-refunding
            refund_data.refund_count = refund_data.total_occurrence_count

            # Store updated refund count back to LMDB
            refund_db.put_refund_count(conv_id, refund_data)

            # Add to user refunds
            user_id = conv_to_user.get(conv_id)
            if user_id:
                if user_id in user_refunds:
                    user_refunds[user_id] += refund_amount
                else:
                    user_refunds[user_id] = refund_amount

    print('✅ Updated refund counts in LMDB to prevent double-refunding')
    return user_refunds


def generate_refund_sql(user_refunds: Dict[str, int]) -> List[str]:
    """Generate PostgreSQL UPDATE statements for credit usage refunds"""
    sql_statements = []

    for user_id, refund_amount in user_refunds.items():
        # Update CreditRefundLogs
        refund_sql = f"""UPDATE public."CreditRefundLogs"
SET "totalRefund" = "totalRefund" + {refund_amount},
    "updatedAt" = CURRENT_TIMESTAMP
WHERE "userId" = '{user_id}'
  AND "featureCode" = 'deep_research';"""
        sql_statements.append(refund_sql)

        # Update CreditUsageLogs
        usage_sql = f"""UPDATE public."CreditUsageLogs"
SET "totalUsed" = "totalUsed" + {refund_amount},
    "updatedAt" = CURRENT_TIMESTAMP
WHERE "userId" = '{user_id}'
  AND "featureCode" = 'deep_research';"""
        sql_statements.append(usage_sql)

    return sql_statements


def main():
    """Main execution function"""
    print('Creating mock conversation data...')
    conversations = create_mock_conversations(50)

    print('Getting conversations with pagination...')
    all_conversations = get_all_conversations_paginated(conversations, limit=20)
    print(f'Total conversations in time range: {len(all_conversations)}')

    conversation_ids = [conv.conversation_id for conv in all_conversations]

    print('Creating mock conversation events...')
    events = create_mock_conversation_events(conversation_ids)

    print('Filtering MCP tool call events...')
    mcp_events = filter_mcp_tool_events(events)
    print(f'Found {len(mcp_events)} MCP tool call events')

    # Use LMDB database for refund count storage
    print('\nInitializing LMDB database...')
    with RefundDatabase() as refund_db:
        # Clear any existing data for clean run
        refund_db.clear_all()

        print('Populating refund database...')
        populate_refund_database(mcp_events, refund_db)

        print('\nBefore Refund Processing:')
        print('-' * 60)
        refund_map = refund_db.get_all_refund_counts()
        for conv_id, refund_data in refund_map.items():
            print(
                f'Conversation {conv_id[:8]}...: '
                f'Occurrences={refund_data.total_occurrence_count}, '
                f'Refunds={refund_data.refund_count}'
            )

        print(f'\nTotal conversations with MCP calls: {len(refund_map)}')
        total_occurrences = sum(r.total_occurrence_count for r in refund_map.values())
        print(f'Total MCP tool call occurrences: {total_occurrences}')

        print('\nCalculating refunds...')
        user_refunds = calculate_refunds(refund_db, all_conversations)

        print('\nAfter Refund Processing:')
        print('-' * 60)
        updated_refund_map = refund_db.get_all_refund_counts()
        for conv_id, refund_data in updated_refund_map.items():
            print(
                f'Conversation {conv_id[:8]}...: '
                f'Occurrences={refund_data.total_occurrence_count}, '
                f'Refunds={refund_data.refund_count}'
            )

        print('\nUser Refund Summary:')
        print('-' * 40)
        for user_id, refund_amount in user_refunds.items():
            print(f'User {user_id[:8]}...: {refund_amount} credits')

        print(f'\nTotal users receiving refunds: {len(user_refunds)}')
        total_refund_amount = sum(user_refunds.values())
        print(f'Total refund amount: {total_refund_amount} credits')

        print('\nGenerating SQL statements...')
        sql_statements = generate_refund_sql(user_refunds)

        print('\nSQL Refund Statements:')
        print('=' * 80)
        for i, sql in enumerate(sql_statements, 1):
            print(f'-- Statement {i}')
            print(sql)
            print()

    print('✅ LMDB database closed successfully')


if __name__ == '__main__':
    main()
