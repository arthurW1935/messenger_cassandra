"""
Sample models for interacting with Cassandra tables.
Students should implement these models based on their database schema design.
"""
import uuid
import logging
logger = logging.getLogger(__name__)

from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client

class MessageModel:
    """
    Message model for interacting with the messages table.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve messages
    - How to handle pagination of results
    - How to filter messages by timestamp
    """
    
    # TODO: Implement the following methods
    @staticmethod
    async def get_new_message_id() -> int:
        """
        Get the next message ID.
        
        Returns:
            int: Next message ID
        """
        new_id = uuid.uuid4()
        logger.info(f"New message ID: {new_id}")
        # Get the next message ID
        query = "SELECT message_id FROM messages LIMIT 1"
        logger.info(f"Query: {query}")
        result = await cassandra_client.execute(query)
        logger.info(f"Result: {result}")
        if result:
            return result[0]["message_id"] + 1
        else:
            return 1
    
    @staticmethod
    async def create_message(conversation_id: int, sender_id: int, receiver_id: int, content: str) -> Dict[str, Any]:
        """
        Create a new message.
 
        Args:
            sender_id (int): ID of the sender
            receiver_id (int): ID of the receiver
            content (str): Content of the message
 
        Returns:
            dict: Details of the created message matching MessageResponse schema
        """
        # Get the next message ID
        message_id = await MessageModel.get_new_message_id()
        logger.info(f"Message ID: {message_id}")
 
        created_at = datetime.now()
 
        # Insert into messages table
        query = """
        INSERT INTO messages (message_id, conversation_id, sender_id, receiver_id, content, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        await cassandra_client.execute(query, (message_id, conversation_id, sender_id, receiver_id, content, created_at))

        # Update it with the new message
        update_query = """
        UPDATE last_message_cache 
        SET last_timestamp = %s, last_message = %s, sender_id = %s, receiver_id = %s 
        WHERE conversation_id = %s
        """
        await cassandra_client.execute(update_query, (created_at, content, sender_id, receiver_id, conversation_id))
 
 
        # Return message details in the format expected by MessageResponse
        return {
            "message_id": message_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content,
            "timestamp": created_at,
            "conversation_id": conversation_id
        }
    
    @staticmethod
    async def get_conversation_messages(conversation_id: int, page: int = 1, limit: int = 20) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get messages for a conversation with pagination.
 
        Args:
            conversation_id (int): ID of the conversation
            page (int): Page number for pagination (default: 1)
            limit (int): Number of messages per page (default: 20)
 
        Returns:
            tuple: (List of messages, Total count) for PaginatedMessageResponse
        """
        # Get total count of messages in the conversation
        count_query = """
        SELECT COUNT(*) as count FROM messages WHERE conversation_id = %s
        """
        count_result = await cassandra_client.execute(count_query, (conversation_id,))
        total = count_result[0]["count"] if count_result else 0
 
        # Get messages with pagination
        query = """
        SELECT message_id, sender_id, receiver_id, content, timestamp
        FROM messages
        WHERE conversation_id = %s
        ORDER BY timestamp DESC 
        """
 
        rows = await cassandra_client.execute(query, (conversation_id,))
 
        messages = []
        for row in rows:
            messages.append({
                "id": row["message_id"],
                "sender_id": row["sender_id"],
                "receiver_id": row["receiver_id"],
                "content": row["content"],
                "created_at": row["timestamp"],
                "conversation_id": conversation_id
            })
        
        # calculate the offset and limit for pagination
        total = len(messages)
        offset = (page - 1) * limit
        paginated_messages = messages[offset:offset + limit]
 
        messages = paginated_messages if paginated_messages else []
        return messages, total

    
    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: int, 
        before_timestamp: datetime, 
        page: int = 1, 
        limit: int = 20
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get messages before a timestamp with pagination.
 
        Args:
            conversation_id (int): ID of the conversation
            before_timestamp (datetime): Timestamp to filter messages
            page (int): Page number for pagination (default: 1)
            limit (int): Number of messages per page (default: 20)
 
        Returns:
            tuple: (List of messages, Total count) for PaginatedMessageResponse
        """
        # Get total count of messages before the timestamp
        count_query = """
        SELECT COUNT(*) as count FROM messages 
        WHERE conversation_id = %s AND timestamp < %s
        """
        count_result = await cassandra_client.execute(count_query, (conversation_id, before_timestamp))
        total = count_result[0]["count"] if count_result else 0
 
        # Get messages before timestamp with pagination
        query = """
        SELECT message_id, sender_id, receiver_id, content, timestamp
        FROM messages
        WHERE conversation_id = %s AND timestamp < %s
        ORDER BY timestamp DESC
        """
        rows = await cassandra_client.execute(query, (conversation_id, before_timestamp))
 
        messages = []
        for row in rows:
            messages.append({
                "id": row["message_id"],
                "sender_id": row["sender_id"],
                "receiver_id": row["receiver_id"],
                "content": row["content"],
                "created_at": row["timestamp"],
                "conversation_id": conversation_id
            })
 
        total = len(messages)
        offset = (page - 1) * limit
        paginated_messages = messages[offset:offset + limit]
 
        messages = paginated_messages if paginated_messages else []
 
        return messages, total



class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve conversations for a user
    - How to handle pagination of results
    - How to optimize for the most recent conversations
    """
    
    # TODO: Implement the following methods
    @staticmethod
    async def get_new_conversation_id() -> int:
        """
        Get the next conversation ID.
        
        Returns:
            int: Next conversation ID
        """
        # Get the next conversation ID from the counter table
        query = "SELECT conversation_id FROM conversations LIMIT 1"
        result = await cassandra_client.execute(query)
        if result:
            return result[0]["conversation_id"] + 1
        else:
            return 1
    
    @staticmethod
    async def get_user_conversations(user_id: int, page: int = 1, limit: int = 20) -> Tuple[List[Dict[str, Any]], int]:
 
        # Get conversations with pagination
        query1 = """
        SELECT conversation_id, sender_id, receiver_id, last_timestamp, last_message
        FROM last_message_cache 
        WHERE sender_id = %s 
        ALLOW FILTERING       
        """
 
        query2 = """
        SELECT conversation_id, sender_id, receiver_id, last_timestamp, last_message
        FROM last_message_cache 
        WHERE receiver_id = %s
        ALLOW FILTERING
        """
        rows1 = await cassandra_client.execute(query2, (user_id,))
        rows2 = await cassandra_client.execute(query1, (user_id,))

        rows_list = list(rows1)
        rows_list.extend(list(rows2))
        logger.info(f"Rows fetched: {rows_list}")

        rows_list.sort(key=lambda x: x["last_timestamp"], reverse=True)
        logger.info(f"Rows after soring: {rows_list}")
 
        total = len(rows_list)
        offset = (page - 1) * limit
        paginated_rows = rows_list[offset:offset + limit]
 
        conversations = []
        for row in paginated_rows:
            conversations.append({
                "id": row["conversation_id"],
                "user1_id": row["sender_id"],
                "user2_id": row["receiver_id"],
                "last_message_at": row["last_timestamp"],
                "last_message_content": row["last_message"]
            })

        # conversations = paginated_rows if paginated_rows else []
        return conversations, total
    
    @staticmethod
    async def get_conversation(conversation_id: int) -> Dict[str, Any]:
        """
        Get a conversation by ID.
 
        Args:
            conversation_id (int): ID of the conversation
 
        Returns:
            dict: Details of the conversation matching ConversationResponse schema
        """
        query = """
        SELECT conversation_id, sender_id, receiver_id, last_timestamp, last_message
        FROM last_message_cache
        WHERE conversation_id = %s
        """
        rows = await cassandra_client.execute(query, (conversation_id,))
 
        if not rows:
            return None
 
        row = rows[0]
 
        return {
            "conversation_id": row["conversation_id"],
            "sender_id": row["sender_id"],
            "receiver_id": row["receiver_id"],
            "last_message_at": row["last_timestamp"],
            "last_message_content": row["last_message"]
        }
    
    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int) -> Dict[str, Any]:
        """
        Get an existing conversation between two users or create a new one.
 
        Args:
            user1_id (int): ID of the first user
            user2_id (int): ID of the second user
 
        Returns:
            dict: Details of the conversation matching ConversationResponse schema
        """

        logger.info(f"Creating or getting conversation between {user1_id} and {user2_id}")
 
        # Check if the conversation already exists
        query = """
        SELECT conversation_id FROM last_message_cache 
        WHERE sender_id = %s AND receiver_id = %s
        ALLOW FILTERING
        """
        # If conversation exists, get its details
        rows1 = await cassandra_client.execute(query, (user1_id, user2_id))
        if rows1:
            return await ConversationModel.get_conversation(rows1[0]["conversation_id"])
        logger.info(f"Rows fetched: {rows1}")

        rows2 = await cassandra_client.execute(query, (user2_id, user1_id))
        if rows2:
            return await ConversationModel.get_conversation(rows2[0]["conversation_id"])
        logger.info(f"Rows fetched: {rows2}")
 
        # If conversation doesn't exist, create a new one
        # Get the next conversation ID
        conversation_id = await ConversationModel.get_new_conversation_id()
        logger.info(f"Conversation ID: {conversation_id}")

        created_at = datetime.now()
 
        # Insert into conversations table
        insert_query = """
        INSERT INTO conversations (conversation_id, user1_id, user2_id, last_timestamp)
        VALUES (%s, %s, %s, %s)
        """
        await cassandra_client.execute(insert_query, (conversation_id, user1_id, user2_id, created_at))

        # Insert into last message_cache table
        insert_cache_query = """
        INSERT INTO last_message_cache (conversation_id, sender_id, receiver_id, last_timestamp, last_message)
        VALUES (%s, %s, %s, %s, %s)
        """
        await cassandra_client.execute(insert_cache_query, (conversation_id, user1_id, user2_id, created_at, None))
 
 
        # Return conversation details in the format expected by ConversationResponse
        return {
            "conversation_id": conversation_id,
            "sender_id": user1_id,
            "receiver_id": user2_id,
            "last_message_at": created_at,
            "last_message_content": None
        }