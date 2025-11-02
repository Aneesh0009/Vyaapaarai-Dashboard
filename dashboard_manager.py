# dashboard_manager.py
import logging
import asyncio
import os
from typing import Any, Dict, List, Optional
from db import DatabaseV6 as Database

from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class DashboardManager:

    # FIX 1: Add __init__ to accept the db_instance
    def __init__(self, db_instance: Database):
        """
        Initialize the DashboardManager with a database instance.
        """
        if not hasattr(db_instance, 'db'): # Quick check
             raise ValueError("Invalid Database instance passed to DashboardManager")
        self.db = db_instance.db # Store the raw motor database object (db.db)
        self.db_instance = db_instance # Store the full class instance
        logger.info("DashboardManager initialized")

    # FIX 2: Implement the logic for get_overview
    async def get_overview(self, merchant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve overview statistics for the dashboard.
        (This logic was previously in the old db.get_overview_stats)
        """
        if self.db is None:
            logger.error("DashboardManager: Database not initialized.")
            return {}
        
        try:
            # --- Define Collections ---
            conversations_coll = self.db["conversations"]
            inventory_coll = self.db["inventory"]
            orders_coll = self.db["orders"]
            
            # --- Build Filters ---
            merchant_filter = {}
            if merchant_id:
                merchant_filter["merchant_id"] = merchant_id

            # --- Timezone handling ---
            tz_env = os.getenv("DASHBOARD_TZ", "+05:30")
            def _parse_tz_offset(tz_str: str) -> timezone:
                try:
                    sign = 1 if tz_str.startswith("+") else -1
                    hh, mm = tz_str[1:].split(":")
                    return timezone(sign * timedelta(hours=int(hh), minutes=int(mm)))
                except Exception:
                    return timezone.utc

            local_tz = _parse_tz_offset(tz_env)

            # --- Run Queries in Parallel ---
            # Total Products
            task_products = inventory_coll.count_documents(merchant_filter)
            
            # Total Orders
            task_orders = orders_coll.count_documents(merchant_filter)
            
            # Low Stock Count
            low_stock_filter = {**merchant_filter, "$expr": {"$lt": ["$quantity", "$reorder_level"]}}
            task_low_stock = inventory_coll.count_documents(low_stock_filter)

            # Total Conversations
            # Adjust filter if conv ID doesn't use merchant_id prefix
            conv_filter = {}
            if merchant_id:
                conv_filter["conversation_id"] = {"$regex": f"^{merchant_id}_"}
            task_conversations = conversations_coll.count_documents(conv_filter)

            # Today Messages (aligned to local timezone day start)
            now_utc = datetime.now(timezone.utc)
            today_local_start = now_utc.astimezone(local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
            today_start = today_local_start.astimezone(timezone.utc)
            today_filter = {**conv_filter, "messages.timestamp": {"$gte": today_start}}
            # This count is complex (counts sub-documents), use aggregation
            pipeline_today = [
                {"$match": {**conv_filter, "messages.timestamp": {"$gte": today_start}}},
                {"$unwind": "$messages"},
                {"$match": {"messages.timestamp": {"$gte": today_start}}},
                {"$count": "count"}
            ]
            task_today_messages = conversations_coll.aggregate(pipeline_today).to_list(length=1)

            # Orders by Day (Last 7 days)
            seven_days_ago = today_start - timedelta(days=7)
            pipeline_orders_day = [
                {"$match": {**merchant_filter, "created_at": {"$gte": seven_days_ago.isoformat()}}},
                {"$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": {"$dateFromString": {"dateString": "$created_at"}}, "timezone": tz_env}},
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id": 1}},
                {"$project": {"date": "$_id", "orders": "$count", "_id": 0}}
            ]
            task_orders_day = orders_coll.aggregate(pipeline_orders_day).to_list(length=7)

            # Top Categories (by product count)
            pipeline_top_cat = [
                {"$match": merchant_filter},
                {"$group": {"_id": "$category", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5},
                {"$project": {"category": "$_id", "count": "$count", "_id": 0}}
            ]
            task_top_categories = inventory_coll.aggregate(pipeline_top_cat).to_list(length=5)

            # Top Intents (from conversations) - filter out empty/null intents
            pipeline_top_intents = [
                {"$match": conv_filter},
                {"$unwind": "$messages"},
                {"$match": {"messages.intent": {"$nin": [None, ""]}}},
                {"$group": {"_id": "$messages.intent", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5},
                {"$project": {"intent": "$_id", "count": "$count", "_id": 0}}
            ]
            task_top_intents = conversations_coll.aggregate(pipeline_top_intents).to_list(length=5)

            # Daily Activity (Messages per day, last 7 days)
            pipeline_daily_activity = [
                {"$match": {**conv_filter, "messages.timestamp": {"$gte": seven_days_ago}}},
                {"$unwind": "$messages"},
                {"$match": {"$and": [
                    {"messages.timestamp": {"$gte": seven_days_ago}},
                ]}},
                {"$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$messages.timestamp", "timezone": tz_env}},
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id": 1}},
                {"$project": {"date": "$_id", "messages": "$count", "_id": 0}}
            ]
            task_daily_activity = conversations_coll.aggregate(pipeline_daily_activity).to_list(length=7)


            # --- Execute all queries ---
            results = await asyncio.gather(
                task_products, task_orders, task_low_stock, task_conversations,
                task_today_messages, task_orders_day, task_top_categories,
                task_top_intents, task_daily_activity,
                return_exceptions=True # Continue if one query fails
            )
            
            # --- Assign results ---
            (
                total_products, total_orders, low_stock_count, total_conversations,
                today_messages_res, orders_by_day, top_categories,
                top_intents, daily_activity
            ) = results
            
            # Helper to check for gather exceptions
            def handle_result(res, default_val):
                if isinstance(res, Exception):
                    logger.error(f"Overview query failed: {res}", exc_info=res)
                    return default_val
                return res

            today_messages_count = handle_result(today_messages_res, [{}])[0].get("count", 0) if today_messages_res and not isinstance(today_messages_res, Exception) else 0

            return {
                "total_products": handle_result(total_products, 0),
                "total_orders": handle_result(total_orders, 0),
                "total_conversations": handle_result(total_conversations, 0),
                "today_messages": today_messages_count,
                "orders_by_day": handle_result(orders_by_day, []),
                "low_stock_count": handle_result(low_stock_count, 0),
                "top_categories": handle_result(top_categories, []),
                "daily_activity": handle_result(daily_activity, []),
                "top_intents": handle_result(top_intents, [])
            }
        except Exception as e:
            logger.error(f"Error in DashboardManager.get_overview: {e}", exc_info=True)
            return {} # Return empty dict on major failure

    # FIX 3: Implement the logic for get_messages
    async def get_messages(self, filters: Dict, limit: int) -> List[Dict]:
        """
        Retrieve recent messages with optional filters.
        (Logic moved from old db.get_messages)
        
        Assumes messages are stored as a sub-array in the 'conversations' collection.
        """
        if self.db is None:
            return []

        try:
            conversations_coll = self.db["conversations"]
            
            # Build query from filters
            query = {}
            if filters.get("merchant_id"):
                query["conversation_id"] = {"$regex": f"^{filters['merchant_id']}_"}
            if filters.get("user_phone"):
                # This logic assumes conversation_id format: {merchant_id}_{user_phone}
                # If user_phone is passed, it might override/conflict with merchant_id
                query["conversation_id"] = {"$regex": f"_{filters['user_phone']}$"}
                # More robustly:
                # query = {}
                # if filters.get("merchant_id"):
                #     query["conversation_id"] = {"$regex": f"^{filters['merchant_id']}_"}
                # if filters.get("user_phone"):
                #     query["conversation_id"] = {"$regex": f"_{filters['user_phone']}$"}
                # If both are present, this will only find convos matching the *last* regex.
                # A better query if both are present:
                # if filters.get("merchant_id") and filters.get("user_phone"):
                #     query["conversation_id"] = f"{filters['merchant_id']}_{filters['user_phone']}"
                # elif filters.get("merchant_id"):
                #     query["conversation_id"] = {"$regex": f"^{filters['merchant_id']}_"}
                # elif filters.get("user_phone"):
                #     query["conversation_id"] = {"$regex": f"_{filters['user_phone']}$"}

            # --- Unwind messages and inject user_phone from conversation_id ---
            pipeline: List[Dict[str, Any]] = [
                {"$match": query},
                {"$addFields": {
                    "user_phone": {
                        "$last": {"$split": ["$conversation_id", "_"]}
                    }
                }},
                {"$unwind": "$messages"},
                {"$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            "$messages",
                            {
                                "user_phone": "$user_phone",
                                "conversation_id": "$conversation_id",
                                "merchant_id": "$merchant_id"
                            }
                        ]
                    }
                }},
                {"$sort": {"timestamp": -1}},
                {"$limit": limit}
            ]
            
            # If no filters, we get latest messages from *all* conversations
            if not query:
                 pipeline.pop(0) # Remove the empty $match

            cursor = conversations_coll.aggregate(pipeline)
            msgs = await cursor.to_list(length=limit)

            # Normalize timestamp and map From
            normalized: List[Dict[str, Any]] = []
            for m in msgs:
                ts = m.get("timestamp") or m.get("created_at")
                if isinstance(ts, datetime):
                    ts = ts.isoformat()
                normalized.append({
                    **m,
                    "timestamp": ts,
                    "role": m.get("role", "unknown"),
                    "content": m.get("content", "N/A"),
                    "intent": m.get("intent", ""),
                    "From": m.get("user_phone", "N/A"),
                    "user_phone": m.get("user_phone", "N/A")
                })
            return normalized
        
        except Exception as e:
            logger.error(f"Error in DashboardManager.get_messages: {e}", exc_info=True)
            return []