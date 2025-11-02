# db.py - Unified Database Layer for VyaapaarAI (v6.0)
"""
MongoDB database layer with async Motor.
MERGED VERSION 6.0: Combines the comprehensive v5 Database class with
v4 admin functions, encapsulating all logic into a single class.

- Unified `DatabaseV6` class handles all collections:
  - Merchant: products, orders, carts, messages, etc.
  - Admin: merchants, admin_actions, logs
- Production-ready connection with SSL (from v4).
- Merged indexes for both admin and merchant queries.
- Provides a backward-compatibility layer of standalone functions
  for the main app.py.
"""

import asyncio
import logging
import re
from typing import Dict, List, Optional, AsyncGenerator, Any
from datetime import datetime, timezone, timedelta

# Motor/Mongo imports
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from bson import ObjectId
from bson.errors import InvalidId
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError, OperationFailure
from dotenv import load_dotenv
import os
import ssl
import certifi # For production SSL

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# ============================================================
# UNIFIED DATABASE CLASS (V6)
# ============================================================

class DatabaseV6:
    """
    Unified MongoDB database interface for VyaapaarAI v6.0.
    Handles all Admin and Merchant data logic.
    """

    def __init__(self, mongo_uri: Optional[str] = None, db_name: Optional[str] = None):
        """
        Initialize MongoDB connection configuration.
        """
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = db_name or os.getenv("DB_NAME", "vyaapaar_ai_v6") # Use v6 DB name

        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self._initialized = False

        logger.info(f"DatabaseV6 configured: uri={self.mongo_uri[:20]}..., db={self.db_name}")

    async def initialize(self):
        """
        Initialize database connection and create indexes.
        Must be called during app startup.
        """
        if self._initialized:
            logger.warning("DatabaseV6 already initialized")
            return

        try:
            # Create MongoDB client with SSL settings for production (from v4)
            self.client = AsyncIOMotorClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=5000,
                tlsCAFile=certifi.where() if "mongodb+srv" in self.mongo_uri else None
            )

            # Get database
            self.db = self.client[self.db_name]

            # Test connection
            await self.client.admin.command('ping')
            logger.info(f"Connected to MongoDB: {self.db_name}")

            # Create indexes
            await self._create_indexes()

            self._initialized = True
            logger.info(f"DatabaseV6 initialized: {self.db_name}")

        except ServerSelectionTimeoutError as sste:
             logger.critical(f"MongoDB connection failed: Timeout. URI: {self.mongo_uri}. Error: {sste}", exc_info=True)
             logger.critical("Check: Is MongoDB running? Is the URI correct? Is the IP whitelisted (if using Atlas)?")
             raise
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}", exc_info=True)
            raise

    async def _create_indexes(self):
        """
        Create all indexes for performance optimization (Merged v5 + v4).
        Uses asyncio.gather for parallel creation.
        """
        if self.db is None:
            logger.error("Database not initialized, cannot create indexes")
            raise RuntimeError("Database not initialized before creating indexes")

        index_tasks = []

        try:
            # === v5 Merchant Indexes (from db-1.py) ===
            
            # Messages collection
            messages = self.db["messages"]
            index_tasks.extend([
                messages.create_index([("merchant_id", 1), ("timestamp", -1)], name="merchant_id_timestamp_desc"),
                messages.create_index([("merchant_id", 1), ("customer_phone", 1)], name="merchant_customer"),
                messages.create_index("timestamp", name="timestamp_desc")
            ])

            # Carts collection with TTL
            carts = self.db["carts"]
            index_tasks.extend([
                carts.create_index("conversation_id", unique=True),
                carts.create_index("created_at", expireAfterSeconds=86400), # 24h TTL
                carts.create_index("merchant_id")
            ])

            # Orders collection
            orders = self.db["orders"]
            index_tasks.extend([
                orders.create_index("order_id", unique=True),
                orders.create_index("customer_phone"),
                orders.create_index([("merchant_id", 1), ("status", 1)]),
                orders.create_index([("merchant_id", 1), ("created_at", -1)]),
                orders.create_index("expiry_time")
            ])
            
            # Products collection (NEW v5/v6)
            products = self.db["products"]
            index_tasks.extend([
                products.create_index([("merchant_id", 1), ("sku", 1)], unique=True, sparse=True, name="merchant_sku_unique"),
                products.create_index([("merchant_id", 1), ("product_name", 1)], name="merchant_product_name"),
                products.create_index("category"),
                products.create_index("product_id")
            ])

            # Inventory collection (LEGACY)
            inventory = self.db["inventory"]
            index_tasks.extend([
                inventory.create_index([("merchant_id", 1), ("product_id", 1)], unique=True, name="legacy_inv_merchant_product_id"),
            ])
            
            # Knowledge Base
            kb = self.db["knowledge_base"]
            index_tasks.extend([
                kb.create_index("merchant_id"),
                kb.create_index("category"),
            ])

            # Alerts collection (general)
            alerts = self.db["alerts"]
            index_tasks.extend([
                alerts.create_index("merchant_id"),
                alerts.create_index("severity"),
                alerts.create_index("product_id"),
                alerts.create_index("status"),
                alerts.create_index([("merchant_id", 1), ("severity", -1), ("created_at", -1)]),
            ])

            # === v4 Admin Indexes (from db-2.py) ===

            # Merchants collection
            merchants = self.db["merchants"]
            index_tasks.extend([
                merchants.create_index("username", unique=True, name="username_unique"),
                merchants.create_index("details.whatsapp_phone_id", name="whatsapp_phone_id")
            ])

            # Admin actions log index
            admin_actions = self.db["admin_actions"]
            index_tasks.extend([
                admin_actions.create_index([("timestamp", -1)], name="admin_action_timestamp_desc")
            ])

            # Execute all index creations in parallel
            results = await asyncio.gather(*index_tasks, return_exceptions=True)
            
            index_conflicts = 0
            other_errors = []
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    if isinstance(result, OperationFailure) and ("already exists" in str(result).lower() or "index options" in str(result).lower()):
                        index_conflicts += 1
                    else:
                        other_errors.append((i, result))
            
            if index_conflicts > 0:
                logger.info(f"Index creation: {index_conflicts} indexes already exist (safe to ignore)")
            
            if other_errors:
                for idx, err in other_errors:
                    logger.error(f"Index creation error for task {idx}: {err}")
            
            if not other_errors:
                logger.info("All database indexes ensured successfully")

        except Exception as e:
            logger.error(f"Unexpected error during index creation: {e}", exc_info=True)

    def get_collection(self, name: str):
        """
        Return a Motor collection handle.
        """
        if self.db is None:
            raise RuntimeError("Database not initialized")
        return self.db[name]

    async def health_check(self) -> bool:
        """
        Check if database connection is healthy.
        """
        if self.db is None:
            logger.warning("Database not initialized for health check")
            return False
        try:
            await asyncio.wait_for(self.db.command("ping"), timeout=2.0)
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    async def close(self):
        """Close database connection."""
        if self.client:
            self.client.close()
            logger.info("DatabaseV6 connection closed")
            self._initialized = False
            self.db = None
            self.client = None
            
    # ============================================================
    # ADMIN METHODS (Moved from v4 standalone)
    # ============================================================

    async def create_merchant(
        self,
        username: str,
        password: str,
        full_name: str,
        phone: str,
        details: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new merchant account.
        """
        from auth import hash_password  # Local import to avoid circular dependency

        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            # Check if username already exists
            existing = await self.db.merchants.find_one({"username": username})
            if existing:
                raise ValueError(f"Merchant with username '{username}' already exists")

            merchant = {
                "username": username,
                "password_hash": hash_password(password),
                "full_name": full_name,
                "phone": phone,
                "details": details or {},
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "active": True
            }

            result = await self.db.merchants.insert_one(merchant)
            merchant_id = str(result.inserted_id)

            logger.info(f"Created merchant: {username} (ID: {merchant_id})")
            return merchant_id

        except Exception as e:
            logger.error(f"Error creating merchant: {e}")
            raise

    async def get_merchant_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get merchant by username.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            merchant = await self.db.merchants.find_one({"username": username})
            if merchant:
                merchant["_id"] = str(merchant["_id"])
            return merchant
        except Exception as e:
            logger.error(f"Error getting merchant by username: {e}")
            return None

    async def get_all_merchants(self) -> List[Dict[str, Any]]:
        """
        Get all merchants.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            merchants = await self.db.merchants.find().to_list(1000)
            for merchant in merchants:
                merchant["_id"] = str(merchant["_id"])
                merchant.pop("password_hash", None) # Remove password hash
            return merchants
        except Exception as e:
            logger.error(f"Error getting all merchants: {e}")
            return []

    async def delete_merchant_cascade(self, merchant_id: str):
        """
        Delete merchant and ALL associated data (cascade delete).
        Runs in background - deletes from all collections.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            # Use merchant_id (which is the _id string)
            oid = ObjectId(merchant_id)

            # Find the merchant doc to get the username for logging
            merchant_doc = await self.db.merchants.find_one({"_id": oid}, {"username": 1})
            username = merchant_doc.get("username", "unknown") if merchant_doc else "unknown"
            
            logger.warning(f"Starting cascade delete for merchant: {username} (ID: {merchant_id})")

            # Collections to clean. Note: 'merchant_id' in other collections
            # might refer to the username OR the _id string.
            # Based on app-2.py, 'merchant_id' seems to be the _id string.
            collections_to_clean = [
                "messages", "products", "inventory", "orders", "carts",
                "alerts", "knowledge_base", "business_rules"
            ]

            total_deleted = 0
            for collection_name in collections_to_clean:
                result = await self.db[collection_name].delete_many({"merchant_id": merchant_id})
                deleted_count = result.deleted_count
                total_deleted += deleted_count
                logger.info(f"Deleted {deleted_count} documents from {collection_name} for merchant {merchant_id}")

            # Delete merchant document itself
            await self.db.merchants.delete_one({"_id": oid})
            logger.info(f"Deleted merchant document for {username} (ID: {merchant_id})")
            logger.info(f"Cascade delete complete. Total documents deleted: {total_deleted + 1}")

        except InvalidId:
             logger.error(f"Invalid merchant_id for cascade delete: {merchant_id}")
        except Exception as e:
            logger.error(f"Error in cascade delete: {e}")
            raise

    async def get_system_wide_stats(self) -> Dict[str, Any]:
        """
        Get system-wide statistics across all merchants.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            total_merchants = await self.db.merchants.count_documents({})
            total_messages = await self.db.messages.count_documents({})
            total_orders = await self.db.orders.count_documents({})
            total_products = await self.db.products.count_documents({})
            pending_orders = await self.db.orders.count_documents({"status": "pending"}) # Assumes "pending" status

            return {
                "total_merchants": total_merchants,
                "total_messages": total_messages,
                "total_orders": total_orders,
                "total_products": total_products,
                "pending_orders": pending_orders,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting system-wide stats: {e}")
            return {}

    async def log_admin_action(
        self,
        admin_username: str,
        action: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Log admin actions for audit trail.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            log_entry = {
                "admin_username": admin_username,
                "action": action,
                "details": details or {},
                "timestamp": datetime.now(timezone.utc)
            }
            await self.db.admin_actions.insert_one(log_entry)
            logger.info(f"Admin action logged: {admin_username} - {action}")
        except Exception as e:
            logger.error(f"Error logging admin action: {e}")

    async def get_all_messages_admin(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent messages from ALL merchants (admin view).
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            messages = await self.db.messages.find().sort("timestamp", -1).limit(limit).to_list(limit)
            for msg in messages:
                msg["_id"] = str(msg["_id"])
            return messages
        except Exception as e:
            logger.error(f"Error getting all messages (admin): {e}")
            return []

    # ============================================================
    # MERCHANT METHODS (from v5 class)
    # ============================================================

    # ========== MESSAGE / CONVERSATION METHODS ==========
    
    async def insert_message(self, message_data: Dict[str, Any]) -> str:
        """
        Insert a new message.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            if "timestamp" not in message_data:
                message_data["timestamp"] = datetime.now(timezone.utc)

            result = await self.db.messages.insert_one(message_data)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error inserting message: {e}")
            raise

    async def save_conversation_message(self, conversation_id: str, message: Dict):
        """
        Save message to conversation history (Legacy v5 method).
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            await self.db.conversations.update_one(
                {"conversation_id": conversation_id},
                {
                    "$push": {"messages": message},
                    "$set": {"updated_at": datetime.now(timezone.utc)}
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving conversation message: {e}", exc_info=True)
            raise

    async def get_conversation_history(self, conversation_id: str, limit: int = 10) -> List[Dict]:
        """
        Retrieve recent conversation messages (Legacy v5 method).
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            doc = await self.db.conversations.find_one(
                {"conversation_id": conversation_id},
                {"_id": 0, "messages": {"$slice": -limit}}
            )
            return doc.get("messages", []) if doc else []
        except Exception as e:
            logger.error(f"Error retrieving conversation history: {e}")
            return []
            
    async def get_messages(self, filters: Dict, limit: int = 25) -> List[Dict]:
        """
        Retrieve recent messages for dashboard.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            query: Dict = {}
            merchant_id = filters.get("merchant_id")
            if merchant_id:
                query["merchant_id"] = merchant_id
            user_phone = filters.get("user_phone")
            if user_phone:
                query["customer_phone"] = user_phone
            
            cursor = self.db.messages.find(query).sort("timestamp", -1).limit(limit)
            messages = await cursor.to_list(length=limit)
            
            for msg in messages:
                msg["_id"] = str(msg["_id"])
            return messages
        except Exception as e:
            logger.error(f"Error retrieving dashboard messages: {e}")
            return []

    # ========== CART METHODS ==========

    async def get_cart(self, conversation_id: str) -> Optional[Dict]:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            cart = await self.db.carts.find_one({"conversation_id": conversation_id})
            if cart:
                cart["_id"] = str(cart["_id"])
            return cart
        except Exception as e:
            logger.error(f"Error retrieving cart: {e}")
            return None

    async def upsert_cart(self, cart_data: Dict):
        if self.db is None: raise RuntimeError("Database not initialized")
        conversation_id = cart_data.get("conversation_id")
        if not conversation_id:
             raise ValueError("conversation_id is required in cart_data")
        try:
            now = datetime.now(timezone.utc)
            cart_data["updated_at"] = now
            if "created_at" in cart_data:
                del cart_data["created_at"] # Avoid conflict
                
            await self.db.carts.update_one(
                {"conversation_id": conversation_id},
                {"$set": cart_data, "$setOnInsert": {"created_at": now}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error upserting cart: {e}", exc_info=True)
            raise

    async def delete_cart(self, conversation_id: str) -> bool:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            result = await self.db.carts.delete_one({"conversation_id": conversation_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting cart: {e}")
            return False

    # ========== ORDER METHODS ==========

    async def create_order(self, order_data: Dict) -> str:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            now = datetime.now(timezone.utc)
            order_data["created_at"] = order_data.get("created_at", now)
            order_data["updated_at"] = now
            result = await self.db.orders.insert_one(order_data)
            logger.info(f"Order {order_data.get('order_id')} created in database")
            return str(result.inserted_id)
        except DuplicateKeyError:
            order_id = order_data.get("order_id")
            logger.error(f"Duplicate order ID: {order_id}")
            raise ValueError(f"Order {order_id} already exists")
        except Exception as e:
            logger.error(f"Error creating order: {e}", exc_info=True)
            raise

    async def get_order(self, order_id: str) -> Optional[Dict]:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            # v6 queries by the custom string order_id
            order = await self.db.orders.find_one({"order_id": order_id})
            if order:
                order["_id"] = str(order["_id"])
            return order
        except Exception as e:
            logger.error(f"Error retrieving order {order_id}: {e}")
            return None

    async def update_order(self, order_id: str, order_data: Dict):
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            now = datetime.now(timezone.utc)
            has_operator = any(isinstance(k, str) and k.startswith("$") for k in order_data.keys())

            if has_operator:
                update_spec = dict(order_data)
                if "$set" not in update_spec: update_spec["$set"] = {}
                update_spec["$set"]["updated_at"] = now
            else:
                fields = dict(order_data)
                fields["updated_at"] = now
                update_spec = {"$set": fields}
            
            # Sanitize datetime strings
            if "$set" in update_spec:
                for key, value in update_spec["$set"].items():
                    if isinstance(value, str) and re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
                        try:
                            update_spec["$set"][key] = datetime.fromisoformat(value.replace("Z", "+00:00"))
                        except ValueError:
                            pass # Keep as string

            result = await self.db.orders.update_one({"order_id": order_id}, update_spec)

            if result.matched_count == 0:
                logger.warning(f"Order {order_id} not found for update")
            else:
                logger.debug(f"Order {order_id} updated (Modified: {result.modified_count})")
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating order {order_id}: {e}", exc_info=True)
            raise

    async def get_orders_by_customer(self, customer_phone: str, limit: int = 10, status_filter: Optional[str] = None) -> List[Dict]:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            query = {"customer_phone": customer_phone}
            if status_filter:
                query["status"] = str(status_filter).lower()
            cursor = self.db.orders.find(query).sort("created_at", -1).limit(limit)
            orders = await cursor.to_list(length=limit)
            for o in orders: o["_id"] = str(o["_id"])
            return orders
        except Exception as e:
            logger.error(f"Error retrieving customer orders: {e}")
            return []

    async def get_orders_by_merchant(self, merchant_id: str, status_filter: Optional[str] = None, limit: int = 50) -> List[Dict]:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            query = {"merchant_id": merchant_id}
            if status_filter:
                query["status"] = str(status_filter).lower()
            cursor = self.db.orders.find(query).sort("created_at", -1).limit(limit)
            orders = await cursor.to_list(length=limit)
            for o in orders: o["_id"] = str(o["_id"])
            return orders
        except Exception as e:
            logger.error(f"Error retrieving merchant orders: {e}")
            return []

    async def get_orders_by_status(self, status: str, limit: int = 1000) -> List[Dict]:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            cursor = self.db.orders.find({"status": str(status).lower()}).limit(limit)
            orders = await cursor.to_list(length=limit)
            for o in orders: o["_id"] = str(o["_id"])
            return orders
        except Exception as e:
            logger.error(f"Error retrieving orders by status: {e}")
            return []

    async def get_orders_by_statuses(self, statuses: List[str], limit: int = 1000) -> List[Dict]:
        """
        Get orders matching any status in the provided list (e.g., for expiry check).
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            # Use MongoDB's "$in" operator to match any status in the list
            query = {"status": {"$in": statuses}}
            cursor = self.db.orders.find(query).limit(limit)
            orders = await cursor.to_list(length=limit)
            for o in orders: o["_id"] = str(o["_id"])
            return orders
        except Exception as e:
            logger.error(f"Error retrieving orders by statuses {statuses}: {e}")
            return []

    # ========== MERCHANT (SELF) METHODS ==========

    async def get_merchant(self, merchant_id: str) -> Optional[Dict]:
        """
        Retrieve merchant data by BSON _id string.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            oid = ObjectId(merchant_id)
            merchant = await self.db.merchants.find_one({"_id": oid})
            if merchant:
                merchant["_id"] = str(merchant["_id"])
                merchant.pop("password_hash", None) # Remove hash
            return merchant
        except (InvalidId, TypeError):
             logger.error(f"Invalid merchant_id format for get_merchant: {merchant_id}")
             return None
        except Exception as e:
            logger.error(f"Error retrieving merchant {merchant_id}: {e}")
            return None

    async def update_merchant(self, merchant_id: str, merchant_data: Dict):
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            oid = ObjectId(merchant_id)
            merchant_data["updated_at"] = datetime.now(timezone.utc)
            if "password" in merchant_data: # Handle password change
                 from auth import hash_password
                 merchant_data["password_hash"] = hash_password(merchant_data.pop("password"))
                 
            await self.db.merchants.update_one(
                {"_id": oid},
                {"$set": merchant_data}
            )
        except Exception as e:
            logger.error(f"Error updating merchant: {e}", exc_info=True)
            raise

    # ========== PRODUCT / INVENTORY METHODS ==========

    async def get_product(self, merchant_id: str, product_id: str) -> Optional[Dict]:
        """
        Get product details by 'product_id' field or BSON '_id' string.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            product = None
            # 1. Try 'product_id' field
            if not isinstance(product_id, ObjectId):
                product = await self.db.products.find_one({
                    "merchant_id": merchant_id,
                    "product_id": product_id
                })
            # 2. Try BSON _id
            if product is None:
                try:
                    oid = ObjectId(product_id)
                    product = await self.db.products.find_one({
                        "merchant_id": merchant_id,
                        "_id": oid
                    })
                except (InvalidId, TypeError):
                     pass
            
            if product:
                product["_id"] = str(product["_id"])
            return product
        except Exception as e:
            logger.error(f"Error retrieving product: {e}")
            return None

    async def get_product_by_name(self, merchant_id: str, product_name: str) -> Optional[Dict]:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            normalized_name = (product_name or "").strip()
            product = await self.db.products.find_one({
                "merchant_id": merchant_id,
                "product_name": {"$regex": f"^{re.escape(normalized_name)}$", "$options": "i"}
            })
            if product: product["_id"] = str(product["_id"])
            return product
        except Exception as e:
            logger.error(f"Error finding product by name: {e}")
            return None

    async def get_product_by_sku(self, merchant_id: str, sku: str) -> Optional[Dict]:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            product = await self.db.products.find_one({
                "merchant_id": merchant_id,
                "sku": sku
            })
            if product: product["_id"] = str(product["_id"])
            return product
        except Exception as e:
            logger.error(f"Error finding product by sku: {e}")
            return None

    async def update_product_stock(self, merchant_id: str, product_id: str, new_stock: float):
        if self.db is None: raise RuntimeError("Database not initialized")
        if new_stock < 0: raise ValueError("Stock quantity cannot be negative")
        try:
            product_doc = await self.get_product(merchant_id, product_id) # Use robust finder
            if not product_doc:
                raise ValueError(f"No product found for merchant={merchant_id}, id={product_id}")
            
            actual_bson_id = ObjectId(product_doc["_id"])
            result = await self.db.products.update_one(
                {"merchant_id": merchant_id, "_id": actual_bson_id},
                {"$set": {"stock_qty": new_stock, "updated_at": datetime.now(timezone.utc)}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating product stock: {e}", exc_info=True)
            raise

    async def update_product(self, merchant_id: str, product_id: str, updates: Dict) -> bool:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            product_doc = await self.get_product(merchant_id, product_id)
            if not product_doc:
                raise ValueError(f"No product found for merchant={merchant_id}, id={product_id}")
            actual_bson_id = ObjectId(product_doc["_id"])
            
            if "updated_at" not in updates:
                updates["updated_at"] = datetime.now(timezone.utc)
                
            result = await self.db.products.update_one(
                {"merchant_id": merchant_id, "_id": actual_bson_id},
                {"$set": updates}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating product: {e}", exc_info=True)
            return False

    async def get_all_products(self, merchant_id: str, limit: int = 1000) -> List[Dict]:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            cursor = self.db.products.find({"merchant_id": merchant_id}).limit(limit)
            items = await cursor.to_list(length=limit)
            for doc in items: doc["_id"] = str(doc["_id"])
            return items
        except Exception as e:
            logger.error(f"Error retrieving all products: {e}")
            return []
            
    async def add_product(self, product_data: Dict) -> str:
        """
        Add a new product. Assumes 'product_data' is a complete doc.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            if "created_at" not in product_data:
                product_data["created_at"] = datetime.now(timezone.utc)
            if "updated_at" not in product_data:
                product_data["updated_at"] = datetime.now(timezone.utc)
                
            result = await self.db.products.insert_one(product_data)
            return str(result.inserted_id)
        except DuplicateKeyError:
            sku = product_data.get("sku")
            logger.warning(f"Duplicate product SKU: {sku}")
            raise ValueError(f"Product with SKU {sku} already exists")
        except Exception as e:
            logger.error(f"Error adding product: {e}", exc_info=True)
            raise
            
    async def upsert_product_compat(self, product_data: Dict[str, Any]) -> str:
        """
        Compatibility upsert based on SKU.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        merchant_id = product_data.get("merchant_id")
        sku = product_data.get("sku")
        if not merchant_id or not sku:
            raise ValueError("merchant_id and sku are required for upsert")
            
        try:
            product_data["updated_at"] = datetime.now(timezone.utc)
            result = await self.db.products.update_one(
                {"merchant_id": merchant_id, "sku": sku},
                {
                    "$set": product_data,
                    "$setOnInsert": {"created_at": product_data.get("created_at", datetime.now(timezone.utc))}
                },
                upsert=True
            )
            if result.upserted_id:
                return f"Created new product with ID: {result.upserted_id}"
            elif result.modified_count > 0:
                return f"Updated product with SKU: {sku}"
            else:
                return f"No changes to product with SKU: {sku}"
        except Exception as e:
            logger.error(f"Error upserting product: {e}")
            raise

    async def get_low_stock_products(self, merchant_id: str, threshold: float = 10) -> List[Dict]:
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            cursor = self.db.products.find({
                "merchant_id": merchant_id,
                "stock_qty": {"$lte": threshold}
            })
            items = await cursor.to_list(length=1000)
            for doc in items: doc["_id"] = str(doc["_id"])
            return items
        except Exception as e:
            logger.error(f"Error retrieving low stock products: {e}")
            return []

    # ========== DASHBOARD STATS METHODS ==========
    
    async def get_overview_stats(self, merchant_id: str) -> Dict[str, Any]:
        """
        Get overview statistics for merchant dashboard.
        """
        if self.db is None: raise RuntimeError("Database not initialized")
        try:
            total_messages = await self.db.messages.count_documents({"merchant_id": merchant_id})
            total_orders = await self.db.orders.count_documents({"merchant_id": merchant_id})
            pending_orders = await self.db.orders.count_documents({"merchant_id": merchant_id, "status": "pending"})
            total_products = await self.db.products.count_documents({"merchant_id": merchant_id})
            unique_customers = await self.db.messages.distinct("customer_phone", {"merchant_id": merchant_id})
            
            # Low stock
            m = await self.get_merchant(merchant_id)
            threshold = m.get("details", {}).get("low_stock_threshold", 10.0)
            low_stock_count = await self.db.products.count_documents({"merchant_id": merchant_id, "stock_qty": {"$lte": threshold}})

            return {
                "total_messages": total_messages,
                "total_orders": total_orders,
                "pending_orders": pending_orders,
                "total_products": total_products,
                "unique_customers": len(unique_customers),
                "low_stock_count": low_stock_count,
                "merchant_id": merchant_id
            }
        except Exception as e:
            logger.error(f"Error getting overview stats: {e}")
            return {}


# ============================================================
# GLOBAL INSTANCE & LIFECYCLE FUNCTIONS
# ============================================================

_db_instance: Optional[DatabaseV6] = None
_db_lock = asyncio.Lock()

async def get_db() -> DatabaseV6:
    """
    Get or create global database instance (async safe).
    """
    global _db_instance
    if _db_instance is None:
         async with _db_lock:
             if _db_instance is None:
                 _db_instance = DatabaseV6()
    return _db_instance

async def init_db() -> DatabaseV6:
    """
    Initialize database connection and indexes.
    Call from FastAPI startup event.
    """
    db = await get_db()
    if not db._initialized:
        await db.initialize()
    return db

async def close_db():
    """
    Close database connection.
    Call from FastAPI shutdown event.   
    """
    global _db_instance
    if _db_instance and _db_instance._initialized:
        await _db_instance.close()
        _db_instance = None


# ============================================================
# BACKWARD-COMPATIBILITY WRAPPER FUNCTIONS
# (For app.py to call)
# ============================================================

# --- Admin Functions ---

async def create_merchant(username: str, password: str, full_name: str, phone: str, details: Optional[Dict[str, Any]] = None) -> str:
    db = await get_db()
    return await db.create_merchant(username, password, full_name, phone, details)

async def get_merchant_by_username(username: str) -> Optional[Dict[str, Any]]:
    db = await get_db()
    return await db.get_merchant_by_username(username)

async def get_all_merchants() -> List[Dict[str, Any]]:
    db = await get_db()
    return await db.get_all_merchants()

async def delete_merchant_cascade(merchant_id: str):
    db = await get_db()
    return await db.delete_merchant_cascade(merchant_id)

async def get_system_wide_stats() -> Dict[str, Any]:
    db = await get_db()
    return await db.get_system_wide_stats()

async def log_admin_action(admin_username: str, action: str, details: Optional[Dict[str, Any]] = None):
    db = await get_db()
    return await db.log_admin_action(admin_username, action, details)

async def get_all_messages_admin(limit: int = 100) -> List[Dict[str, Any]]:
    db = await get_db()
    return await db.get_all_messages_admin(limit)

# --- Shared Functions ---

async def get_messages(filter_criteria: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
    db = await get_db()
    # The new class method is more specific, let's call it
    return await db.get_messages(filter_criteria, limit)

async def insert_message(message_data: Dict[str, Any]) -> str:
    db = await get_db()
    return await db.insert_message(message_data)

async def get_overview_stats(merchant_id: str) -> Dict[str, Any]:
    db = await get_db()
    return await db.get_overview_stats(merchant_id)

async def get_products(merchant_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
    db = await get_db()
    return await db.get_all_products(merchant_id, limit)

async def upsert_product(product_data: Dict[str, Any]) -> str:
    db = await get_db()
    # Use the compatibility upsert method
    return await db.upsert_product_compat(product_data)

