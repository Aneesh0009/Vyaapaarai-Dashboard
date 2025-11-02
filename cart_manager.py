# cart_manager.py - Shopping Cart Management for WhatsApp Orders
"""
Manages conversation-level shopping carts with TTL, persistence, and totals.
UPDATED: Fixed datetime deprecation, added validation, thread-safety, error handling
VERSION: 1.1.0
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class CartItem:
    """Single item in a shopping cart."""
    product_id: str
    product_name: str
    quantity: int
    unit_price: float
    unit: str = "piece"
    
    def __post_init__(self):
        """Validate CartItem on creation."""
        if not self.product_id or not isinstance(self.product_id, str):
            raise ValueError("product_id must be non-empty string")
        if not self.product_name or not isinstance(self.product_name, str):
            raise ValueError("product_name must be non-empty string")
        if not isinstance(self.quantity, int) or self.quantity <= 0:
            raise ValueError("quantity must be positive integer")
        if not isinstance(self.unit_price, (int, float)) or self.unit_price < 0:
            raise ValueError("unit_price must be non-negative number")
        if not self.unit or not isinstance(self.unit, str):
            raise ValueError("unit must be non-empty string")
    
    @property
    def subtotal(self) -> float:
        """Calculate item subtotal."""
        return self.quantity * self.unit_price
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for persistence."""
        return {
            "product_id": self.product_id,
            "product_name": self.product_name,
            "quantity": self.quantity,
            "unit_price": self.unit_price,
            "unit": self.unit,
            "subtotal": self.subtotal
        }


@dataclass
class Cart:
    """Shopping cart for a conversation."""
    conversation_id: str
    items: List[CartItem] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ttl_hours: int = 24
    
    @property
    def total(self) -> float:
        """Calculate total cart value."""
        return sum(item.subtotal for item in self.items)
    
    @property
    def item_count(self) -> int:
        """Get total number of items (by quantity)."""
        return sum(item.quantity for item in self.items)
    
    @property
    def unique_items(self) -> int:
        """Get number of unique products."""
        return len(self.items)
    
    @property
    def is_expired(self) -> bool:
        """Check if cart has expired based on TTL."""
        return datetime.now(timezone.utc) > self.created_at + timedelta(hours=self.ttl_hours)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for persistence."""
        return {
            "conversation_id": self.conversation_id,
            "items": [item.to_dict() for item in self.items],
            "total": self.total,
            "item_count": self.item_count,
            "unique_items": self.unique_items,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "ttl_hours": self.ttl_hours,
            "is_expired": self.is_expired
        }


class CartManager:
    """
    Manages shopping carts for WhatsApp conversations.
    Provides CRUD operations, persistence, and cart lifecycle management.
    """
    
    def __init__(self, db_module):
        """
        Initialize cart manager.
        
        Args:
            db_module: Reference to db.py for persistence.
                      Required methods:
                      - get_cart(conversation_id: str) -> Optional[Dict]
                      - upsert_cart(cart_dict: Dict) -> None
                      - delete_cart(conversation_id: str) -> None
        
        Raises:
            ValueError: If db_module doesn't have required methods
        """
        # Validate db_module has required methods
        required_methods = ["get_cart", "upsert_cart", "delete_cart"]
        for method in required_methods:
            if not hasattr(db_module, method) or not callable(getattr(db_module, method)):
                raise ValueError(f"db_module must have callable method: {method}")
        
        self.db = db_module
        self._carts: Dict[str, Cart] = {}  # In-memory cache
        self._cache_lock = asyncio.Lock()  # Thread-safety for concurrent access
        logger.info("CartManager initialized with thread-safe cache")
    
    async def get_or_create_cart(self, conversation_id: str) -> Cart:
        """
        Retrieve existing cart or create new one.
        Thread-safe with async lock protection.
        
        Args:
            conversation_id: Unique conversation identifier
            
        Returns:
            Cart instance
            
        Raises:
            ValueError: If conversation_id is invalid
        """
        if not conversation_id or not isinstance(conversation_id, str):
            raise ValueError("conversation_id must be non-empty string")
        
        async with self._cache_lock:
            # Check in-memory cache first
            if conversation_id in self._carts:
                cart = self._carts[conversation_id]
                if not cart.is_expired:
                    logger.debug(f"Retrieved cart from cache: {conversation_id}")
                    return cart
                else:
                    # Expired, remove from cache
                    del self._carts[conversation_id]
                    logger.info(f"Removed expired cart from cache: {conversation_id}")
            
            # Try to load from database
            try:
                cart_data = await self.db.get_cart(conversation_id)
                if cart_data:
                    cart = self._dict_to_cart(cart_data)
                    if not cart.is_expired:
                        self._carts[conversation_id] = cart
                        logger.debug(f"Retrieved cart from database: {conversation_id}")
                        return cart
                    else:
                        # Expired in DB, attempt to clear it
                        try:
                            await self.db.delete_cart(conversation_id)
                            logger.info(f"Deleted expired cart from database: {conversation_id}")
                        except Exception as e:
                            logger.error(f"Failed to delete expired cart {conversation_id}: {e}")
                            # Continue anyway; new cart will be created
            except Exception as e:
                logger.error(f"Error retrieving cart from database: {e}")
                # Continue with new cart creation
            
            # Create new cart
            cart = Cart(conversation_id=conversation_id)
            self._carts[conversation_id] = cart
            await self._persist_cart(cart)
            logger.info(f"Created new cart for conversation: {conversation_id}")
            return cart

    async def get_cart(self, conversation_id: str) -> Dict:
        """
        Retrieve the cart as a serializable dictionary.
        """
        cart = await self.get_or_create_cart(conversation_id)
        return cart.to_dict()
    
    async def add_item(self, conversation_id: str, product_id: str, 
                   product_name: str, quantity: float, unit_price: float, unit: str = "piece"):
        """
        Add or UPDATE item in cart.
        ✨ CRITICAL FIX: Checks if product already exists, UPDATES instead of duplicating
        """
        if not conversation_id or not isinstance(conversation_id, str):
            raise ValueError("conversation_id must be non-empty string")
        if not product_id or not isinstance(product_id, str):
            raise ValueError("product_id must be non-empty string")
        if not product_name or not isinstance(product_name, str):
            raise ValueError("product_name must be non-empty string")
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValueError("quantity must be positive integer")
        if not isinstance(unit_price, (int, float)) or unit_price < 0:
            raise ValueError("unit_price must be non-negative number")
        if not unit or not isinstance(unit, str):
            raise ValueError("unit must be non-empty string")
        cart = await self.get_or_create_cart(conversation_id)
        
        # ✅ Check if product already in cart
        item_found = False
        for item in cart.items:
            if item.product_id == product_id:
                # ✅ UPDATE existing item (NOT duplicate!)
                item.quantity = quantity  # Replace quantity
                # Keep price in sync if updated
                item.unit_price = unit_price
                item_found = True
                logger.info(f"Updated: {product_name} quantity to {quantity}")
                break
        
        if not item_found:
            # ✅ ADD new item (not in cart)
            cart.items.append(
                CartItem(
                    product_id=product_id,
                    product_name=product_name,
                    quantity=quantity,
                    unit_price=unit_price,
                    unit=unit,
                )
            )
            logger.info(f"Added: {product_name} ({quantity}{unit})")
        
        # Save to database
        await self._persist_cart(cart)
        
        return cart, not item_found
    
    async def remove_item(
        self,
        conversation_id: str,
        product_id: str
    ) -> Optional[Cart]:
        """
        Remove item from cart by product ID.
        
        Args:
            conversation_id: Conversation identifier
            product_id: Product to remove
        
        Returns:
            Updated cart or None if not found
        """
        if not conversation_id or not product_id:
            raise ValueError("conversation_id and product_id must be non-empty")
        
        cart = await self.get_or_create_cart(conversation_id)
        
        # Find and remove item
        removed_item = None
        cart.items = [item for item in cart.items if item.product_id != product_id]
        
        # Check if item was actually removed
        if len(cart.items) != len([i for i in cart.items if i.product_id != product_id]):
            cart.updated_at = datetime.now(timezone.utc)
            await self._persist_cart(cart)
            logger.info(f"Removed product {product_id} from cart {conversation_id}")
        else:
            logger.warning(f"Product {product_id} not found in cart {conversation_id}")
        
        return cart
    
    async def update_quantity(
        self,
        conversation_id: str,
        product_id: str,
        new_quantity: int
    ) -> Optional[Cart]:
        """
        Update item quantity in cart.
        
        Args:
            conversation_id: Conversation identifier
            product_id: Product to update
            new_quantity: New quantity (must be positive, or item is removed)
        
        Returns:
            Updated cart
            
        Raises:
            ValueError: If new_quantity is negative
        """
        if not isinstance(new_quantity, int):
            raise ValueError("new_quantity must be integer")
        
        if new_quantity < 0:
            raise ValueError("new_quantity cannot be negative")
        
        if new_quantity == 0:
            return await self.remove_item(conversation_id, product_id)
        
        cart = await self.get_or_create_cart(conversation_id)
        
        for item in cart.items:
            if item.product_id == product_id:
                old_qty = item.quantity
                item.quantity = new_quantity
                cart.updated_at = datetime.now(timezone.utc)
                await self._persist_cart(cart)
                logger.info(
                    f"Updated quantity for {item.product_name}: {old_qty} → {new_quantity}"
                )
                return cart
        
        logger.warning(f"Product {product_id} not found in cart {conversation_id}")
        return cart
    
    async def clear_cart(self, conversation_id: str) -> None:
        """
        Clear all items from cart and remove from database.
        
        Args:
            conversation_id: Conversation identifier
        """
        if not conversation_id:
            raise ValueError("conversation_id must be non-empty")
        
        async with self._cache_lock:
            if conversation_id in self._carts:
                del self._carts[conversation_id]
        
        try:
            await self.db.delete_cart(conversation_id)
            logger.info(f"Cleared cart for conversation: {conversation_id}")
        except Exception as e:
            logger.error(f"Error clearing cart {conversation_id}: {e}")
            raise
    
    async def get_cart_summary(self, conversation_id: str) -> str:
        """
        Generate human-readable cart summary for WhatsApp.
        Uses plain text formatting compatible with WhatsApp Cloud API.
        
        Args:
            conversation_id: Conversation identifier
        
        Returns:
            Formatted string with cart contents and total
        """
        if not conversation_id:
            raise ValueError("conversation_id must be non-empty")
        
        cart = await self.get_or_create_cart(conversation_id)
        
        if not cart.items:
            return "🛒 Your cart is empty."
        
        lines = ["🛒 YOUR CART", "=" * 50]
        
        for idx, item in enumerate(cart.items, 1):
            lines.append(f"\n{idx}. {item.product_name}")
            lines.append(
                f"   Qty: {item.quantity} {item.unit} @ ₹{item.unit_price:.2f} each"
            )
            lines.append(f"   Subtotal: ₹{item.subtotal:.2f}")
        
        lines.append("\n" + "=" * 50)
        lines.append(f"TOTAL: ₹{cart.total:.2f}")
        lines.append(f"Items: {cart.item_count} ({cart.unique_items} products)")
        lines.append("=" * 50)
        
        return "\n".join(lines)
    
    async def validate_cart_with_inventory(
        self,
        conversation_id: str,
        inventory_manager,
        merchant_id: str,
        timeout_seconds: int = 5
    ) -> Tuple[bool, List[str]]:
        """
        Validate cart items against current inventory.
        Checks stock availability and handles timeouts gracefully.
        
        Args:
            conversation_id: Conversation identifier
            inventory_manager: Reference to inventory manager module
            merchant_id: Merchant identifier for inventory query
            timeout_seconds: Timeout for each inventory check (default: 5)
        
        Returns:
            Tuple of (is_valid, list_of_issue_messages)
        """
        if not conversation_id or not merchant_id:
            raise ValueError("conversation_id and merchant_id must be non-empty")
        
        if not hasattr(inventory_manager, 'get_product_stock'):
            raise ValueError("inventory_manager must have get_product_stock() method")
        
        cart = await self.get_or_create_cart(conversation_id)
        issues = []
        
        for item in cart.items:
            try:
                # Query stock with timeout
                stock = await asyncio.wait_for(
                    inventory_manager.get_product_stock(merchant_id, item.product_id),
                    timeout=timeout_seconds
                )
                
                if stock is None:
                    issues.append(f"❌ {item.product_name} is no longer available")
                    logger.warning(f"Product {item.product_id} not found in inventory")
                elif stock < item.quantity:
                    issues.append(
                        f"⚠️ {item.product_name}: only {stock} {item.unit} available "
                        f"(you requested {item.quantity})"
                    )
                    logger.warning(
                        f"Insufficient stock for {item.product_id}: "
                        f"requested {item.quantity}, available {stock}"
                    )
            except asyncio.TimeoutError:
                issues.append(
                    f"⏱️ {item.product_name}: could not verify availability (timeout)"
                )
                logger.error(f"Timeout checking stock for {item.product_id}")
            except Exception as e:
                issues.append(f"❓ {item.product_name}: could not verify availability")
                logger.error(f"Error checking stock for {item.product_id}: {e}")
        
        is_valid = len(issues) == 0
        return is_valid, issues
    
    async def create_reorder_cart(
        self,
        conversation_id: str,
        order_data: Dict
    ) -> Cart:
        """
        Create cart from previous order for quick reorder.
        Validates order structure before adding items.
        
        Args:
            conversation_id: Current conversation ID
            order_data: Previous order dictionary with items array
        
        Returns:
            Newly created cart
            
        Raises:
            ValueError: If order_data structure is invalid
        """
        if not conversation_id:
            raise ValueError("conversation_id must be non-empty")
        
        if not isinstance(order_data, dict):
            raise ValueError("order_data must be dictionary")
        
        # Clear existing cart
        await self.clear_cart(conversation_id)
        
        # Add items from order with validation
        items_added = 0
        for idx, item_data in enumerate(order_data.get("items", [])):
            # Validate required keys
            required_keys = ["product_id", "product_name", "quantity", "unit_price"]
            missing_keys = [k for k in required_keys if k not in item_data]
            
            if missing_keys:
                logger.warning(
                    f"Skipping reorder item {idx}: missing keys {missing_keys}"
                )
                continue
            
            try:
                # Convert and validate types
                await self.add_item(
                    conversation_id=conversation_id,
                    product_id=str(item_data["product_id"]),
                    product_name=str(item_data["product_name"]),
                    quantity=int(item_data["quantity"]),
                    unit_price=float(item_data["unit_price"]),
                    unit=str(item_data.get("unit", "piece"))
                )
                items_added += 1
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(f"Failed to add reorder item {idx}: {e}")
                continue
        
        logger.info(
            f"Created reorder cart from order {order_data.get('order_id')} "
            f"with {items_added} items for conversation {conversation_id}"
        )
        
        return await self.get_or_create_cart(conversation_id)
    
    async def get_cart_from_history(
        self,
        conversation_id: str,
        max_items: int = 5
    ) -> Dict[str, List[Dict]]:
        """
        Get customer's order history and frequently ordered items.
        Useful for "Recently Ordered" suggestions.
        
        Args:
            conversation_id: Conversation identifier
            max_items: Maximum number of past orders to retrieve
        
        Returns:
            Dictionary with 'orders' and 'frequent_items' keys
        """
        if not conversation_id:
            raise ValueError("conversation_id must be non-empty")
        
        try:
            # Try to get order history from database
            # This assumes db.py has get_customer_order_history() method
            if hasattr(self.db, 'get_customer_order_history'):
                history = await self.db.get_customer_order_history(
                    conversation_id,
                    limit=max_items
                )
                
                # Extract frequently ordered items
                item_frequency = {}
                for order in history:
                    for item in order.get("items", []):
                        pid = item["product_id"]
                        if pid not in item_frequency:
                            item_frequency[pid] = {"count": 0, "data": item}
                        item_frequency[pid]["count"] += 1
                
                # Sort by frequency
                frequent_items = sorted(
                    item_frequency.values(),
                    key=lambda x: x["count"],
                    reverse=True
                )
                
                logger.info(f"Retrieved {len(history)} orders for {conversation_id}")
                
                return {
                    "orders": history,
                    "frequent_items": [item["data"] for item in frequent_items[:3]]
                }
            else:
                logger.warning("db.py doesn't have get_customer_order_history method")
                return {"orders": [], "frequent_items": []}
        
        except Exception as e:
            logger.error(f"Error retrieving cart history: {e}")
            return {"orders": [], "frequent_items": []}
    
    def _dict_to_cart(self, cart_data: Dict) -> Cart:
        """
        Convert database dict to Cart object.
        Handles datetime parsing with fallback.
        
        Args:
            cart_data: Cart dictionary from database
        
        Returns:
            Cart instance
            
        Raises:
            ValueError: If cart_data structure is invalid
        """
        if not isinstance(cart_data, dict):
            raise ValueError("cart_data must be dictionary")
        
        if "conversation_id" not in cart_data:
            raise ValueError("cart_data must have conversation_id")
        
        try:
            # Parse items with validation
            items = []
            for item_data in cart_data.get("items", []):
                try:
                    # Filter out unexpected fields (e.g., 'subtotal') before creating CartItem
                    allowed_keys = {"product_id", "product_name", "quantity", "unit_price", "unit"}
                    filtered_item = {k: item_data[k] for k in allowed_keys if k in item_data}
                    items.append(CartItem(**filtered_item))
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping invalid cart item: {e}")
                    continue
            
            # Parse timestamps with error handling (supports datetime or ISO strings)
            try:
                raw_created = cart_data.get("created_at")
                if isinstance(raw_created, datetime):
                    created_at = raw_created if raw_created.tzinfo else raw_created.replace(tzinfo=timezone.utc)
                else:
                    created_at = datetime.fromisoformat(raw_created)
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                logger.warning("Invalid created_at timestamp, using current time")
                created_at = datetime.now(timezone.utc)

            try:
                raw_updated = cart_data.get("updated_at")
                if isinstance(raw_updated, datetime):
                    updated_at = raw_updated if raw_updated.tzinfo else raw_updated.replace(tzinfo=timezone.utc)
                else:
                    updated_at = datetime.fromisoformat(raw_updated)
                    if updated_at.tzinfo is None:
                        updated_at = updated_at.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                logger.warning("Invalid updated_at timestamp, using current time")
                updated_at = datetime.now(timezone.utc)
            
            return Cart(
                conversation_id=cart_data["conversation_id"],
                items=items,
                created_at=created_at,
                updated_at=updated_at,
                ttl_hours=cart_data.get("ttl_hours", 24)
            )
        
        except Exception as e:
            logger.error(f"Error converting cart data: {e}")
            raise ValueError(f"Invalid cart data structure: {e}")
    
    async def _persist_cart(self, cart: Cart) -> None:
        """
        Save cart to database.
        
        Args:
            cart: Cart instance to persist
            
        Raises:
            Exception: If database operation fails
        """
        try:
            await self.db.upsert_cart(cart.to_dict())
        except Exception as e:
            logger.error(f"Failed to persist cart: {e}")
            raise

    async def add_item_safe(self, conversation_id: str, item):
        """
        Add item safely (handles both dict and CartItem objects)
        Converts CartItem → dict automatically to prevent .get() errors
        """
        try:
            # Convert incoming item to a normalized dict
            if hasattr(item, 'to_dict') and callable(getattr(item, 'to_dict')):
                item_dict = item.to_dict()
            elif isinstance(item, dict):
                item_dict = dict(item)
            else:
                item_dict = {
                    "product_id": getattr(item, "product_id", None),
                    "product_name": getattr(item, "product_name", None),
                    "quantity": getattr(item, "quantity", 0),
                    "unit": getattr(item, "unit", "piece"),
                    "unit_price": getattr(item, "unit_price", 0),
                }

            # Validate minimal required fields
            if not item_dict.get("product_id") or not item_dict.get("product_name"):
                raise ValueError("item must include product_id and product_name")

            # Get or create cart
            cart = await self.get_or_create_cart(conversation_id)
            if not cart.items:
                cart.items = []

            # Try update existing item (cart stores CartItem objects)
            found = False
            for existing_item in cart.items:
                existing_pid = getattr(existing_item, "product_id", None)
                if existing_pid == item_dict.get("product_id"):
                    # Update existing CartItem
                    existing_item.quantity = int(item_dict.get("quantity", existing_item.quantity))
                    existing_item.unit_price = float(item_dict.get("unit_price", existing_item.unit_price))
                    if hasattr(existing_item, "unit") and item_dict.get("unit"):
                        existing_item.unit = str(item_dict.get("unit"))
                    logger.info(
                        f"Updated cart: {item_dict.get('product_name')} → {existing_item.quantity}"
                    )
                    found = True
                    break

            # If not found, append as CartItem
            if not found:
                # Filter keys for CartItem constructor
                allowed_keys = {"product_id", "product_name", "quantity", "unit_price", "unit"}
                filtered = {k: item_dict[k] for k in allowed_keys if k in item_dict}
                # Type coercion
                filtered["quantity"] = int(filtered.get("quantity", 0))
                filtered["unit_price"] = float(filtered.get("unit_price", 0))
                if not filtered.get("unit"):
                    filtered["unit"] = "piece"
                cart.items.append(CartItem(**filtered))
                logger.info(f"Added to cart: {item_dict.get('product_name')}")

            # Persist
            await self._persist_cart(cart)
            return cart

        except Exception as e:
            logger.error(f"Add item safe error: {e}", exc_info=True)
            raise


# Singleton instance getter (optional, for easy access)
_cart_manager_instance: Optional[CartManager] = None


async def get_cart_manager(db_module) -> CartManager:
    """
    Get or create CartManager singleton instance.
    
    Args:
        db_module: Database module for persistence
    
    Returns:
        CartManager instance
    """
    global _cart_manager_instance
    if _cart_manager_instance is None:
        _cart_manager_instance = CartManager(db_module)
    return _cart_manager_instance
