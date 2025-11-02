# inventory_manager_v6.py
"""
Unified, role-aware inventory management (v6).
Combines v5's merchant-facing atomic operations with v4's admin controls.
Handles atomic stock updates, batch rollbacks, admin adjustments, and analytics.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import uuid
import re # re was in v5, keeping it just in case, though not used in this merge

# Use the new v6 logger name as specified
logger = logging.getLogger("inventory_manager_v6")

class InventoryManagerV6:
    """
    Manages product inventory with atomic stock operations for both Merchants and Admins.
    Provides thread-safe stock updates, validation, admin correction, and analytics.
    """
    def __init__(self, db_instance):
        """
        Initialize the unified inventory manager.
        """
        self.db = db_instance
        self._locks: Dict[str, asyncio.Lock] = {}
        # Use the new init log message from the v6 spec
        logger.info("InventoryManagerV6 initialized (Unified Admin + Merchant)")

    async def _get_lock(self, lock_key: str) -> asyncio.Lock:
        """Get or create lock for lock_key in a thread-safe way."""
        if lock_key not in self._locks:
            self._locks[lock_key] = asyncio.Lock()
        return self._locks[lock_key]

    # --------------------------------------------------
    # Merchant Read Methods (Preserved from v5)
    # --------------------------------------------------

    async def get_product(self, merchant_id: str, product_id: str) -> Optional[Dict]:
        """Retrieve product details using the db instance."""
        if not self.db or not self.db._initialized:
             logger.error("DB not initialized in InventoryManager for get_product")
             return None
        return await self.db.get_product(merchant_id, product_id)

    async def get_product_by_name(self, merchant_id: str, product_name: str) -> Optional[Dict]:
        """Find product by name (case-insensitive) using the db instance."""
        if not self.db or not self.db._initialized: return None
        return await self.db.get_product_by_name(merchant_id, product_name)

    async def get_product_by_sku(self, merchant_id: str, sku: str) -> Optional[Dict]:
        """Find product by SKU via db layer."""
        if not self.db or not self.db._initialized: return None
        if not hasattr(self.db, "get_product_by_sku"):
             logger.error("DB layer does not support get_product_by_sku"); return None
        return await self.db.get_product_by_sku(merchant_id, sku)

    async def get_inventory(self, merchant_id: str) -> List[Dict]:
        """Get all inventory items for a merchant."""
        if not self.db or not self.db._initialized: return []
        return await self.db.get_all_products(merchant_id)

    async def get_product_stock(self, merchant_id: str, product_id: str) -> Optional[float]:
        """Get current stock level for a product (read-only)."""
        product = await self.get_product(merchant_id, product_id)
        stock = product.get("stock_qty") if product else None
        try:
            return float(stock) if stock is not None else None
        except (ValueError, TypeError):
             logger.warning(f"Non-numeric stock value for {product_id}: {stock}")
             return None

    # --------------------------------------------------
    # Unified Atomic Write Methods (Merged v5 Logic + v6 Role)
    # --------------------------------------------------

    async def update_quantity(
        self,
        merchant_id: str,
        product_id: str,
        quantity_change: float,
        change_reason: str = "order_update",
        role: str = "merchant" # New role parameter from v6 spec
    ) -> bool:
        """
        Atomically update stock quantity (positive or negative change).
        This is the *only* method that should hold a lock and write to stock.
        Uses v5's read-after-write verification.
        """
        if quantity_change == 0:
            logger.info(f"[{role}] Quantity change is zero for {product_id}, no update needed.")
            return True

        lock_key = f"{merchant_id}:{product_id}"
        lock = await self._get_lock(lock_key)

        async with lock: # Acquire lock for this specific product
            try:
                # 1. READ
                product = await self.get_product(merchant_id, product_id)
                if not product:
                    logger.error(f"[{role}] Product {product_id} not found (inside lock) for quantity update.")
                    return False

                current_stock = float(product.get("stock_qty", 0))
                new_stock = current_stock + quantity_change
                final_stock = max(new_stock, 0.0) # Ensure non-negative

                # 2. VALIDATE
                if quantity_change < 0 and new_stock < 0:
                    logger.warning(
                        f"[{role}] Insufficient stock for {product_id}: requested change {quantity_change}, "
                        f"available {current_stock}. Update aborted."
                    )
                    return False # Failure

                # 3. COMMIT (WRITE)
                await self.db.update_product_stock(merchant_id, product_id, final_stock)

                # 4. VERIFY (READ AGAIN *inside lock* - Preserved from v5)
                verified_product = await self.get_product(merchant_id, product_id)
                verified_stock = float(verified_product.get("stock_qty", -999))

                if abs(verified_stock - final_stock) > 1e-6:
                     logger.critical(f"[{role}] DB COMMIT FAILURE for {product_id}: Expected {final_stock}, found {verified_stock}. Aborting.")
                     # We might attempt a rollback write here, but for now, logging critical is essential.
                     return False

                # 5. LOG (after success)
                # Use the new v6 _log_stock_movement signature
                await self._log_stock_movement(
                    merchant_id, product_id, product.get("product_name"),
                    change_reason, quantity_change, current_stock, final_stock, role
                )
                logger.info(
                    f"[{role}] Updated quantity for {product_id} ({change_reason}): {quantity_change:+.2f}. "
                    f"Stock: {current_stock} -> {final_stock}"
                )
                return True # Success

            except Exception as e:
                 logger.error(f"[{role}] Failed to update stock for {product_id} (inside lock): {e}", exc_info=True)
                 return False

    async def deduct_stock(self, merchant_id: str, product_id: str, quantity: float, role: str = "merchant") -> bool:
        """Atomically deduct stock. Calls the main update_quantity method."""
        if quantity < 0: quantity = abs(quantity)
        elif quantity == 0: return True
        return await self.update_quantity(merchant_id, product_id, -quantity, change_reason="deduction", role=role)

    async def return_stock(self, merchant_id: str, product_id: str, quantity: float, role: str = "merchant") -> bool:
        """Return stock. Calls the main update_quantity method."""
        if quantity < 0: quantity = abs(quantity)
        elif quantity == 0: return True
        return await self.update_quantity(merchant_id, product_id, quantity, change_reason="return", role=role)

    # --------------------------------------------------
    # Admin Methods (Integrated from v4/v6 Spec)
    # --------------------------------------------------

    async def adjust_stock_admin(
        self, merchant_id: str, product_id: str, new_stock: float, admin_user: str, reason: str = "manual_adjustment"
    ) -> bool:
        """Admin manual stock correction with audit log. (From v6 spec)."""
        # This bypasses the relative change and sets an absolute value
        # We still use the lock to prevent conflicts with simultaneous orders
        lock_key = f"{merchant_id}:{product_id}"
        lock = await self._get_lock(lock_key)

        async with lock:
            try:
                product = await self.get_product(merchant_id, product_id)
                if not product:
                    logger.warning(f"[admin] Admin {admin_user}: Product {product_id} not found.")
                    return False

                old_stock = float(product.get("stock_qty", 0))
                final_new_stock = max(new_stock, 0.0) # Ensure non-negative

                # 1. COMMIT
                await self.db.update_product_stock(merchant_id, product_id, final_new_stock)

                # 2. VERIFY
                verified_product = await self.get_product(merchant_id, product_id)
                verified_stock = float(verified_product.get("stock_qty", -999))
                if abs(verified_stock - final_new_stock) > 1e-6:
                     logger.critical(f"[admin] DB COMMIT FAILURE for {product_id} (Admin Adjust): Expected {final_new_stock}, found {verified_stock}. Aborting.")
                     return False

                # 3. LOG MOVEMENT
                await self._log_stock_movement(
                    merchant_id, product_id, product.get("product_name"), reason,
                    final_new_stock - old_stock, old_stock, final_new_stock, "admin"
                )

                # 4. LOG ADMIN ACTION (Audit Trail)
                if hasattr(self.db, "record_admin_action"):
                    await self.db.record_admin_action(admin_user, reason, {
                        "merchant_id": merchant_id,
                        "product_id": product_id,
                        "old_stock": old_stock,
                        "new_stock": final_new_stock,
                        "admin_user": admin_user
                    })
                else:
                    logger.warning(f"[admin] DB layer missing 'record_admin_action' method. Audit log skipped.")

                logger.info(f"[admin] Admin {admin_user} adjusted {product_id} from {old_stock} -> {final_new_stock}")
                return True
            except Exception as e:
                logger.error(f"[admin] Admin stock adjustment failed: {e}", exc_info=True)
                return False

    async def sync_all_merchants_inventory(self, admin_user: str = "system") -> bool:
        """Sync or validate inventory for all merchants. (From v6 spec)."""
        logger.info(f"[admin] Admin {admin_user} triggered global inventory sync.")
        try:
            merchants = await self.db.get_all_merchants()
            if not merchants:
                logger.warning("[admin] No merchants found for sync.")
                return False
                
            tasks = []
            for m in merchants:
                merchant_id = m.get("merchant_id")
                if merchant_id:
                    tasks.append(self.get_inventory(merchant_id))
            
            all_inventories = await asyncio.gather(*tasks, return_exceptions=True)
            
            total_products = 0
            for i, result in enumerate(all_inventories):
                merchant_id = merchants[i].get("merchant_id")
                if isinstance(result, list):
                    logger.info(f"[admin] Synced {len(result)} products for merchant {merchant_id}")
                    total_products += len(result)
                else:
                    logger.error(f"[admin] Failed to sync inventory for merchant {merchant_id}: {result}")
            
            logger.info(f"[admin] Global sync complete. Checked {len(merchants)} merchants and {total_products} total products.")
            return True
        except Exception as e:
            logger.error(f"[admin] Global inventory sync failed: {e}", exc_info=True)
            return False

    # --------------------------------------------------
    # Batch & Validation Methods (Preserved from v5, now role-aware)
    # --------------------------------------------------

    async def validate_order_stock(
        self, merchant_id: str, items: List[Dict]
    ) -> Tuple[bool, List[str]]:
        """Validate stock availability for all items in an order (read-only). (From v5)"""
        issues = []
        if not items: return True, []

        async def check_item(item):
            product_id = item.get("product_id")
            required_quantity_str = item.get("quantity")
            if not product_id or required_quantity_str is None: return f"Invalid item data: {item}"
            try:
                required_quantity = float(required_quantity_str)
                if required_quantity <= 0: return f"Invalid quantity for {item.get('product_name', product_id)}: {required_quantity}"
            except (ValueError, TypeError): return f"Non-numeric quantity for {item.get('product_name', product_id)}: {required_quantity_str}"

            product = await self.get_product(merchant_id, product_id)
            if not product: return f"Product '{item.get('product_name', product_id)}' not found"

            current_stock = float(product.get("stock_qty", 0))
            if current_stock < required_quantity:
                return (f"{product.get('product_name', product_id)}: only {current_stock} "
                        f"{product.get('unit', 'units')} available (requested {required_quantity})")
            return None

        check_tasks = [check_item(item) for item in items]
        results = await asyncio.gather(*check_tasks)
        issues = [res for res in results if res is not None]
        return len(issues) == 0, issues

    async def batch_deduct_stock(
        self, merchant_id: str, items: List[Dict], role: str = "merchant"
    ) -> Tuple[bool, List[Dict]]:
        """
        Deduct stock for multiple items with rollback on failure. (From v5)
        Now includes 'role' parameter as per v6 spec.
        """
        results = []
        successful_deductions_info = [] # List of (product_id, quantity) for rollback
        failure_reasons = []

        if not items:
            return True, []

        # --- Phase 1: Attempt Deductions Sequentially ---
        for item in items:
            product_id = item.get("product_id")
            product_name = item.get("product_name", product_id)
            quantity = 0.0

            try:
                quantity_str = item.get("quantity")
                if quantity_str is None: raise ValueError("missing quantity")
                quantity = float(quantity_str)
                if quantity < 0: raise ValueError("Negative quantity")

                if quantity == 0:
                    results.append({"product_id": product_id, "success": True, "reason": "Zero quantity"})
                    continue

                # Call the ATOMIC, role-aware update_quantity method
                success = await self.update_quantity(
                    merchant_id=merchant_id,
                    product_id=product_id,
                    quantity_change=-quantity, # Deduct
                    change_reason="batch_deduction",
                    role=role # Pass the role
                )

                if success:
                    results.append({"product_id": product_id, "success": True})
                    successful_deductions_info.append({"product_id": product_id, "quantity": quantity, "product_name": product_name})
                else:
                    reason = f"Insufficient stock for {product_name}"
                    current_stock = await self.get_product_stock(merchant_id, product_id)
                    if current_stock is not None:
                         reason = f"Insufficient stock for {product_name}: needed {quantity}, have {current_stock}"

                    results.append({"product_id": product_id, "success": False, "reason": reason})
                    failure_reasons.append(reason)
                    break # Stop on first failure

            except (ValueError, TypeError) as e:
                reason = f"Invalid item data for {product_name}: {e}"
                results.append({"product_id": product_id, "success": False, "reason": reason})
                failure_reasons.append(reason)
                break
            except Exception as e:
                 reason = f"Unexpected error deducting {product_name}: {e}"
                 results.append({"product_id": product_id, "success": False, "reason": reason})
                 failure_reasons.append(reason)
                 break

        # --- Phase 2: Rollback if any part failed ---
        if failure_reasons:
            logger.warning(f"[{role}] Batch deduction failed for {merchant_id}. Rolling back {len(successful_deductions_info)} items. Reasons: {failure_reasons}")

            rollback_tasks = []
            for deduction_info in successful_deductions_info:
                # Add back the quantity that was successfully deducted
                rollback_tasks.append(
                    self.update_quantity(
                        merchant_id=merchant_id,
                        product_id=deduction_info["product_id"],
                        quantity_change=deduction_info["quantity"], # Add back positive quantity
                        change_reason="batch_deduction_rollback",
                        role=role # Pass the role
                    )
                )

            rollback_results = await asyncio.gather(*rollback_tasks, return_exceptions=True)

            for i, res in enumerate(rollback_results):
                 if res is not True:
                      pid = successful_deductions_info[i]["product_id"]
                      logger.critical(f"[{role}] CRITICAL: ROLLBACK FAILED for product {pid}. Manual intervention required. Error: {res}")

            processed_ids = {r["product_id"] for r in results}
            for item in items:
                 if item.get("product_id") not in processed_ids:
                      results.append({"product_id": item.get("product_id"), "success": False, "reason": "Batch stopped"})

            return False, results # Return False to OrderManager

        # --- Phase 3: Success ---
        logger.info(f"[{role}] Batch deduction successful for {len(successful_deductions_info)} items for {merchant_id}.")
        return True, results

    # --------------------------------------------------
    # Product Management & Low Stock (Preserved from v5)
    # --------------------------------------------------

    async def get_low_stock_products(
        self, merchant_id: str, threshold: Optional[float] = None
    ) -> List[Dict]:
        """Get products below stock threshold using db layer."""
        if not self.db or not self.db._initialized:
             logger.error("DB not initialized for get_low_stock_products"); return []
        effective_threshold = threshold
        if effective_threshold is None:
            try:
                merchant_data = await self.db.get_merchant(merchant_id)
                effective_threshold = float(merchant_data.get("low_stock_threshold", 10)) if merchant_data else 10.0
            except Exception as e:
                 logger.warning(f"Could not fetch merchant {merchant_id} for threshold: {e}"); effective_threshold = 10.0
        return await self.db.get_low_stock_products(merchant_id, effective_threshold)

    async def update_product(
        self, merchant_id: str, product_id: str, updates: Dict
    ) -> bool:
        """Update product details (price, name, unit, etc.) via DB abstraction. (From v5)"""
        if not self.db or not self.db._initialized:
             logger.error("DB not initialized for update_product"); return False
        try:
            safe_updates = updates.copy()
            safe_updates.pop("merchant_id", None); safe_updates.pop("product_id", None)
            safe_updates.pop("created_at", None)
            # CRITICAL: Do not update stock fields here. Use update_quantity or adjust_stock_admin
            safe_updates.pop("quantity", None); safe_updates.pop("stock_qty", None); safe_updates.pop("stock", None)
            safe_updates["updated_at"] = datetime.now(timezone.utc)

            if not hasattr(self.db, "update_product"):
                 logger.error("DB layer missing update_product method"); return False

            success = await self.db.update_product(merchant_id, product_id, safe_updates)
            if success:
                logger.info(f"Updated product details for {product_id} for {merchant_id}")
                return True
            else:
                logger.warning(f"No product updated or found for {product_id} with updates {safe_updates}")
                return False
        except Exception as e:
            logger.error(f"Error updating product {product_id}: {e}", exc_info=True)
            return False

    async def add_product(
        self, merchant_id: str, product_data: Dict
    ) -> Optional[str]:
        """Add new product to inventory (validates inputs). (From v5)"""
        if not self.db or not self.db._initialized:
             logger.error("DB not initialized for add_product"); return None
        try:
            if not product_data.get("product_name"): raise ValueError("Missing product_name")
            if product_data.get("price") is None: raise ValueError("Missing price")
            price = float(product_data["price"]); assert price >= 0
            stock = float(product_data.get("stock_qty", product_data.get("stock", 0)))
            assert stock >= 0

            product_id = product_data.get("product_id") or f"prod_{uuid.uuid4().hex}"
            now = datetime.now(timezone.utc)
            product = {
                "merchant_id": merchant_id,
                "product_id": product_id,
                "sku": product_data.get("sku"),
                "product_name": str(product_data["product_name"]).strip(),
                "price": price,
                "stock_qty": stock, # Standardized to stock_qty
                "unit": product_data.get("unit", "piece"),
                "category": product_data.get("category"),
                "description": product_data.get("description"),
                "reorder_level": float(product_data.get("reorder_level", max(stock * 0.2, 1.0))),
                "created_at": now,
                "updated_at": now
            }
            if not hasattr(self.db, "add_product"):
                 logger.error("DB layer missing add_product method"); return None

            await self.db.add_product(product)
            logger.info(f"Added new product {product_id} ('{product.get('product_name')}') for {merchant_id}")
            return product_id
        except (ValueError, AssertionError) as ve:
             logger.error(f"Validation error adding product: {ve}"); raise
        except Exception as e:
            logger.error(f"Error adding product for {merchant_id}: {e}", exc_info=True); return None

    # --------------------------------------------------
    # Shared Utilities & Analytics (Merged v5/v6)
    # --------------------------------------------------

    async def get_inventory_stats(self, merchant_id: Optional[str] = None) -> Dict:
        """Get inventory stats for one or all merchants. (From v6 spec)"""
        if merchant_id:
            try:
                items = await self.get_inventory(merchant_id)
                total_value = sum(float(i.get("price", 0)) * float(i.get("stock_qty", 0)) for i in items if i.get("price") is not None and i.get("stock_qty") is not None)
                low_stock_items = [i for i in items if float(i.get("stock_qty", 0)) < float(i.get("reorder_level", 0))]
                return {
                    "merchant_id": merchant_id,
                    "product_count": len(items),
                    "total_units": sum(float(i.get("stock_qty", 0)) for i in items),
                    "total_value": total_value,
                    "low_stock_count": len(low_stock_items)
                }
            except Exception as e:
                logger.error(f"[stats] Failed to get stats for {merchant_id}: {e}")
                return {"merchant_id": merchant_id, "error": str(e)}
        else:
            # Admin-level: Get stats for all merchants
            logger.info("[stats] Calculating global inventory stats...")
            merchants = await self.db.get_all_merchants()
            if not merchants: return {"summary": [], "global_value": 0, "global_products": 0}
            
            tasks = [self.get_inventory_stats(m.get("merchant_id")) for m in merchants if m.get("merchant_id")]
            summaries = await asyncio.gather(*tasks, return_exceptions=True)
            
            valid_summaries = [s for s in summaries if isinstance(s, dict) and "error" not in s]
            global_value = sum(s.get("total_value", 0) for s in valid_summaries)
            global_products = sum(s.get("product_count", 0) for s in valid_summaries)
            
            return {
                "summary_by_merchant": summaries,
                "global_total_value": global_value,
                "global_product_count": global_products,
                "merchant_count": len(summaries)
            }

    async def _log_stock_movement(
        self, merchant_id: str, product_id: str, product_name: Optional[str],
        movement_type: str, quantity_change: float, old_stock: float, new_stock: float,
        role: str # New field from v6 spec
    ):
        """Log stock movement using the unified v6 signature."""
        if not self.db or not self.db._initialized:
             logger.error("DB not initialized, cannot log stock movement."); return
        try:
            movement = {
                "merchant_id": merchant_id,
                "product_id": product_id,
                "product_name": product_name,
                "movement_type": movement_type,
                "quantity_change": quantity_change,
                "old_stock": old_stock,
                "new_stock": new_stock,
                "role": role, # Add the role to the log document
                "timestamp": datetime.now(timezone.utc)
            }
            movements_collection = await self.db.get_collection("stock_movements")
            await movements_collection.insert_one(movement)
        except Exception as e:
             logger.error(f"Failed to log stock movement for {product_id}: {e}", exc_info=True)

    async def get_stock_movement_history(
        self, merchant_id: str, product_id: Optional[str] = None, limit: int = 50
    ) -> List[Dict]:
        """Retrieve stock movement history. (From v5)"""
        if not self.db or not self.db._initialized:
             logger.error("DB not initialized, cannot get stock history."); return []
        try:
            query = {"merchant_id": merchant_id}
            if product_id: query["product_id"] = product_id
            movements_collection = await self.db.get_collection("stock_movements")
            cursor = movements_collection.find(query).sort("timestamp", -1).limit(limit)
            movements = await cursor.to_list(length=limit)
            return movements
        except Exception as e:
             logger.error(f"Failed to get stock movement history: {e}", exc_info=True); return []
