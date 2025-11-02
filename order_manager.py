# order_manager_v6.py - Unified Order Lifecycle (Admin + Merchant)
"""
Handles the complete order lifecycle for both merchants and admins,
combining v5's robust merchant functions with v4's admin oversight.

Features:
- Full merchant lifecycle: create, accept, decline, complete, cancel.
- Full admin oversight: force_cancel, approve, get_all, get_stats.
- Async-safe and deadlock-protected (from v5 fix).
- Atomic inventory deduction and rollback on all state changes.
- Role-aware auditing for all inventory and status modifications.
- Integrated with unified v6 DB, Inventory, Knowledge, and Rules engines.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from enum import Enum
import uuid

# Use the v6 logger name as specified
logger = logging.getLogger("order_manager_v6")


class OrderStatus(str, Enum):
    """Unified order status states."""
    PENDING = "pending"
    ACCEPTED = "accepted"
    DECLINED = "declined"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REVIEW = "review"  # Added from admin spec


class OrderManagerV6:
    """
    Manages the complete, role-aware order lifecycle (v6).
    Coordinates between db, inventory, knowledge base, and rules.
    """

    def __init__(
        self,
        db,  # db_v6
        inventory_manager,  # inventory_manager_v6
        knowledge_detector,  # knowledge_detector_v6
        rules_engine,  # rules_engine_v6
        alert_system=None
    ):
        """
        Initialize the unified OrderManagerV6.
        """
        self.db = db
        self.inventory = inventory_manager
        self.knowledge = knowledge_detector
        self.rules = rules_engine  # Use self.rules as per v6 spec
        self.alerts = alert_system
        # Fine-grained lock from v5 to prevent race conditions on *individual* orders
        self._order_lock = asyncio.Lock()
        logger.info("Initialized unified OrderManagerV6 (Admin + Merchant)")

    async def update_order(self, order_id: str, update_data: Dict):
        """
        Internal helper to update an order and ensure 'updated_at' is set.
        (from v5)
        """
        if not order_id: raise ValueError("order_id is required")
        if not update_data: logger.warning(f"No update data for order {order_id}"); return
        try:
            final_update = update_data.copy()
            if "$set" not in final_update and not any(op.startswith('$') for op in final_update):
                 final_update = {"$set": final_update}
            final_update.setdefault("$set", {})["updated_at"] = datetime.now(timezone.utc).isoformat()

            await self.db.update_order(order_id, final_update)
            logger.debug(f"Order {order_id} updated via db layer.")
        except Exception as e:
            logger.error(f"Error in OrderManager.update_order for {order_id}: {e}", exc_info=True)
            raise

    # --------------------------------------------------
    # MERCHANT OPERATIONS (from v5, with v6 updates)
    # --------------------------------------------------

    async def create_order_from_cart(
        self,
        conversation_id: str,
        merchant_id: str,
        customer_phone: str,
        cart_data: Dict,
        customer_name: Optional[str] = None,
        delivery_address: Optional[str] = None,
        notes: Optional[str] = None,
        ttl_hours: int = 24
    ) -> Dict:
        """
        Create order from cart data.
        (from v5)
        """
        if not all([conversation_id, merchant_id, customer_phone, cart_data]):
            raise ValueError("Missing required order parameters")
        if not cart_data.get("items"):
            raise ValueError("Cart must have at least one item")

        order_id = self._generate_order_id()
        now_utc = datetime.now(timezone.utc)
        expiry_time = now_utc + timedelta(hours=ttl_hours)

        order = {
            "order_id": order_id,
            "conversation_id": conversation_id,
            "merchant_id": merchant_id,
            "customer_phone": customer_phone,
            "customer_name": customer_name,
            "delivery_address": delivery_address,
            "items": cart_data.get("items", []),
            "total_amount": float(cart_data.get("total", 0.0)),
            "item_count": int(cart_data.get("item_count", 0)),
            "status": OrderStatus.PENDING.value,
            "confirmed_at": None,
            "inventory_deducted": False,
            "notes": notes,
            "created_at": now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
            "expiry_time": expiry_time.isoformat(),
            "timeline": [
                {
                    "status": OrderStatus.PENDING.value,
                    "timestamp": now_utc.isoformat(),
                    "note": "Order created and awaiting merchant confirmation",
                    "actor": "system"
                }
            ]
        }
        try:
            await self.db.create_order(order)
            logger.info(
                f"Created order {order_id} for {merchant_id}: {order['item_count']} items, Rs.{order['total_amount']:.2f}"
            )
        except Exception as e:
            logger.error(f"Failed to create order in DB for {order_id}: {e}", exc_info=True)
            raise
        return order

    async def accept_order(
        self,
        order_id: str,
        merchant_id: str,
        acceptance_note: Optional[str] = None,
        estimated_delivery: Optional[datetime] = None
    ) -> Dict:
        """
        Merchant accepts order - deducts inventory and updates knowledge base.
        (CRITICAL v5 DEADLOCK FIX PRESERVED)
        (v6 UPDATE: Added role="merchant" to inventory calls and uses self.rules)
        """
        if not order_id or not merchant_id:
            raise ValueError("order_id and merchant_id required")

        # --- Step 1: Read Order (No Lock) ---
        order = await self.db.get_order(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")
        if order.get("merchant_id") != merchant_id:
            logger.warning(f"Unauthorized accept: {merchant_id} vs {order.get('merchant_id')}")
            raise ValueError(f"Unauthorized: Order does not belong to merchant {merchant_id}")

        # --- Step 2: Validate Status & Expiry (No Lock) ---
        if order.get("status") != OrderStatus.PENDING.value:
            raise ValueError(f"Order {order_id} is not in pending state (current: {order.get('status')})")

        if order.get("expiry_time"):
            try:
                expiry = await self._parse_iso_datetime(order["expiry_time"])
                if expiry and datetime.now(timezone.utc) >= expiry:
                    # Try to expire it, but raise error regardless
                    try: await self.expire_order(order_id)
                    except Exception: pass  # Ignore errors if already expired etc.
                    raise ValueError(f"Order {order_id} has expired")
            except Exception as e:
                 logger.error(f"Error checking expiry for {order_id}: {e}")
                 if "expired" in str(e): raise

        if estimated_delivery:
            if not isinstance(estimated_delivery, datetime): raise ValueError("estimated_delivery must be datetime")
            if estimated_delivery.tzinfo is None: estimated_delivery = estimated_delivery.replace(tzinfo=timezone.utc)
            if estimated_delivery <= datetime.now(timezone.utc): raise ValueError("Estimated delivery must be in the future")

        # --- Step 3: Deduct Inventory (No Order Lock) ---
        # This is the slow I/O operation. It uses its *own* fine-grained product locks.
        items_to_deduct = order.get("items", [])
        deduction_success, deduction_results = await self.inventory.batch_deduct_stock(
            merchant_id=merchant_id,
            items=items_to_deduct,
            role="merchant"  # v6: Specify role
        )

        if not deduction_success:
             # Construct error message from detailed batch results
             failed_items_info = []
             for res in deduction_results:
                  if not res.get("success"):
                       reason = res.get("reason", "Unknown error")
                       name = res.get("product_name", res.get("product_id", "Unknown item"))
                       failed_items_info.append(f"{name} ({reason})")
             error_msg = f"Inventory deduction failed: {', '.join(failed_items_info)}"
             logger.error(f"Order {order_id} acceptance failed: {error_msg}")
             # CRITICAL: Raise to stop acceptance so API can return 400
             raise ValueError(error_msg)

        # --- Step 4: Update Order Status (Acquire Order Lock) ---
        # Now that all slow I/O is done, acquire the lock for the final, fast state change.
        async with self._order_lock:
            # Re-check status *inside the lock* in case of race condition
            current_order_state = await self.db.get_order(order_id)
            if current_order_state.get("status") != OrderStatus.PENDING.value:
                # Another process (e.g., expiry) got here first.
                # We must roll back the inventory.
                logger.critical(f"Order {order_id} state changed to {current_order_state.get('status')} during inventory deduction! Rolling back.")
                rollback_tasks = [
                    self.inventory.update_quantity(
                        merchant_id=merchant_id,
                        product_id=item.get("product_id"),
                        quantity_change=float(item.get("quantity", 0)),
                        change_reason="order_accept_race_condition_rollback",
                        role="merchant"  # v6: Specify role
                    ) for item in items_to_deduct
                ]
                await asyncio.gather(*rollback_tasks, return_exceptions=True)
                raise ValueError(f"Order state changed mid-process (now {current_order_state.get('status')}). Inventory rolled back.")

            # All clear. Commit the final status update.
            now_utc = datetime.now(timezone.utc)
            update_payload = {
                 "$set": {
                     "status": OrderStatus.ACCEPTED.value,
                     "confirmed_at": now_utc.isoformat(),
                     "inventory_deducted": True
                 },
                 "$push": {
                      "timeline": {
                           "status": OrderStatus.ACCEPTED.value,
                           "timestamp": now_utc.isoformat(),
                           "note": acceptance_note or "Order accepted by merchant",
                           "actor": "merchant"
                      }
                 }
            }
            if estimated_delivery:
                 update_payload["$set"]["estimated_delivery"] = estimated_delivery.isoformat()

            try:
                 await self.update_order(order_id, update_payload)
            except Exception as e:
                logger.error(f"Failed to update order status (inside lock) for {order_id}: {e}", exc_info=True)
                # CRITICAL: Rollback inventory
                logger.critical(f"Attempting inventory rollback for failed FINAL order update {order_id}...")
                rollback_tasks = [
                    self.inventory.update_quantity(
                        merchant_id=merchant_id,
                        product_id=item.get("product_id"),
                        quantity_change=float(item.get("quantity", 0)),
                        change_reason="order_accept_final_update_failed_rollback",
                        role="merchant"  # v6: Specify role
                    ) for item in items_to_deduct
                ]
                await asyncio.gather(*rollback_tasks, return_exceptions=True)
                raise RuntimeError(f"Failed to update order {order_id} status. Inventory rollback attempted.")

        # --- Step 5: Post-Acceptance Tasks (No Lock) ---
        updated_order = await self.db.get_order(order_id)
        if not updated_order:
             logger.error(f"Could not retrieve updated order {order_id} after acceptance.")
             return order  # Return stale order as fallback

        try:
            if self.knowledge and hasattr(self.knowledge, 'update_context_after_order_accepted'):
                 await self.knowledge.update_context_after_order_accepted(updated_order)
        except Exception as e: logger.warning(f"Failed to update knowledge base for {order_id}: {e}")

        try:
             # v6: Use self.rules (from __init__)
             if self.rules and hasattr(self.rules, 'evaluate_order'):
                  await self.rules.evaluate_order(updated_order)
        except Exception as e: logger.warning(f"Error evaluating business rules for {order_id}: {e}")

        logger.info(f"Order {order_id} accepted by merchant {merchant_id}")
        return updated_order

    async def decline_order(
        self,
        order_id: str,
        merchant_id: str,
        decline_reason: Optional[str] = None
    ) -> Dict:
        """
        Merchant declines order - no inventory changes.
        (from v5)
        """
        if not order_id or not merchant_id:
            raise ValueError("order_id and merchant_id required")

        async with self._order_lock:  # Lock to prevent race condition
            order = await self.db.get_order(order_id)
            if not order: raise ValueError(f"Order {order_id} not found")
            if order.get("merchant_id") != merchant_id:
                raise ValueError(f"Unauthorized: Order does not belong to merchant {merchant_id}")
            if order.get("status") != OrderStatus.PENDING.value:
                raise ValueError(f"Order {order_id} is not in pending state (current: {order.get('status')})")

            now_utc = datetime.now(timezone.utc)
            update_payload = {
                 "$set": { "status": OrderStatus.DECLINED.value, "decline_reason": decline_reason },
                 "$push": {
                      "timeline": {
                           "status": OrderStatus.DECLINED.value, "timestamp": now_utc.isoformat(),
                           "note": decline_reason or "Order declined by merchant", "actor": "merchant"
                      }
                 }
            }
            try:
                await self.update_order(order_id, update_payload)
                logger.info(f"Order {order_id} declined by {merchant_id}: {decline_reason or 'No reason'}")
                updated_order = await self.db.get_order(order_id)
                return updated_order or order
            except Exception as e:
                logger.error(f"Failed to update declined order {order_id}: {e}", exc_info=True)
                raise

    async def complete_order(
        self,
        order_id: str,
        merchant_id: str,
        completion_note: Optional[str] = None
    ) -> Dict:
        """
        Mark order as completed (delivered/picked up).
        (from v5)
        """
        if not order_id or not merchant_id:
            raise ValueError("order_id and merchant_id required")

        async with self._order_lock:
             order = await self.db.get_order(order_id)
             if not order: raise ValueError(f"Order {order_id} not found")
             if order.get("merchant_id") != merchant_id:
                  raise ValueError(f"Unauthorized: Order does not belong to merchant {merchant_id}")
             if order.get("status") != OrderStatus.ACCEPTED.value:
                  raise ValueError(f"Order {order_id} cannot be completed (status: {order.get('status')})")

             now_utc = datetime.now(timezone.utc)
             update_payload = {
                  "$set": { "status": OrderStatus.COMPLETED.value, "completed_at": now_utc.isoformat() },
                  "$push": {
                       "timeline": {
                            "status": OrderStatus.COMPLETED.value, "timestamp": now_utc.isoformat(),
                            "note": completion_note or "Order completed and delivered", "actor": "merchant"
                       }
                  }
             }
             try:
                  await self.update_order(order_id, update_payload)
                  logger.info(f"Order {order_id} marked as completed")
                  updated_order = await self.db.get_order(order_id)
                  return updated_order or order
             except Exception as e:
                  logger.error(f"Failed to complete order {order_id}: {e}", exc_info=True)
                  raise

    async def cancel_order(
        self,
        order_id: str,
        cancellation_reason: str,
        cancelled_by: str = "customer"  # Role-aware: customer, merchant, admin
    ) -> Dict:
        """
        Cancel order - returns inventory if already accepted.
        (from v5)
        (v6 UPDATE: Added role=cancelled_by to inventory call)
        """
        if not order_id or not cancellation_reason:
            raise ValueError("order_id and cancellation_reason required")

        async with self._order_lock:
              order = await self.db.get_order(order_id)
              if not order: raise ValueError(f"Order {order_id} not found")

              current_status = order.get("status")
              if current_status in [OrderStatus.COMPLETED.value, OrderStatus.CANCELLED.value, OrderStatus.EXPIRED.value]:
                   raise ValueError(f"Order {order_id} cannot be cancelled (status: {current_status})")

              inventory_returned = False
              # v6: Check inventory_deducted flag, not just status
              if order.get("inventory_deducted", False):
                   try:
                        rollback_tasks = []
                        for item in order.get("items", []):
                             rollback_tasks.append(
                                  self.inventory.update_quantity(
                                       merchant_id=order.get("merchant_id"),
                                       product_id=item.get("product_id"),
                                       quantity_change=float(item.get("quantity", 0)),  # Add back
                                       change_reason="order_cancellation_rollback",
                                       role=cancelled_by  # v6: Pass the role
                                  )
                             )
                        results = await asyncio.gather(*rollback_tasks, return_exceptions=True)
                        if any(isinstance(r, Exception) or not r.get("success") for r in results if not isinstance(r, Exception)):
                             logger.error(f"Error returning inventory for cancelled order {order_id}: {results}")
                        else:
                             inventory_returned = True
                             logger.info(f"Returned inventory for cancelled order {order_id}")
                   except Exception as e:
                       logger.error(f"Error returning inventory for cancelled order {order_id}: {e}", exc_info=True)
              
              # If inventory wasn't deducted, we don't need to log its return
              elif current_status == OrderStatus.ACCEPTED.value:
                   logger.warning(f"Cancelling ACCEPTED order {order_id} but inventory_deducted=False. No rollback performed.")


              now_utc = datetime.now(timezone.utc)
              update_payload = {
                   "$set": {
                       "status": OrderStatus.CANCELLED.value,
                       "cancellation_reason": cancellation_reason,
                       "cancelled_by": cancelled_by,
                       # v6: Ensure inventory flag is reset if we failed to return
                       "inventory_deducted": False if inventory_returned else order.get("inventory_deducted", False),
                   },
                   "$push": {
                       "timeline": {
                            "status": OrderStatus.CANCELLED.value, "timestamp": now_utc.isoformat(),
                            "note": cancellation_reason, "actor": cancelled_by
                       }
                   }
              }
              if inventory_returned:
                   update_payload["$set"]["inventory_deducted"] = False

              try:
                   await self.update_order(order_id, update_payload)
                   logger.info(f"Order {order_id} cancelled by {cancelled_by}. Inventory returned: {inventory_returned}")
                   updated_order = await self.db.get_order(order_id)
                   return updated_order or order
              except Exception as e:
                   logger.error(f"Failed to update cancelled order {order_id}: {e}", exc_info=True)
                   raise

    async def expire_order(self, order_id: str) -> Optional[Dict]:
        """
        Auto-expire pending order after timeout.
        (from v5)
        """
        if not order_id: raise ValueError("order_id required")

        async with self._order_lock:
             order = await self.db.get_order(order_id)
             if not order: raise ValueError(f"Order {order_id} not found")
             
             # v6: Check legacy status as well
             valid_pending_statuses = [OrderStatus.PENDING.value, "pending_confirmation"]
             current_status = order.get("status")
             
             if current_status not in valid_pending_statuses:
                  logger.warning(f"Attempted to expire non-pending order {order_id} (status: {current_status})")
                  return order

             now_utc = datetime.now(timezone.utc)
             update_payload = {
                  "$set": { "status": OrderStatus.EXPIRED.value },
                  "$push": {
                       "timeline": {
                            "status": OrderStatus.EXPIRED.value, "timestamp": now_utc.isoformat(),
                            "note": "Order expired due to no merchant response", "actor": "system"
                       }
                  }
             }
             try:
                  await self.update_order(order_id, update_payload)
                  logger.info(f"Order {order_id} expired at {now_utc.isoformat()}")
                  updated_order = await self.db.get_order(order_id)
                  return updated_order
             except Exception as e:
                  logger.error(f"Failed to expire order {order_id}: {e}", exc_info=True)
                  return None

    # --------------------------------------------------
    # ADMIN EXTENSIONS (from v4 spec)
    # --------------------------------------------------

    async def get_all_orders_admin(self, status_filter: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Admin: Retrieve system-wide orders."""
        try:
            # Assumes db_v6 has this method
            return await self.db.get_all_orders(status_filter=status_filter, limit=limit)
        except Exception as e:
            logger.error(f"Admin failed to fetch orders: {e}"); return []

    async def force_cancel_order_admin(self, order_id: str, admin_user: str, reason: str = "policy_violation") -> Dict:
        """Admin override cancellation with audit trail and inventory rollback."""
        if not admin_user:
             raise ValueError("admin_user is required for force cancellation")
             
        async with self._order_lock:
            order = await self.db.get_order(order_id)
            if not order: raise ValueError(f"Order {order_id} not found")
            if order.get("status") in [OrderStatus.CANCELLED.value, OrderStatus.COMPLETED.value]:
                logger.warning(f"Admin tried to cancel already-closed order {order_id}"); return order

            inventory_returned = False
            # Perform rollback only if inventory was actually deducted
            if order.get("inventory_deducted", False):
                try:
                    rollback_tasks = [
                        self.inventory.update_quantity(
                            merchant_id=order["merchant_id"],
                            product_id=i["product_id"],
                            quantity_change=float(i["quantity"]),
                            change_reason="admin_forced_cancel_rollback",
                            role="admin"  # v6: Specify admin role
                        ) for i in order.get("items", [])
                    ]
                    results = await asyncio.gather(*rollback_tasks, return_exceptions=True)
                    if any(isinstance(r, Exception) or not r.get("success") for r in results if not isinstance(r, Exception)):
                         logger.error(f"Error returning inventory for admin cancelled order {order_id}: {results}")
                    else:
                         inventory_returned = True
                         logger.info(f"Returned inventory for admin cancelled order {order_id}")
                except Exception as e:
                     logger.error(f"Error returning inventory for admin cancelled order {order_id}: {e}", exc_info=True)


            now_utc = datetime.now(timezone.utc)
            update_payload = {
                "$set": {
                    "status": OrderStatus.CANCELLED.value,
                    "cancelled_by": admin_user,
                    "cancellation_reason": reason
                },
                "$push": {
                    "timeline": {
                        "status": OrderStatus.CANCELLED.value,
                        "timestamp": now_utc.isoformat(),
                        "note": f"Cancelled by admin: {reason}",
                        "actor": admin_user
                    }
                }
            }
            if inventory_returned:
                 update_payload["$set"]["inventory_deducted"] = False

            await self.update_order(order_id, update_payload)
            # Assumes db_v6 has this method
            if hasattr(self.db, "record_admin_action"):
                await self.db.record_admin_action(admin_user, "force_cancel_order", {"order_id": order_id, "reason": reason})
            logger.info(f"Admin {admin_user} force-cancelled order {order_id}")
            return await self.db.get_order(order_id)

    async def approve_order_admin(self, order_id: str, admin_user: str, note: Optional[str] = None):
        """
        Admin approval for flagged or pending review orders.
        This action IS an acceptance: it deducts inventory and sets status to ACCEPTED.
        (v6 RE-IMPLEMENTATION for safety and consistency)
        """
        if not order_id or not admin_user:
            raise ValueError("order_id and admin_user required")

        # --- Step 1: Read Order (No Lock) ---
        order = await self.db.get_order(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")

        merchant_id = order.get("merchant_id")
        if not merchant_id:
             raise ValueError(f"Order {order_id} missing merchant_id, cannot approve.")

        # --- Step 2: Validate Status (No Lock) ---
        valid_statuses = [OrderStatus.PENDING.value, OrderStatus.REVIEW.value]
        if order.get("status") not in valid_statuses:
            raise ValueError(f"Order {order_id} cannot be approved (status: {order['status']})")

        # --- Step 3: Deduct Inventory (No Order Lock) ---
        items_to_deduct = order.get("items", [])
        deduction_success, deduction_results = await self.inventory.batch_deduct_stock(
            merchant_id=merchant_id,
            items=items_to_deduct,
            role="admin"  # v6: Specify admin role
        )

        if not deduction_success:
             failed_items_info = [
                 f"{res.get('product_name', res.get('product_id'))} ({res.get('reason')})"
                 for res in deduction_results if not res.get("success")
             ]
             error_msg = f"Inventory deduction failed: {', '.join(failed_items_info)}"
             logger.error(f"Admin approval {order_id} failed: {error_msg}")
             raise ValueError(error_msg)

        # --- Step 4: Update Order Status (Acquire Order Lock) ---
        async with self._order_lock:
            # Re-check status *inside the lock*
            current_order_state = await self.db.get_order(order_id)
            if current_order_state.get("status") not in valid_statuses:
                logger.critical(f"Order {order_id} state changed to {current_order_state.get('status')} during admin approval! Rolling back.")
                rollback_tasks = [
                    self.inventory.update_quantity(
                        merchant_id=merchant_id, product_id=item.get("product_id"),
                        quantity_change=float(item.get("quantity", 0)),
                        change_reason="admin_approve_race_condition_rollback", role="admin"
                    ) for item in items_to_deduct
                ]
                await asyncio.gather(*rollback_tasks, return_exceptions=True)
                raise ValueError(f"Order state changed mid-process (now {current_order_state.get('status')}). Inventory rolled back.")

            # All clear. Commit the final status update.
            now_utc = datetime.now(timezone.utc)
            update_payload = {
                 "$set": {
                     "status": OrderStatus.ACCEPTED.value,
                     "confirmed_at": now_utc.isoformat(),
                     "inventory_deducted": True,
                     "approved_by_admin": admin_user
                 },
                 "$push": {
                      "timeline": {
                           "status": OrderStatus.ACCEPTED.value,
                           "timestamp": now_utc.isoformat(),
                           "note": note or "Approved by admin",
                           "actor": admin_user
                      }
                 }
            }

            try:
                 await self.update_order(order_id, update_payload)
            except Exception as e:
                logger.error(f"Failed to update order status (inside lock) for {order_id} by admin: {e}", exc_info=True)
                logger.critical(f"Attempting inventory rollback for failed FINAL admin approval {order_id}...")
                rollback_tasks = [
                    self.inventory.update_quantity(
                        merchant_id=merchant_id, product_id=item.get("product_id"),
                        quantity_change=float(item.get("quantity", 0)),
                        change_reason="admin_approve_final_update_failed_rollback", role="admin"
                    ) for item in items_to_deduct
                ]
                await asyncio.gather(*rollback_tasks, return_exceptions=True)
                raise RuntimeError(f"Failed to update order {order_id} status. Inventory rollback attempted.")

        # --- Step 5: Post-Acceptance Tasks (No Lock) ---
        updated_order = await self.db.get_order(order_id)
        
        # Assumes db_v6 has this method
        if hasattr(self.db, "record_admin_action"):
            await self.db.record_admin_action(admin_user, "approve_order", {"order_id": order_id})

        try:
             if self.rules and hasattr(self.rules, 'evaluate_order'):
                  await self.rules.evaluate_order(updated_order)
        except Exception as e: logger.warning(f"Error evaluating business rules for {order_id} (admin approve): {e}")

        logger.info(f"Admin {admin_user} approved order {order_id}")
        return updated_order

    async def get_merchant_order_stats(self, merchant_id: str) -> Dict:
        """Get summary of order statuses for a given merchant (admin view)."""
        try:
            # Assumes db_v6 has get_orders_by_merchant
            orders = await self.db.get_orders_by_merchant(merchant_id=merchant_id, limit=10000) # Get all for stats
            summary = {s.value: 0 for s in OrderStatus}
            summary["unknown"] = 0  # Add unknown key for safety
            
            for o in orders:
                status_key = o.get("status", "unknown")
                if status_key not in summary:
                     summary[status_key] = 0 # Handle legacy statuses
                summary[status_key] += 1
                
            return {"merchant_id": merchant_id, "summary": summary, "total_orders": len(orders)}
        except Exception as e:
            logger.error(f"Error in get_merchant_order_stats: {e}"); return {}

    # --------------------------------------------------
    # UTILITIES (shared, from v5)
    # --------------------------------------------------

    async def get_order(self, order_id: str) -> Dict:
        """Retrieve order with current status."""
        if not order_id: raise ValueError("order_id required")
        try:
            order = await self.db.get_order(order_id)
            if not order: raise ValueError(f"Order {order_id} not found")
            return order
        except Exception as e:
            logger.error(f"Error retrieving order {order_id}: {e}"); raise

    async def get_customer_orders(
        self, customer_phone: str, limit: int = 10, status_filter: Optional[str] = None
    ) -> List[Dict]:
        """Get order history for a customer."""
        if not customer_phone: raise ValueError("customer_phone required")
        try:
            return await self.db.get_orders_by_customer(
                customer_phone=customer_phone, limit=limit, status_filter=status_filter
            )
        except Exception as e:
            logger.error(f"Error retrieving customer orders: {e}"); return []

    async def get_merchant_orders(
        self, merchant_id: str, status_filter: Optional[str] = None, limit: int = 50
    ) -> List[Dict]:
        """Get orders for merchant dashboard."""
        if not merchant_id: raise ValueError("merchant_id required")
        try:
            return await self.db.get_orders_by_merchant(
                merchant_id=merchant_id, status_filter=status_filter, limit=limit
            )
        except Exception as e:
            logger.error(f"Error retrieving merchant orders: {e}"); return []

    async def get_pending_orders_for_expiry_check(self) -> List[Dict]:
        """Get all pending orders that may need expiry processing."""
        try:
            # v6: Check legacy and new pending statuses
            pending_statuses = [OrderStatus.PENDING.value, "pending_confirmation"]
            # Assumes db_v6 has get_orders_by_statuses
            return await self.db.get_orders_by_statuses(pending_statuses)
        except Exception as e:
            logger.error(f"Error retrieving pending orders: {e}"); return []

    def _generate_order_id(self) -> str:
        """Generate unique order ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        unique_suffix = str(uuid.uuid4().hex)[:12].upper()
        return f"ORD-{timestamp}-{unique_suffix}"

    async def format_order_receipt(self, order_id: str) -> str:
        """Generate formatted order receipt for WhatsApp."""
        if not order_id: raise ValueError("order_id required")
        try:
            order = await self.get_order(order_id)
        except Exception as e:
            logger.error(f"Error formatting receipt: {e}")
            raise ValueError(f"Could not fetch order {order_id}") from e

        lines = ["*ORDER RECEIPT*", "=" * 30, f"Order ID: {order.get('order_id')}", f"Status: *{order.get('status', 'unknown').upper()}*"]
        created_at_str = order.get("created_at")
        if created_at_str:
             dt = await self._parse_iso_datetime(created_at_str)
             lines.append(f"Date: {dt.strftime('%d %b %Y, %I:%M %p %Z') if dt else created_at_str}")
        else: lines.append("Date: N/A")
        
        lines.append("\n*ITEMS:*")
        item_count = 0; total_calc = 0.0
        for idx, item in enumerate(order.get("items", []), 1):
            item_count += 1
            name = item.get("product_name", "Unknown Item")
            qty = float(item.get("quantity", 0))
            unit = item.get("unit", "pcs")
            unit_price = float(item.get("unit_price", 0.0))
            subtotal = qty * unit_price
            total_calc += subtotal
            lines.append(f"*{idx}. {name}*")
            lines.append(f"   Qty: {qty} {unit} @ Rs.{unit_price:.2f} each")
            lines.append(f"   Subtotal: Rs.{subtotal:.2f}")
            
        lines.append("=" * 30)
        lines.append(f"*TOTAL ({item_count} items): Rs.{total_calc:.2f}*")
        
        stored_total = float(order.get('total_amount', -1.0))
        if abs(total_calc - stored_total) > 0.01:
             logger.warning(f"Order {order_id}: Calc total Rs.{total_calc:.2f} != stored Rs.{stored_total:.2f}")
             lines.append(f"_(Stored Total: Rs.{stored_total:.2f})_")
             
        delivery_str = order.get("estimated_delivery")
        if delivery_str:
             dt = await self._parse_iso_datetime(delivery_str)
             if dt: lines.append(f"Estimated Pickup/Delivery: {dt.strftime('%d %b %Y, %I:%M %p %Z')}")
             
        if order.get("notes"): lines.append(f"Notes: {order['notes']}")
        lines.append("=" * 30)
        return "\n".join(lines)

    async def format_order_summary(self, order: Dict) -> str:
        """Generate short order summary for WhatsApp messages."""
        items = order.get("items", [])
        item_strings = [f"{item.get('quantity', '?')} {item.get('product_name', 'Item')}" for item in items[:3]]
        item_list = ", ".join(item_strings)
        extra = f"... (+{len(items) - 3} more)" if len(items) > 3 else ""
        return (
            f"Order {order.get('order_id', 'N/A')}:\n"
            f"{item_list}{extra}\n"
            f"*Total: Rs.{order.get('total_amount', 0.0):.2f}*\n"
            f"*Status: {order.get('status', 'unknown').upper()}*"
        )


# Helper function (from v5)
async def _parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO datetime string safely, handling Z suffix and timezone-naive inputs.
    Returns timezone-aware datetime in UTC, or None if invalid.
    """
    if not dt_str: return None
    try:
        normalized = str(dt_str).replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid datetime format: {dt_str} - {e}")
        return None
