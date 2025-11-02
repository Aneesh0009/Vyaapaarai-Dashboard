# reminder_system.py - Order Reminders and Auto-Expiry Management
"""
Manages automated reminders for pending orders and handles auto-expiry.
Runs as asyncio task in FastAPI event loop (not blocking thread).
UPDATED: Fixed event loop handling, datetime deprecation, function signatures, error handling
VERSION: 1.1.0
"""

import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class ReminderSystem:
    """
    Manages automated reminders and order expiry.
    Runs as asyncio background task to avoid thread/event loop conflicts.
    """

    def __init__(
        self,
        order_manager,
        integrations_module,
        db_module, # db_module is expected here
        check_interval_seconds: int = 300,
        reminder_intervals: Optional[List[int]] = None,
        ttl_hours: int = 24
    ):
        """
        Initialize reminder system.

        Args:
            order_manager: Reference to order_manager.py
            integrations_module: Reference to integrations.py for WhatsApp messaging
            db_module: Reference to db.py for merchant/order data
            check_interval_seconds: How often to check for reminders/expiry (default: 300 = 5 min)
            reminder_intervals: List of hours when reminders are sent (default: [2, 6, 24])
            ttl_hours: Order TTL before auto-expiry in hours (default: 24)

        Raises:
            ValueError: If required methods missing or parameters invalid
        """
        # Validate order_manager has required methods
        required_methods = [
            "get_pending_orders_for_expiry_check",
            "expire_order",
            "get_order",
            # Need update_order to mark reminders sent
            "update_order"
        ]
        for method in required_methods:
            if not hasattr(order_manager, method) or not callable(getattr(order_manager, method)):
                raise ValueError(f"order_manager missing required method: {method}")

        # Validate integrations has send_whatsapp_message
        # Assume get_whatsapp() returns the integration object correctly
        if not hasattr(integrations_module, "send_whatsapp_message"):
             # Check the object returned by get_whatsapp() if integrations_module is that function
             whatsapp_instance = integrations_module # Assuming it's already the instance
             if not hasattr(whatsapp_instance, "send_whatsapp_message"):
                 raise ValueError("integrations_module must have send_whatsapp_message(phone, text) method")

        # Validate db has required methods
        db_methods = ["get_merchant"] # Assuming db_module is the Database instance
        for method in db_methods:
            if not hasattr(db_module, method) or not callable(getattr(db_module, method)):
                raise ValueError(f"db_module missing required method: {method}")

        self.order_manager = order_manager
        self.integrations = integrations_module # Store the whatsapp instance
        self.db = db_module # Store the db instance
        self.check_interval = check_interval_seconds
        self.ttl_hours = ttl_hours
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Reminder intervals in hours (when reminders are sent after order creation)
        if reminder_intervals:
            if not reminder_intervals or any(h <= 0 for h in reminder_intervals):
                raise ValueError("reminder_intervals must be list of positive hours")
            self.reminder_intervals = sorted(reminder_intervals)
        else:
            self.reminder_intervals = [2, 6, 24] # Default intervals

        logger.info(
            f"ReminderSystem initialized: "
            f"check_interval={check_interval_seconds}s, "
            f"ttl={ttl_hours}h, "
            f"reminders={self.reminder_intervals}h"
        )

    async def start(self):
        """
        Start the background reminder task.
        Should be called from FastAPI startup event.
        """
        if self._running:
            logger.warning("ReminderSystem already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._check_loop())
        logger.info("ReminderSystem background task started")

    async def stop(self):
        """Stop the background reminder task."""
        if not self._running or not self._task:
            return

        self._running = False
        logger.info("Attempting to stop ReminderSystem task...")

        # Cancel the task and wait for it to finish
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            logger.info("ReminderSystem task successfully cancelled.")
        except Exception as e:
            logger.error(f"Error during ReminderSystem task shutdown: {e}", exc_info=True)

        self._task = None
        logger.info("ReminderSystem stopped")

    async def _check_loop(self):
        """Main async loop for checking pending orders and sending reminders."""
        logger.info("ReminderSystem check loop starting.")

        while self._running:
            try:
                logger.debug("Running reminder/expiry check cycle...")
                await self._check_all_pending_orders()
                logger.debug("Reminder/expiry check cycle finished.")
            except asyncio.CancelledError:
                 logger.info("ReminderSystem check loop received cancellation.")
                 break # Exit loop cleanly on cancellation
            except Exception as e:
                logger.error(f"Error in reminder check cycle: {e}", exc_info=True)
                # Avoid tight loop on persistent errors
                await asyncio.sleep(min(self.check_interval, 60)) # Sleep briefly even on error

            # Sleep until the next check interval
            if self._running: # Check running flag again before sleeping
                try:
                    await asyncio.sleep(self.check_interval)
                except asyncio.CancelledError:
                    logger.info("ReminderSystem check loop cancelled during sleep.")
                    break # Exit loop cleanly
        logger.info("ReminderSystem check loop stopped.")


    async def _check_all_pending_orders(self):
        """Check all pending orders for reminders and expiry."""
        try:
            pending_orders = await self.order_manager.get_pending_orders_for_expiry_check()

            if not pending_orders:
                logger.debug("No pending orders found.")
                return

            logger.info(f"Checking {len(pending_orders)} pending orders for reminders/expiry")
            # Debug: Log basic info to diagnose filter mismatches
            try:
                for o in pending_orders:
                    logger.debug(
                        f"PendingOrder -> id={o.get('order_id')}, merchant_id={o.get('merchant_id')}, status={o.get('status')}"
                    )
            except Exception:
                pass

            now_utc = datetime.now(timezone.utc)
            tasks = []
            for order in pending_orders:
                tasks.append(self._process_single_order(order, now_utc))

            # Process orders concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                 if isinstance(result, Exception):
                      order_id = pending_orders[i].get("order_id", "unknown")
                      logger.error(f"Error processing order {order_id} in gather: {result}", exc_info=result)


        except Exception as e:
            logger.error(f"Error retrieving or processing pending orders: {e}", exc_info=True)

    async def _process_single_order(self, order: Dict, current_time: datetime):
        """ Process reminders and expiry for a single order."""
        order_id = order.get("order_id")
        if not order_id:
            logger.warning("Found pending order with no order_id.")
            return

        try:
            # Check for expiry first
            expired = await self._check_and_process_expiry(order, current_time)
            if expired:
                 return # Don't send reminders for expired orders

            # If not expired, check for reminders
            await self._check_and_process_reminders(order, current_time)

        except Exception as e:
             logger.error(f"Failed processing order {order_id}: {e}", exc_info=True)
             # Raise the exception so asyncio.gather can report it
             raise


    async def _check_and_process_expiry(self, order: Dict, current_time: datetime) -> bool:
        """ Checks if an order is expired and processes it if so. Returns True if expired."""
        order_id = order.get("order_id")
        expiry_time_str = order.get("expiry_time")
        if not expiry_time_str:
            # Try to calculate and set default expiry if missing
            created_at_str = order.get("created_at")
            if not created_at_str:
                 logger.warning(f"Order {order_id} has no created_at or expiry_time.")
                 return False # Cannot determine expiry
            try:
                created_at = datetime.fromisoformat(created_at_str)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                expiry_time = created_at + timedelta(hours=self.ttl_hours)
                expiry_time_str = expiry_time.isoformat()
                # Update DB - fire and forget, log error if fails
                asyncio.create_task(self.order_manager.update_order(order_id, {"expiry_time": expiry_time_str}))
            except (ValueError, TypeError, Exception) as e:
                 logger.error(f"Error calculating/setting expiry for {order_id}: {e}")
                 return False

        # Now parse the expiry time
        try:
            expiry_norm = str(expiry_time_str).replace("Z", "+00:00")
            expiry_time = datetime.fromisoformat(expiry_norm)
            if expiry_time.tzinfo is None:
                expiry_time = expiry_time.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
             logger.error(f"Invalid expiry_time format for order {order_id}: {expiry_time_str}")
             return False # Treat as not expired if format is bad

        # Check if expired
        if current_time >= expiry_time:
            logger.info(f"Order {order_id} has passed expiry time {expiry_time_str}. Processing expiry...")
            await self._expire_order(order)
            return True # Order was expired
        else:
             return False # Order not yet expired


    async def _check_and_process_reminders(self, order: Dict, current_time: datetime):
        """Check if order needs reminder and send if due."""
        order_id = order.get("order_id")
        created_at_str = order.get("created_at")
        if not created_at_str:
            logger.warning(f"Order {order_id} has no created_at timestamp for reminder check.")
            return

        try:
            created_at_norm = str(created_at_str).replace("Z", "+00:00")
            created_at = datetime.fromisoformat(created_at_norm)
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            logger.error(f"Invalid created_at timestamp for reminder check {order_id}: {created_at_str}")
            return

        # Calculate hours elapsed
        hours_elapsed = (current_time - created_at).total_seconds() / 3600
        sent_reminders = order.get("sent_reminders", []) # Should be list of hours

        # Find the next applicable reminder interval that hasn't been sent
        next_reminder_hour = None
        for interval_hours in self.reminder_intervals:
            if hours_elapsed >= interval_hours and interval_hours not in sent_reminders:
                next_reminder_hour = interval_hours
                break # Found the earliest due reminder

        if next_reminder_hour is not None:
            logger.info(f"Order {order_id} due for {next_reminder_hour}h reminder.")
            # Send reminder
            reminder_sent = await self._send_reminder(order, int(next_reminder_hour))

            # Update order ONLY if reminder was successfully sent (or attempted)
            if reminder_sent: # Assuming _send_reminder returns True/False
                try:
                    # Use $addToSet to prevent duplicates if run concurrently
                    await self.order_manager.update_order(
                        order_id,
                        {"$addToSet": {"sent_reminders": next_reminder_hour}}
                    )
                    logger.info(f"Marked {next_reminder_hour}h reminder sent for order {order_id}")
                except Exception as e:
                    logger.error(f"Failed to update order {order_id} after sending reminder: {e}", exc_info=True)
            else:
                 logger.warning(f"Did not mark reminder sent for {order_id} as send failed.")


    async def _send_reminder(self, order: Dict, hours_elapsed: int) -> bool:
        """
        Send reminder message to merchant via WhatsApp. Returns True on success/attempt.
        """
        merchant_id = order.get("merchant_id")
        order_id = order.get("order_id")
        if not merchant_id or not order_id:
            logger.error("Cannot send reminder: order missing merchant_id or order_id.")
            return False

        try:
            # Get merchant phone using the db instance
            merchant_data = await self.db.get_merchant(merchant_id)
            if not merchant_data:
                # Use debug level for missing merchants in dev mode (e.g., "demo")
                # Only warn if it's not a known dev/test merchant
                if merchant_id in ["demo", "test_merchant", "default_merchant"]:
                    logger.debug(f"Cannot send reminder: No merchant found for ID: {merchant_id} (dev mode)")
                else:
                    logger.warning(f"Cannot send reminder: No merchant found for ID: {merchant_id}")
                return False

            merchant_phone = merchant_data.get("phone")
            if not merchant_phone:
                logger.warning(f"Cannot send reminder: Merchant {merchant_id} has no phone number")
                return False

            # Format message
            message = self._format_reminder_message(order, hours_elapsed)

            # Send via WhatsApp using the integrations instance
            await self.integrations.send_whatsapp_message(
                phone=merchant_phone,
                text=message
            )
            logger.info(
                f"Reminder sent to merchant {merchant_id} (phone: {merchant_phone}) "
                f"for order {order_id}"
            )
            return True # Indicate successful send

        except Exception as e:
            logger.error(f"Failed sending reminder for order {order_id} to merchant {merchant_id}: {e}", exc_info=True)
            return False # Indicate send failure


    def _format_reminder_message(self, order: Dict, hours_elapsed: int) -> str:
        """
        Format reminder message for merchant.
        Plain text compatible with WhatsApp Cloud API. Emojis removed.

        Args:
            order: Order dictionary
            hours_elapsed: Hours since order creation

        Returns:
            Formatted message string
        """
        remaining_hours = max(0, self.ttl_hours - hours_elapsed)

        lines = [
            "ORDER REMINDER", # Removed emoji
            "=" * 50,
            f"Order ID: {order.get('order_id')}",
            f"Customer: {order.get('customer_name', order.get('customer_phone', 'Unknown'))}",
            f"Amount: Rs.{order.get('total_amount', 0):.2f}", # Used Rs. instead of ₹
            "",
            f"Pending for: {int(hours_elapsed)} hours",
            f"Expires in: {int(remaining_hours)} hours",
            "",
            "ITEMS:",
        ]

        # Add items
        items = order.get("items", [])
        for idx, item in enumerate(items[:5], 1):  # Show first 5 items
            qty = item.get("quantity", 0)
            name = item.get("product_name", "Unknown")
            lines.append(f"{idx}. {qty} {name}")

        if len(items) > 5:
            lines.append(f"... and {len(items) - 5} more items")

        lines.append("")
        lines.append("Please review and accept/decline from dashboard.")
        lines.append("=" * 50)

        return "\n".join(lines)

    async def _expire_order(self, order: Dict):
        """
        Expire order and notify both merchant and customer.
        Handles notification failures gracefully.

        Args:
            order: Order dictionary to expire
        """
        order_id = order.get("order_id")
        merchant_id = order.get("merchant_id")
        customer_phone = order.get("customer_phone")
        if not order_id or not merchant_id or not customer_phone:
             logger.error(f"Cannot expire order, missing critical info: {order.get('order_id')}")
             return

        try:
            # Update order status to expired using OrderManager
            expired_order = await self.order_manager.expire_order(order_id)
            if not expired_order: # expire_order might return None/False if already expired
                 logger.warning(f"Order {order_id} was possibly already expired or failed to expire.")
                 # Fetch again to be sure of status before notifying
                 expired_order = await self.order_manager.get_order(order_id)
                 if not expired_order or expired_order.get("status") != "expired":
                      logger.error(f"Failed to confirm expiry status for order {order_id}")
                      return # Avoid sending notifications if status is wrong

            logger.info(f"Order {order_id} marked as expired")

            # Notify merchant (non-blocking failure)
            asyncio.create_task(self._notify_merchant_expiry(merchant_id, order))

            # Notify customer (non-blocking failure)
            asyncio.create_task(self._notify_customer_expiry(customer_phone, order))

        except Exception as e:
            logger.error(f"Error during order expiry process for {order_id}: {e}", exc_info=True)


    async def _notify_merchant_expiry(self, merchant_id: str, order: Dict):
        """ Task to notify merchant about expiry. """
        order_id = order.get("order_id")
        try:
            merchant_data = await self.db.get_merchant(merchant_id)
            if merchant_data and merchant_data.get("phone"):
                merchant_phone = merchant_data.get("phone")
                merchant_message = (
                    f"ORDER EXPIRED\n" # Removed emoji
                    f"{'=' * 40}\n"
                    f"Order ID: {order_id}\n"
                    f"Customer: {order.get('customer_name', order.get('customer_phone'))}\n"
                    f"Amount: Rs.{order.get('total_amount', 0):.2f}\n" # Used Rs.
                    f"\n"
                    f"This order expired due to no response\n"
                    f"within {self.ttl_hours} hours.\n"
                    f"{'=' * 40}"
                )
                await self.integrations.send_whatsapp_message(
                    phone=merchant_phone,
                    text=merchant_message
                )
                logger.info(f"Expiry notification sent to merchant {merchant_id}")
            else:
                 logger.warning(f"Could not notify merchant {merchant_id} of expiry: No phone found.")
        except Exception as e:
            logger.error(f"Failed to notify merchant {merchant_id} about expired order {order_id}: {e}", exc_info=True)


    async def _notify_customer_expiry(self, customer_phone: str, order: Dict):
        """ Task to notify customer about expiry. """
        order_id = order.get("order_id")
        try:
            customer_message = (
                f"ORDER EXPIRED\n" # Removed emoji
                f"{'=' * 40}\n"
                f"Order ID: {order_id}\n"
                f"Amount: Rs.{order.get('total_amount', 0):.2f}\n" # Used Rs.
                f"\n"
                f"Unfortunately, your order expired as the\n"
                f"merchant did not respond within {self.ttl_hours} hours.\n"
                f"\n"
                f"You can place a new order anytime.\n"
                f"Sorry for the inconvenience!\n"
                f"{'=' * 40}"
            )
            await self.integrations.send_whatsapp_message(
                phone=customer_phone,
                text=customer_message
            )
            logger.info(f"Expiry notification sent to customer {customer_phone}")
        except Exception as e:
            logger.error(f"Failed to notify customer {customer_phone} about expired order {order_id}: {e}", exc_info=True)


    # This method seems redundant if _check_all_pending_orders covers it
    # async def send_immediate_reminder(self, order_id: str): ...


# Removed old setup_reminder_system function as initialization is handled in app lifespan now