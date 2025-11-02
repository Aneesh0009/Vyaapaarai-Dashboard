# integrations.py - WhatsApp Cloud API Integration (Enhanced v2.1)
"""
WhatsApp Business Cloud API integration with plain-text message templates.
Handles all outbound messaging for customer and merchant interactions.

UPDATED: Fixed markdown formatting, datetime parsing, credential validation, retry logic,
button validation, international phone support, and error handling.
VERSION: 2.1.0
"""

import logging
import os
import asyncio
import httpx
from typing import Dict, List, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO datetime string safely, handling Z suffix and timezone-naive inputs.
    
    Args:
        dt_str: ISO datetime string (may contain Z suffix or +00:00)
        
    Returns:
        Timezone-aware datetime in UTC, or None if invalid
    """
    if not dt_str:
        return None
    try:
        # Normalize Z suffix to +00:00 for fromisoformat
        normalized = str(dt_str).replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        # Ensure timezone-aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid datetime format: {dt_str} - {e}")
        return None


def _format_e164_phone(phone: str, default_country_code: str = "91") -> str:
    """
    Best-effort E.164 phone formatting without external dependencies.
    
    Handles:
    - Strip all non-digits
    - Remove leading zeros for lengths > 10
    - Prefix country code for 10-digit numbers
    - Accept 11-15 digits as valid E.164
    
    Args:
        phone: Raw phone number string
        default_country_code: Country code digits (no +)
        
    Returns:
        E.164 formatted phone digits-only string
        
    Raises:
        ValueError: If number cannot be normalized
    """
    digits = "".join(c for c in str(phone) if c.isdigit())
    
    # Trim leading zeros if longer than 10 digits
    if digits.startswith("0") and len(digits) > 10:
        while digits.startswith("0"):
            digits = digits[1:]
    
    # If 10 digits, prefix with country code
    if len(digits) == 10:
        return f"{default_country_code}{digits}"
    
    # Accept 11-15 digits as valid E.164
    if 11 <= len(digits) <= 15:
        return digits
    
    raise ValueError(f"Invalid phone number: {phone}")


class WhatsAppIntegration:
    """
    WhatsApp Business Cloud API integration.
    Provides plain-text templated messaging for customer and merchant communications.
    """
    
    def __init__(self):
        """
        Initialize WhatsApp API client with credential validation.
        
        Raises:
            ValueError: If required credentials not found in environment
        """
        self.api_version = os.getenv("WHATSAPP_API_VERSION", "v19.0")
        self.phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
        self.access_token = os.getenv("WHATSAPP_ACCESS_TOKEN")
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        self.default_country_code = os.getenv("DEFAULT_COUNTRY_CODE", "91")
        self.order_ttl_hours = int(os.getenv("ORDER_TTL_HOURS", "24"))
        
        # Validate credentials on init
        if not self.phone_number_id or not self.access_token:
            error_msg = (
                "WhatsApp credentials not configured. "
                "Set WHATSAPP_PHONE_ID and WHATSAPP_ACCESS_TOKEN environment variables."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.client = httpx.AsyncClient(timeout=30.0)
        logger.info("WhatsAppIntegration initialized with valid credentials")
    
    async def send_whatsapp_message(
        self,
        phone: str,
        text: str,
        buttons: Optional[List[Dict]] = None
    ) -> Dict:
        """
        Send plain-text message via WhatsApp with optional interactive buttons.
        
        Args:
            phone: Recipient phone number (any format; normalized to E.164)
            text: Message text (plain text; emojis and newlines allowed)
            buttons: Optional list[dict] with buttons: {"id": str, "title": str}
                    Max 3 buttons, titles truncated to 20 chars
            
        Returns:
            API response dictionary
            
        Raises:
            ValueError: On invalid inputs
            httpx.HTTPError: On non-retryable HTTP errors
        """
        # Normalize phone
        to_phone = _format_e164_phone(phone, self.default_country_code)
        
        # Base payload (text-only)
        payload: Dict = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to_phone,
            "type": "text",
            "text": {
                "preview_url": False,
                "body": text or ""
            }
        }
        
        # Add interactive reply buttons if provided
        if buttons:
            payload = self._add_interactive_buttons(payload, text, buttons)
        
        response = await self._make_api_request(payload)
        msg_id = None
        try:
            msg_id = response.get("messages", [{}])[0].get("id")
        except Exception:
            pass
        logger.info(f"Message sent to {to_phone}: {msg_id}")
        return response
    
    async def send_cart_summary(
        self,
        phone: str,
        cart_data: Dict,
        include_checkout_button: bool = True
    ) -> Dict:
        """Send formatted cart summary with optional checkout buttons."""
        message = self._format_cart_message(cart_data)
        
        buttons = None
        if include_checkout_button and cart_data.get("items"):
            buttons = [
                {"id": "confirm_order", "title": "Confirm Order"},
                {"id": "continue_shopping", "title": "Add More"},
                {"id": "clear_cart", "title": "Clear Cart"}
            ]
        
        return await self.send_whatsapp_message(phone=phone, text=message, buttons=buttons)
    
    async def send_order_confirmation(
        self,
        phone: str,
        order_data: Dict,
        merchant_name: Optional[str] = None
    ) -> Dict:
        """Send order confirmation to customer."""
        message = self._format_order_confirmation(order_data, merchant_name)
        return await self.send_whatsapp_message(phone=phone, text=message)
    
    async def send_order_receipt(
        self,
        phone: str,
        order_data: Dict
    ) -> Dict:
        """Send detailed order receipt."""
        message = self._format_order_receipt(order_data)
        return await self.send_whatsapp_message(phone=phone, text=message)
    
    async def send_order_accepted_notification(
        self,
        phone: str,
        order_data: Dict
    ) -> Dict:
        """Notify customer that merchant accepted their order."""
        estimated_delivery = order_data.get("estimated_delivery")
        delivery_time = _parse_iso_datetime(estimated_delivery)
        delivery_text = (
            f"\nEstimated Delivery: {delivery_time.strftime('%d %b, %I:%M %p')}"
            if delivery_time else ""
        )
        
        message = (
            "ORDER ACCEPTED!\n\n"
            f"Good news! Your order {order_data.get('order_id')} has been confirmed by the merchant.\n\n"
            f"Total: ₹{order_data.get('total_amount', 0):.2f}\n"
            f"Items: {order_data.get('item_count', 0)}"
            f"{delivery_text}\n\n"
            "Thank you for your order!"
        )
        
        return await self.send_whatsapp_message(phone=phone, text=message)
    
    async def send_order_declined_notification(
        self,
        phone: str,
        order_data: Dict
    ) -> Dict:
        """Notify customer that merchant declined their order."""
        decline_reason = order_data.get("decline_reason", "Merchant is unable to fulfill this order")
        
        message = (
            "ORDER DECLINED\n\n"
            f"Sorry, your order {order_data.get('order_id')} could not be accepted.\n\n"
            f"Reason: {decline_reason}\n\n"
            "You can place a new order anytime."
        )
        
        return await self.send_whatsapp_message(phone=phone, text=message)
    
    async def send_reminder_to_merchant(
        self,
        phone: str,
        order_data: Dict,
        hours_elapsed: int
    ) -> Dict:
        """Send reminder to merchant about pending order."""
        remaining = max(0, int(self.order_ttl_hours - hours_elapsed))
        
        message = (
            "ORDER REMINDER\n\n"
            f"Order ID: {order_data.get('order_id')}\n"
            f"Customer: {order_data.get('customer_name', order_data.get('customer_phone', 'Unknown'))}\n"
            f"Amount: ₹{order_data.get('total_amount', 0):.2f}\n\n"
            f"This order has been pending for {int(hours_elapsed)} hours.\n\n"
            "Please review and accept/decline from your dashboard.\n"
            f"Order will auto-expire in {remaining} hours if no action is taken."
        )
        
        return await self.send_whatsapp_message(phone=phone, text=message)
    
    async def send_order_expired_notification(
        self,
        phone: str,
        order_id: str,
        recipient_type: str = "customer"
    ) -> Dict:
        """Send order expiry notification to merchant or customer."""
        recipient_type = (recipient_type or "customer").lower()
        
        if recipient_type == "customer":
            message = (
                "ORDER EXPIRED\n\n"
                f"Unfortunately, your order {order_id} has expired as the merchant "
                f"did not respond within {self.order_ttl_hours} hours.\n\n"
                "You can place a new order anytime."
            )
        else:
            message = (
                "ORDER EXPIRED\n\n"
                f"Order {order_id} has been automatically expired due to no response "
                f"within {self.order_ttl_hours} hours."
            )
        
        return await self.send_whatsapp_message(phone=phone, text=message)
    
    async def send_low_stock_alert(
        self,
        phone: str,
        product_data: Dict,
        threshold: int
    ) -> Dict:
        """Send low stock alert to merchant."""
        message = (
            "LOW STOCK ALERT\n\n"
            f"Product: {product_data.get('product_name', 'Unknown')}\n"
            f"Current Stock: {product_data.get('quantity', product_data.get('stock', 0))} "
            f"{product_data.get('unit', 'units')}\n"
            f"Alert Threshold: {threshold}\n\n"
            "Please restock soon to avoid order rejections."
        )
        
        return await self.send_whatsapp_message(phone=phone, text=message)
    
    async def send_new_order_notification_to_merchant(
        self,
        phone: str,
        order_data: Dict
    ) -> Dict:
        """Notify merchant about new order placement."""
        items = order_data.get("items", [])
        shown = []
        for idx, item in enumerate(items[:3], 1):
            shown.append(f"{idx}. {item.get('product_name', 'Item')} - {item.get('quantity', 0)} {item.get('unit', 'pcs')}")
        if len(items) > 3:
            shown.append(f"... and {len(items) - 3} more items")
        
        message = (
            "NEW ORDER RECEIVED\n\n"
            f"Order ID: {order_data.get('order_id')}\n"
            f"Customer: {order_data.get('customer_name', order_data.get('customer_phone', 'Unknown'))}\n"
            f"Total: ₹{order_data.get('total_amount', 0):.2f}\n\n"
            "Items:\n" + "\n".join(shown) + "\n\n"
            "Please review and respond within 24 hours.\n"
            "Login to your dashboard to accept or decline."
        )
        
        return await self.send_whatsapp_message(phone=phone, text=message)
    
    def _format_cart_message(self, cart_data: Dict) -> str:
        """Format cart data into readable plain-text message."""
        items = cart_data.get("items") or []
        if not items:
            return "Your cart is empty."
        
        lines = ["YOUR CART", "=" * 50, ""]
        for idx, item in enumerate(items, 1):
            name = item.get("product_name", "Item")
            qty = item.get("quantity", 0)
            unit = item.get("unit", "pcs")
            unit_price = item.get("unit_price", 0.0)
            subtotal = item.get("subtotal", qty * unit_price)
            lines.append(f"{idx}. {name}")
            lines.append(f"   Qty: {qty} {unit} @ ₹{unit_price:.2f} each")
            lines.append(f"   Subtotal: ₹{subtotal:.2f}")
        lines.append("")
        lines.append("=" * 50)
        lines.append(f"TOTAL: ₹{cart_data.get('total', 0):.2f}")
        lines.append(f"Items: {cart_data.get('item_count', 0)}")
        lines.append("=" * 50)
        return "\n".join(lines)
    
    def _format_order_confirmation(self, order_data: Dict, merchant_name: Optional[str]) -> str:
        """Format order confirmation message (plain text)."""
        merchant_text = f" from {merchant_name}" if merchant_name else ""
        return (
            "ORDER PLACED SUCCESSFULLY\n\n"
            f"Thank you for your order{merchant_text}!\n\n"
            f"Order ID: {order_data.get('order_id')}\n"
            f"Total: ₹{order_data.get('total_amount', 0):.2f}\n"
            f"Items: {order_data.get('item_count', 0)}\n\n"
            "Your order is being reviewed by the merchant.\n"
            "You'll receive a confirmation soon.\n\n"
            f"Track your order anytime: Order Status {order_data.get('order_id')}"
        )
    
    def _format_order_receipt(self, order_data: Dict) -> str:
        """Format detailed order receipt (plain text only)."""
        created_at = _parse_iso_datetime(order_data.get("created_at"))
        status_val = order_data.get("status", "unknown")
        if hasattr(status_val, "value"):
            status_val = status_val.value
        status_text = str(status_val).upper()
        
        lines = [
            "ORDER RECEIPT",
            "=" * 50,
            f"Order ID: {order_data.get('order_id')}",
            f"Status: {status_text}",
            f"Date: {created_at.strftime('%d %b %Y, %I:%M %p') if created_at else order_data.get('created_at', 'N/A')}",
            "",
            "ITEMS:",
        ]
        
        for idx, item in enumerate(order_data.get("items", []), 1):
            name = item.get("product_name", "Item")
            qty = item.get("quantity", 0)
            unit = item.get("unit", "pcs")
            unit_price = item.get("unit_price", 0.0)
            subtotal = item.get("subtotal", qty * unit_price)
            lines.append(f"{idx}. {name}")
            lines.append(f"   Qty: {qty} {unit} @ ₹{unit_price:.2f} each")
            lines.append(f"   Subtotal: ₹{subtotal:.2f}")
        
        lines.append("=" * 50)
        lines.append(f"TOTAL: ₹{order_data.get('total_amount', 0):.2f}")
        
        if order_data.get("estimated_delivery"):
            dt = _parse_iso_datetime(order_data["estimated_delivery"])
            if dt:
                lines.append(f"Pickup: {dt.strftime('%d %b, %I:%M %p')}")
        
        if order_data.get("delivery_address"):
            lines.append(f"Delivery Address: {order_data['delivery_address']}")
        
        if order_data.get("notes"):
            lines.append(f"Notes: {order_data['notes']}")
        
        lines.append("=" * 50)
        return "\n".join(lines)
    
    def _add_interactive_buttons(
        self,
        payload: Dict,
        message: str,
        buttons: List[Dict]
    ) -> Dict:
        """
        Validate and add interactive reply buttons to payload.
        
        - Max 3 buttons per message (Cloud API limitation)
        - Each button requires 'id' and 'title'
        - Titles truncated to 20 chars (platform requirement)
        - Warnings logged for truncations
        """
        if not isinstance(buttons, list):
            raise ValueError("buttons must be a list of dicts")
        
        if len(buttons) > 3:
            logger.warning(f"Max 3 buttons allowed; truncating from {len(buttons)} to 3")
            buttons = buttons[:3]
        
        safe_buttons = []
        for btn in buttons:
            if not isinstance(btn, dict) or "id" not in btn or "title" not in btn:
                raise ValueError(f"Invalid button structure: {btn}")
            title = str(btn["title"])
            if len(title) > 20:
                logger.warning(f"Button title truncated from {len(title)} to 20 chars")
            safe_buttons.append(
                {
                    "type": "reply",
                    "reply": {"id": str(btn["id"]), "title": title[:20]}
                }
            )
        
        payload["type"] = "interactive"
        payload["interactive"] = {
            "type": "button",
            "body": {"text": message},
            "action": {"buttons": safe_buttons}
        }
        payload.pop("text", None)
        return payload
    
    async def _make_api_request(self, payload: Dict) -> Dict:
        """
        Make API request to WhatsApp Cloud API with retry logic.
        
        Retries:
        - Up to 3 attempts
        - Exponential backoff: 2s, 4s
        - Retries on HTTP 429 (rate limit) and 503 (service unavailable)
        - No retry on permanent errors (4xx except 429)
        
        Raises:
            httpx.HTTPError: On non-retryable errors after retries exhausted
        """
        if not self.phone_number_id or not self.access_token:
            raise ValueError("WhatsApp credentials not set")
        
        url = f"{self.base_url}/{self.phone_number_id}/messages"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                resp = await self.client.post(url, json=payload, headers=headers)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                body_text = e.response.text
                # Retry on transient errors
                if status in (429, 503):
                    if attempt < attempts:
                        wait_s = 2 ** attempt
                        logger.warning(
                            f"Transient WhatsApp error {status}; "
                            f"retrying in {wait_s}s (attempt {attempt}/{attempts})"
                        )
                        await asyncio.sleep(wait_s)
                        continue
                # Handle permanent error: recipient not in allowed list (131030)
                try:
                    body_json = e.response.json()
                except Exception:
                    body_json = None
                if status == 400 and isinstance(body_json, dict):
                    err = body_json.get("error", {})
                    if str(err.get("code")) == "131030":
                        logger.error(f"WhatsApp API error {status} (recipient not allowed): {body_text}")
                        # Return a structured response instead of raising
                        return {
                            "status": "error",
                            "error_code": 131030,
                            "message": "Recipient phone number not in allowed list",
                            "raw": body_json,
                        }
                logger.error(f"WhatsApp API error {status}: {body_text}")
                raise
            except httpx.HTTPError as e:
                # Network errors: retry once
                if attempt < attempts:
                    wait_s = 2 ** attempt
                    logger.warning(
                        f"Network error; retrying in {wait_s}s (attempt {attempt}/{attempts}): {e}"
                    )
                    await asyncio.sleep(wait_s)
                    continue
                logger.error(f"Network error (final): {e}")
                raise
        
        raise httpx.HTTPError("WhatsApp API request failed after retries")
    
    async def parse_incoming_message(self, webhook_data: Dict) -> Optional[Dict]:
        """
        Parse incoming WhatsApp webhook payload.
        
        Returns None if:
        - Webhook is a status update (not a message)
        - No messages in payload
        - Parsing error
        """
        try:
            entry = (webhook_data or {}).get("entry", [{}])[0]
            changes = entry.get("changes", [{}])[0]
            value = changes.get("value", {})
            
            # Check if this is a status update, not a message
            if "messages" not in value and "statuses" in value:
                logger.debug("Received webhook status update (not a message)")
                return None
            
            messages = value.get("messages", [])
            if not messages:
                logger.debug("No messages in webhook payload")
                return None
            
            message = messages[0]
            logger.info(f"Parsed message {message.get('id')} from {message.get('from')}")
            
            return {
                "message_id": message.get("id"),
                "from": message.get("from"),
                "timestamp": message.get("timestamp"),
                "type": message.get("type"),
                "text": message.get("text", {}).get("body", ""),
                "interactive": message.get("interactive"),
                "context": message.get("context"),
            }
        except Exception as e:
            logger.error(f"Error parsing webhook: {e}", exc_info=True)
            return None
    
    async def close(self):
        """Close HTTP client connection."""
        await self.client.aclose()
        logger.info("WhatsApp client closed")


# Global integration instance (lazy initialization)
whatsapp: Optional[WhatsAppIntegration] = None


def get_whatsapp() -> WhatsAppIntegration:
    """
    Get or create global WhatsApp integration instance.
    
    Returns:
        WhatsAppIntegration instance
        
    Raises:
        ValueError: If credentials not configured
    """
    global whatsapp
    if whatsapp is None:
        whatsapp = WhatsAppIntegration()
    return whatsapp
