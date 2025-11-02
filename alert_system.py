# alerts_system_v6.py
"""
Unified, role-aware alert management system for both merchants and admins.
Merges the comprehensive features of merchant_v5 (async, deduplication, 
multi-channel) with the admin-centric logic of admin_merchant_v4 
(system alerts, WebSocket push).

VERSION: 6.0.0
"""

import os
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union
from enum import Enum

from bson import ObjectId
from bson.errors import InvalidId
from db import get_db # Assuming db.py is in the PYTHONPATH

# =====================================
# CONFIGURATION
# =====================================
# Time window (in minutes) to prevent duplicate alerts
ALERT_DEDUP_WINDOW_MINUTES = int(os.getenv("ALERT_DEDUP_WINDOW_MINUTES", "5"))
# Max alerts to fetch in a single get_alerts() call
MAX_ALERTS_FETCH = int(os.getenv("MAX_ALERTS_FETCH", "100"))

# Setup structured logger
logger = logging.getLogger("alerts_v6")
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# =====================================
# ENUMS
# =====================================

class AlertPriority(Enum):
    """Defines the urgency of the alert."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertChannel(Enum):
    """Defines the delivery channel for the alert."""
    WHATSAPP = "whatsapp"
    EMAIL = "email"
    SMS = "sms"
    DASHBOARD = "dashboard" # For persistent display in the UI
    WEBSOCKET = "websocket" # For real-time push to admin/merchant UI

class AlertRole(Enum):
    """Defines the target audience role for the alert."""
    MERCHANT = "merchant"
    ADMIN = "admin"

# =====================================
# ALERT TEMPLATES
# =====================================

ALERT_TEMPLATES = {
    # == Merchant Templates (from v5) ==
    "low_stock_alert": {
        "title": "⚠️ Low Stock: {product_name}", 
        "message": "{product_name} is running low in stock (Current Quantity: {quantity})."
    },
    "restock_reminder": {
        "title": "🔔 Restock Reminder", 
        "message": "It's time to restock {product_name}. Current stock is {quantity}."
    },
    "sales_target": {
        "title": "🎯 Sales Target Update", 
        "message": "Your sales today are ₹{sales_amount} / ₹{target_amount}."
    },
    "monthly_report": {
        "title": "📊 Monthly Report Ready", 
        "message": "Your comprehensive monthly report for {month} is now available!"
    },
    
    # == Admin Templates (from v4 logic) ==
    "system_health": {
        "title": "🧩 System Health Warning", 
        "message": "Service {service_name} is experiencing high latency: {latency}ms."
    },
    "merchant_audit": {
        "title": "📋 Merchant Activity Audit", 
        "message": "Merchant {merchant_name} (ID: {merchant_id}) triggered an audit rule: {activity_description}."
    },
    "report_generated": {
        "title": "📊 New Admin Report", 
        "message": "A new admin report '{report_name}' was successfully generated at {timestamp}."
    }
}

# =====================================
# ALERT SYSTEM CLASS
# =====================================

class AlertSystem:
    """
    Centralized, async, role-aware alert management system.
    """
    def __init__(self):
        """Initializes the alert system. Currently holds no state."""
        # This lock is available for future use, e.g., for in-memory caching
        self.lock = asyncio.Lock()
        logger.debug("AlertSystem class initialized")

    def _validate_object_id(self, alert_id: str) -> ObjectId:
        """
        Validates a string and converts it to a BSON ObjectId.
        Raises ValueError if the ID is invalid.
        """
        try:
            return ObjectId(alert_id)
        except (InvalidId, TypeError, Exception) as e:
            logger.warning(f"Invalid alert_id format: {alert_id}")
            raise ValueError(f"Invalid alert_id format: {alert_id}") from e

    def _normalize_channels(
        self, 
        channels: Optional[List[Union[str, AlertChannel]]], 
        priority: AlertPriority
    ) -> List[AlertChannel]:
        """
        Determines the final list of delivery channels based on priority and user input.
        Includes new defaults for WEBSOCKET.
        """
        if channels is None:
            # Apply default channels based on priority
            if priority == AlertPriority.CRITICAL:
                return [AlertChannel.WHATSAPP, AlertChannel.EMAIL, AlertChannel.WEBSOCKET, AlertChannel.DASHBOARD]
            elif priority == AlertPriority.HIGH:
                return [AlertChannel.EMAIL, AlertChannel.WEBSOCKET, AlertChannel.DASHBOARD]
            elif priority == AlertPriority.MEDIUM:
                return [AlertChannel.DASHBOARD, AlertChannel.EMAIL]
            else: # LOW
                return [AlertChannel.DASHBOARD]
        
        normalized = set() # Use a set to avoid duplicate channels
        for ch in channels:
            try:
                if isinstance(ch, AlertChannel):
                    normalized.add(ch)
                elif isinstance(ch, str):
                    normalized.add(AlertChannel(ch))
            except ValueError:
                logger.warning(f"Unknown alert channel specified: {ch}. Skipping.")
        
        # Ensure DASHBOARD is almost always present for persistence
        if AlertChannel.DASHBOARD not in normalized and priority in [AlertPriority.LOW, AlertPriority.MEDIUM]:
             normalized.add(AlertChannel.DASHBOARD)
             
        return list(normalized) or [AlertChannel.DASHBOARD]

    async def _check_duplicate(
        self, 
        target_id: str, 
        alert_type: str, 
        role: AlertRole
    ) -> Optional[Dict[str, Any]]:
        """
        Checks if a similar alert was sent to the same target role
        within the deduplication window.
        """
        try:
            db = await get_db()
            collection = await db.get_collection("alerts")
            
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=ALERT_DEDUP_WINDOW_MINUTES)
            
            return await collection.find_one({
                "target_id": target_id, 
                "alert_type": alert_type, 
                "role": role.value, 
                "created_at": {"$gte": cutoff}
            })
        except Exception as e:
            logger.error(f"Failed to check for duplicate alerts: {e}", exc_info=True)
            return None # Fail safe - allow alert to be created

    async def create_alert(
        self, 
        target_id: str, 
        alert_type: str, 
        title: str, 
        message: str, 
        role: AlertRole, 
        priority: AlertPriority = AlertPriority.MEDIUM, 
        channels: Optional[List[Union[str, AlertChannel]]] = None, 
        metadata: Optional[Dict[str, Any]] = None, 
        check_duplicates: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Creates, stores, and dispatches a new alert for a specific target and role.
        
        Args:
            target_id: The ID of the user (e.g., merchant_id, admin_user_id).
            alert_type: A unique key for the alert (e.g., "low_stock_alert").
            title: The alert title.
            message: The alert body/message.
            role: The role of the target (AlertRole.MERCHANT or AlertRole.ADMIN).
            priority: The urgency (AlertPriority).
            channels: A list of channels to send to. Defaults based on priority if None.
            metadata: Any extra data to store with the alert.
            check_duplicates: Whether to perform deduplication.
        
        Returns:
            The created alert document as a dictionary, or None if skipped.
        """
        if check_duplicates:
            existing = await self._check_duplicate(target_id, alert_type, role)
            if existing:
                logger.info(f"Duplicate {alert_type} alert skipped for {role.value}:{target_id}")
                existing["_id"] = str(existing["_id"])
                return existing # Return the existing alert
        
        try:
            db = await get_db()
            collection = await db.get_collection("alerts")
            
            normalized_channels = self._normalize_channels(channels, priority)
            
            alert_doc = {
                "target_id": target_id,
                "role": role.value,
                "alert_type": alert_type,
                "title": title,
                "message": message,
                "priority": priority.value,
                "channels": [c.value for c in normalized_channels],
                "created_at": datetime.now(timezone.utc),
                "read": False,
                "read_at": None,
                "metadata": metadata or {},
                "delivery_status": {} # Will be populated by _send_alert
            }
            
            result = await collection.insert_one(alert_doc)
            inserted_id = result.inserted_id
            alert_doc["_id"] = str(inserted_id)
            
            # Asynchronously dispatch the alert to all channels
            # This is "fire and forget" from the create_alert perspective.
            # We will update the delivery status in the DB after sending.
            delivery_status = await self._send_alert(target_id, role, alert_doc, normalized_channels)
            
            # Update the doc in the DB with the final delivery status
            await collection.update_one(
                {"_id": inserted_id}, 
                {"$set": {"delivery_status": delivery_status}}
            )
            
            alert_doc["delivery_status"] = delivery_status
            logger.info(f"✅ Created {priority.value} alert ({alert_type}) for {role.value}:{target_id}")
            return alert_doc
            
        except Exception as e:
            logger.error(f"Failed to create alert: {e}", exc_info=True)
            return None

    async def _send_alert(
        self, 
        target_id: str, 
        role: AlertRole,
        alert_doc: Dict[str, Any], 
        channels: List[AlertChannel]
    ) -> Dict[str, Any]:
        """
        Internal dispatcher to send alerts to all specified channels.
        This version uses placeholders for external APIs (Email, SMS, WA).
        """
        status = {"sent_all": True, "channels": {}}
        
        # In a real app, you might fetch user contact info here based on target_id and role
        # e.g., user = await get_user_contact_info(target_id, role)
        
        for ch in channels:
            try:
                if ch == AlertChannel.WHATSAPP:
                    # from integrations import send_whatsapp_message
                    # await send_whatsapp_message(user.phone, alert_doc['message'])
                    logger.debug(f"Sending {ch.value} to {role.value}:{target_id}")
                    status["channels"][ch.value] = "sent" # Placeholder
                
                elif ch == AlertChannel.EMAIL:
                    # from integrations import send_email
                    # await send_email(user.email, alert_doc['title'], alert_doc['message'])
                    logger.debug(f"Sending {ch.value} to {role.value}:{target_id}")
                    status["channels"][ch.value] = "queued" # Placeholder
                
                elif ch == AlertChannel.SMS:
                    # from integrations import send_sms
                    # await send_sms(user.phone, alert_doc['message'])
                    logger.debug(f"Sending {ch.value} to {role.value}:{target_id}")
                    status["channels"][ch.value] = "queued" # Placeholder
                
                elif ch == AlertChannel.WEBSOCKET:
                    # This is the new channel for real-time UI updates
                    await self._send_websocket_alert(role, target_id, alert_doc)
                    status["channels"][ch.value] = "pushed"
                
                elif ch == AlertChannel.DASHBOARD:
                    # This is a "virtual" channel; storage in the DB is the delivery.
                    status["channels"][ch.value] = "stored"
            
            except Exception as e:
                logger.error(f"Error sending alert via {ch.value}: {e}", exc_info=True)
                status["sent_all"] = False
                status["channels"][ch.value] = f"failed: {str(e)}"
                
        return status

    async def _send_websocket_alert(
        self, 
        role: AlertRole, 
        target_id: str, 
        alert_doc: Dict[str, Any]
    ):
        """
        Placeholder for pushing alerts via WebSocket.
        In a real FastAPI app, this would use a WebSocketManager
        to broadcast the message to the relevant user(s).
        
        Example:
        from aio_app import ws_manager
        await ws_manager.broadcast_to_user(target_id, role, alert_doc)
        """
        try:
            # This is a placeholder. A real implementation would require
            # access to a shared WebSocket connection manager.
            logger.info(f"🔔 WebSocket push simulated for {role.value}:{target_id} - {alert_doc['title']}")
            # In a real app:
            # await connection_manager.send_to_user(target_id, alert_doc)
        except Exception as e:
            logger.warning(f"WebSocket alert push failed (simulation): {e}")

    async def create_alert_from_template(
        self, 
        target_id: str, 
        alert_type: str, 
        vars: Dict[str, Any], 
        role: AlertRole, 
        priority: AlertPriority = AlertPriority.MEDIUM, 
        channels: Optional[List[Union[str, AlertChannel]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Creates an alert using a predefined template.
        
        Args:
            target_id: The ID of the user (e.g., merchant_id, admin_user_id).
            alert_type: The key of the template in ALERT_TEMPLATES.
            vars: A dictionary of values to format the template strings.
            role: The role of the target (AlertRole.MERCHANT or AlertRole.ADMIN).
            priority: The urgency (AlertPriority).
            channels: A list of channels to send to. Defaults based on priority if None.
        
        Returns:
            The created alert document as a dictionary, or None if failed.
        """
        template = ALERT_TEMPLATES.get(alert_type)
        if not template:
            logger.error(f"Unknown alert template: {alert_type}")
            raise ValueError(f"Unknown alert template: {alert_type}")
            
        try:
            title = template["title"].format(**vars)
            message = template["message"].format(**vars)
        except KeyError as e:
            logger.error(f"Missing variable {e} for alert template {alert_type}")
            raise ValueError(f"Missing template variable: {e} for {alert_type}") from e
        
        return await self.create_alert(
            target_id=target_id,
            alert_type=alert_type,
            title=title,
            message=message,
            role=role,
            priority=priority,
            channels=channels,
            metadata=vars,
            check_duplicates=True # Always dedupe template alerts by default
        )

    async def get_alerts(
        self, 
        target_id: str, 
        role: AlertRole, 
        unread_only: bool = False, 
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Fetches a list of alerts for a specific target and role.
        This single function serves both merchants and admins.
        
        Args:
            target_id: The ID of the user (e.g., merchant_id, admin_user_id).
            role: The role of the target (AlertRole.MERCHANT or AlertRole.ADMIN).
            unread_only: If True, only return alerts where read == False.
            limit: The maximum number of alerts to return.
        
        Returns:
            A list of alert documents.
        """
        try:
            db = await get_db()
            coll = await db.get_collection("alerts")
            
            query = {"target_id": target_id, "role": role.value}
            if unread_only: 
                query["read"] = False
                
            # Fetch alerts, newest first
            cursor = coll.find(query).sort("created_at", -1).limit(limit)
            
            # Use MAX_ALERTS_FETCH as a safeguard for to_list
            alerts = await cursor.to_list(length=min(limit, MAX_ALERTS_FETCH))
            
            # Convert ObjectId to string for JSON serialization
            for a in alerts:
                a["_id"] = str(a["_id"])
                
            return alerts
        except Exception as e:
            logger.error(f"Failed to get alerts for {role.value}:{target_id}: {e}", exc_info=True)
            return []

    async def mark_alert_as_read(self, target_id: str, role: AlertRole, alert_id: str) -> bool:
        """
        Marks a specific alert as read.
        """
        try:
            oid = self._validate_object_id(alert_id)
            db = await get_db()
            coll = await db.get_collection("alerts")
            
            result = await coll.update_one(
                {"_id": oid, "target_id": target_id, "role": role.value},
                {"$set": {"read": True, "read_at": datetime.now(timezone.utc)}}
            )
            
            if result.modified_count == 0:
                logger.warning(f"Alert {alert_id} not found or already read for {role.value}:{target_id}")
                return False
            
            logger.debug(f"Marked alert {alert_id} as read")
            return True
        except ValueError:
            return False # Invalid ObjectId
        except Exception as e:
            logger.error(f"Failed to mark alert as read: {e}", exc_info=True)
            return False

# =====================================
# SINGLETON INSTANCE
# =====================================

_alert_instance: Optional[AlertSystem] = None
_alert_lock = asyncio.Lock()

async def get_alert_system() -> AlertSystem:
    """
    Asynchronous singleton factory for the AlertSystem.
    
    Ensures only one instance of the AlertSystem is created and shared.
    
    Returns:
        The singleton AlertSystem instance.
    """
    global _alert_instance
    if _alert_instance is None:
        async with _alert_lock:
            if _alert_instance is None:
                _alert_instance = AlertSystem()
                logger.info("Created shared AlertSystem_v6 instance")
    return _alert_instance
