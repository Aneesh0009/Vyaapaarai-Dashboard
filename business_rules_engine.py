"""
business_rules_engine.py (Enhanced v2.1)
Business rules and automated alerts for merchants.
"""

from typing import Dict, Optional, Any, List
from datetime import datetime, timedelta, timezone
import logging
from bson import ObjectId

logger = logging.getLogger(__name__)


class BusinessRulesEngine:
    """
    Manages business rules and automated merchant alerts.
    """
    def __init__(self, db_module, inventory_manager, integrations_module):
        self.db = db_module
        self.inventory = inventory_manager
        self.integrations = integrations_module
        self._alert_cooldowns: Dict[str, datetime] = {}
        logger.info("BusinessRulesEngine initialized")

    def _validate_phone(self, phone: Optional[str]) -> bool:
        return phone is not None and phone.isdigit() and 10 <= len(phone) <= 15

    async def check_stock_alert(self, merchant_id: str, product_id: str):
        """
        Check if product stock is low and send alert if needed.
        """
        merchant = await self.db.get_merchant(merchant_id)
        if not merchant:
            logger.warning(f"Merchant {merchant_id} not found for stock alert.")
            return

        threshold = merchant.get("low_stock_threshold", 10)
        try:
            threshold = int(threshold)
        except Exception:
            logger.error(f"Threshold value invalid for merchant {merchant_id}, using default 10.")
            threshold = 10

        product = await self.inventory.get_product(merchant_id, product_id)
        if not product:
            logger.warning(f"Product {product_id} not found for merchant {merchant_id}.")
            return

        try:
            current_stock = int(product.get("stock", 0))
        except Exception:
            logger.error(f"Invalid stock value for product {product_id}.")
            return

        if current_stock <= threshold:
            cooldown_key = f"{merchant_id}:{product_id}"
            now = datetime.now(timezone.utc)
            last_alert = self._alert_cooldowns.get(cooldown_key)
            if last_alert and (now - last_alert < timedelta(hours=6)):
                logger.debug(f"Alert cooldown active for {product_id}")
                return

            merchant_phone = merchant.get("phone")
            if merchant_phone and self._validate_phone(merchant_phone):
                try:
                    await self.integrations.send_low_stock_alert(
                        phone=merchant_phone,
                        product_data=product,
                        threshold=threshold
                    )
                except Exception as e:
                    logger.error(f"Failed to send low stock alert: {e}")
            else:
                logger.warning(f"Invalid merchant phone for low stock alert: {merchant_phone}")

            alert_data = {
                "merchant_id": merchant_id,
                "product_id": product_id,
                "product_name": product["product_name"],
                "alert_type": "low_stock",
                "current_stock": current_stock,
                "threshold": threshold,
                "timestamp": now.isoformat(),
                "acknowledged": False,
                "last_alert_timestamp": now.isoformat()  # For potential distributed cooldown
            }
            try:
                await self.db.insert_alert(alert_data)
            except Exception as e:
                logger.error(f"Error inserting alert into DB: {e}")

            self._alert_cooldowns[cooldown_key] = now
            logger.info(f"Low stock alert sent for {product_id} (stock: {current_stock})")

    async def get_active_alerts(self, merchant_id: str) -> list:
        """Get unacknowledged alerts for merchant."""
        try:
            return await self.db.get_active_alerts(merchant_id, limit=50)
        except Exception as e:
            logger.error(f"Error fetching active alerts: {e}")
            return []

    async def acknowledge_alert(self, alert_id: str):
        """Mark alert as acknowledged."""
        try:
            success = await self.db.acknowledge_alert(alert_id)
            if not success:
                logger.warning(f"Alert {alert_id} not found or already acknowledged.")
        except Exception as e:
            logger.error(f"Error acknowledging alert {alert_id}: {e}")

    # --- Rules CRUD to support /rules endpoints ---
    async def get_merchant_rules(self, merchant_id: str, enabled_only: bool = False) -> List[Dict[str, Any]]:
        """
        Fetch rules for a merchant from 'business_rules' collection.
        """
        collection = await self.db.get_collection("business_rules")
        query: Dict[str, Any] = {"merchant_id": merchant_id}
        if enabled_only:
            query["enabled"] = True
        cursor = collection.find(query).sort("updated_at", -1)
        rules = await cursor.to_list(length=100)
        # Convert _id to str for JSON
        for r in rules:
            if r.get("_id") is not None:
                r["_id"] = str(r["_id"])
        return rules

    async def create_rule(self, merchant_id: str, rule_type: str, rule_config: Dict[str, Any], enabled: bool = True) -> Dict[str, Any]:
        """
        Create a new rule document.
        """
        if not rule_type or not isinstance(rule_config, dict):
            raise ValueError("rule_type and rule_config are required")
        collection = await self.db.get_collection("business_rules")
        now = datetime.now(timezone.utc)
        doc = {
            "merchant_id": merchant_id,
            "rule_type": rule_type,
            "rule_config": rule_config,
            "enabled": bool(enabled),
            "created_at": now,
            "updated_at": now,
        }
        result = await collection.insert_one(doc)
        doc["_id"] = str(result.inserted_id)
        return doc

    async def update_rule(self, merchant_id: str, rule_id: str, rule_config: Optional[Dict[str, Any]] = None, enabled: Optional[bool] = None) -> Dict[str, Any]:
        """
        Update an existing rule document and return it.
        """
        if not rule_id:
            raise ValueError("rule_id is required")
        collection = await self.db.get_collection("business_rules")
        update_set: Dict[str, Any] = {"updated_at": datetime.now(timezone.utc)}
        if rule_config is not None:
            if not isinstance(rule_config, dict):
                raise ValueError("rule_config must be a dict")
            update_set["rule_config"] = rule_config
        if enabled is not None:
            update_set["enabled"] = bool(enabled)
        from bson import ObjectId
        result = await collection.find_one_and_update(
            {"_id": ObjectId(rule_id), "merchant_id": merchant_id},
            {"$set": update_set},
            return_document=True
        )
        if not result:
            raise ValueError("Rule not found or does not belong to user")
        result["_id"] = str(result["_id"])  # Convert for JSON
        return result


business_rules_engine: Optional[BusinessRulesEngine] = None
