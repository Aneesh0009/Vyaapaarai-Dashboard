"""
Order Confirmation Handler
Handles inventory checking and deduction when orders are accepted
"""

import logging
from typing import Dict, List, Any, Optional
from bson import ObjectId
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


async def check_inventory_availability(items: List[Dict], merchant_id: str, db_instance=None) -> Dict:
    """
    Check if all items in order are available in inventory.
    
    Returns:
        {
            "available": True/False,
            "missing_items": ["item1", "item2"],
            "details": {...}
        }
    """
    try:
        if not db_instance:
            from app import _db_instance
            db_instance = _db_instance
        
        db = db_instance.db
        missing_items = []
        details = {}
        
        for item in items:
            product_id = item.get("product_id")
            required_qty = item.get("quantity")
            
            # Get product from inventory
            product = await db["products"].find_one({
                "_id": ObjectId(product_id),
                "merchant_id": merchant_id
            })
            
            if not product:
                missing_items.append(f"{item.get('product_name')} - Not found")
                continue
            
            available_qty = product.get("stock_qty", 0)
            
            details[item.get("product_name")] = {
                "required": required_qty,
                "available": available_qty,
                "sufficient": available_qty >= required_qty
            }
            
            # Check if sufficient
            if available_qty < required_qty:
                missing_items.append(
                    f"{item.get('product_name')}: Need {required_qty}, Have {available_qty}"
                )
        
        available = len(missing_items) == 0
        
        logger.info(f"Inventory check: Available={available}, Missing={len(missing_items)}")
        
        return {
            "available": available,
            "missing_items": missing_items,
            "details": details
        }
    
    except Exception as e:
        logger.error(f"Inventory check error: {e}")
        return {
            "available": False,
            "missing_items": [f"Error checking inventory: {str(e)}"],
            "details": {}
        }


async def deduct_inventory(items: List[Dict], merchant_id: str, order_id: str, db_instance=None) -> Dict:
    """
    Deduct items from inventory when order is accepted.
    
    Returns:
        {
            "success": True/False,
            "deducted_items": [...],
            "error": "error message if failed"
        }
    """
    try:
        if not db_instance:
            from app import _db_instance
            db_instance = _db_instance
        
        db = db_instance.db
        deducted_items = []
        
        for item in items:
            product_id = item.get("product_id")
            deduct_qty = item.get("quantity")
            unit = item.get("unit")
            
            try:
                # Get current product
                product = await db["products"].find_one({
                    "_id": ObjectId(product_id),
                    "merchant_id": merchant_id
                })
                
                if not product:
                    logger.error(f"Product not found: {product_id}")
                    continue
                
                current_stock = product.get("stock_qty", 0)
                new_stock = max(0, current_stock - deduct_qty)
                
                # Update product stock
                await db["products"].update_one(
                    {"_id": ObjectId(product_id)},
                    {
                        "$set": {
                            "stock_qty": new_stock,
                            "last_updated": datetime.now(timezone.utc).isoformat()
                        }
                    }
                )
                
                # Log transaction
                await db["stock_transactions"].insert_one({
                    "product_id": str(product_id),
                    "product_name": item.get("product_name"),
                    "operation": "deduct_for_order",
                    "quantity": deduct_qty,
                    "unit": unit,
                    "old_stock": current_stock,
                    "new_stock": new_stock,
                    "reason": f"Order #{order_id[:8]} accepted",
                    "order_id": order_id,
                    "merchant_id": merchant_id,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                
                deducted_items.append({
                    "product_name": item.get("product_name"),
                    "quantity_deducted": deduct_qty,
                    "old_stock": current_stock,
                    "new_stock": new_stock
                })
                
                # Check for low stock alert
                if new_stock < product.get("min_stock", 0):
                    await db["alerts"].insert_one({
                        "merchant_id": merchant_id,
                        "title": f"{item.get('product_name')} - Stock Low",
                        "message": f"Stock: {new_stock}{unit} (Min: {product.get('min_stock')}{unit})",
                        "severity": "critical" if new_stock == 0 else "warning",
                        "alert_type": "low_stock",
                        "product_id": str(product_id),
                        "product_name": item.get("product_name"),
                        "status": "active",
                        "created_at": datetime.now(timezone.utc).isoformat()
                    })
                    logger.warning(f"Low stock alert created for {item.get('product_name')}")
                
                logger.info(f"✅ Stock deducted: {item.get('product_name')} {deduct_qty}{unit}")
            
            except Exception as e:
                logger.error(f"Error deducting item {item.get('product_name')}: {e}")
                raise
        
        logger.info(f"✅ Inventory deduction complete for order {order_id}")
        
        return {
            "success": True,
            "deducted_items": deducted_items,
            "total_items_deducted": len(deducted_items)
        }
    
    except Exception as e:
        logger.error(f"Deduct inventory error: {e}")
        return {
            "success": False,
            "deducted_items": [],
            "error": str(e)
        }


async def revert_inventory(order_id: str, merchant_id: str, db_instance=None) -> Dict:
    """
    Revert inventory if order is cancelled after confirmation.
    Useful for order cancellations.
    """
    try:
        if not db_instance:
            from app import _db_instance
            db_instance = _db_instance
        
        db = db_instance.db
        
        # Get original transaction
        transactions = await db["stock_transactions"].find({
            "order_id": order_id,
            "operation": "deduct_for_order"
        }).to_list(1000)
        
        reverted_items = []
        
        for transaction in transactions:
            product_id = transaction.get("product_id")
            quantity = transaction.get("quantity")
            
            # Get current product
            product = await db["products"].find_one({
                "_id": ObjectId(product_id),
                "merchant_id": merchant_id
            })
            
            if product:
                old_stock = product.get("stock_qty", 0)
                new_stock = old_stock + quantity
                
                # Revert stock
                await db["products"].update_one(
                    {"_id": ObjectId(product_id)},
                    {"$set": {"stock_qty": new_stock}}
                )
                
                # Log revert transaction
                await db["stock_transactions"].insert_one({
                    "product_id": product_id,
                    "product_name": product.get("product_name"),
                    "operation": "revert_for_cancelled_order",
                    "quantity": quantity,
                    "old_stock": old_stock,
                    "new_stock": new_stock,
                    "reason": f"Order #{order_id[:8]} cancelled",
                    "order_id": order_id,
                    "merchant_id": merchant_id,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                
                reverted_items.append({
                    "product_name": product.get("product_name"),
                    "quantity_reverted": quantity
                })
        
        return {
            "success": True,
            "reverted_items": reverted_items
        }
    
    except Exception as e:
        logger.error(f"Revert inventory error: {e}")
        return {
            "success": False,
            "reverted_items": [],
            "error": str(e)
        }
