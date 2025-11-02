# dashboard_app_v6.py - Unified Admin & Merchant Dashboard
"""
VyaapaarAI Dashboard v6.0.0 UNIFIED
Merges v5 (complete merchant) and v4 (admin portal) into one application.
- Single login, routes to Admin or Merchant portal based on user role.
- Preserves all v5 merchant functionality (Orders, Chat, Alerts, etc.).
- Integrates all v4 admin functionality (Merchant Management, System Stats).
- Unified API request handling and session management.
"""

import streamlit as st
import httpx
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta, timezone, time as dt_time
from typing import Optional, Dict, List
import json
import os
import logging
import requests
import base64 # For JWT decoding
import time # For admin page UX

# ==========================================
# V5 UTILS (Missing file placeholders)
# ==========================================

def validate_phone_number(phone: str) -> bool:
    """Placeholder for phone validation."""
    if phone and isinstance(phone, str) and len(phone) > 10:
        return True
    return False # Simple check

def format_phone_number(phone: str) -> str:
    """Placeholder for phone formatting."""
    return str(phone).strip().replace("+", "")

# ==========================================
# CONFIGURATION
# ==========================================

# API Configuration
if 'API_BASE_URL' not in globals():
    API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

DEFAULT_MERCHANT_ID = os.getenv("DEFAULT_MERCHANT_ID", "demo")

# Unified session state keys
SESSION_STATE_KEYS = [
    "auth_token", 
    "username", 
    "logged_in", 
    "unread_alerts", 
    "pending_orders_count", 
    "merchant_id", 
    "role"
]

# Initialize session state
for key in SESSION_STATE_KEYS:
    if key not in st.session_state:
        if key == "logged_in":
            st.session_state[key] = False
        elif key in ["unread_alerts", "pending_orders_count"]:
            st.session_state[key] = 0
        elif key == "merchant_id":
            st.session_state[key] = DEFAULT_MERCHANT_ID
        else:
            st.session_state[key] = None

# Configure logger
logger = logging.getLogger("dashboard_v6")
if not logger.hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

# ==========================================
# TEMPLATES (From v5)
# ==========================================

def get_low_stock_alert_template(threshold: float) -> dict:
    """Template for low stock alert rule."""
    try:
        t = float(threshold)
    except Exception:
        t = 20.0
    return {
        "rule_config": {
            "type": "low_stock",
            "threshold": t,
            "channels": ["whatsapp", "dashboard"],
            "send_immediately": True
        }
    }

def get_restock_reminder_template(frequency: str) -> dict:
    """Template for restock reminder rule."""
    freq_map = {
        "daily": "0 09:00",
        "weekly": "mon 09:00",
        "once": "today 09:00"
    }
    return {
        "rule_config": {
            "type": "restock_reminder",
            "frequency": frequency,
            "schedule": freq_map.get(frequency, "daily"),
            "channels": ["whatsapp", "dashboard"]
        }
    }

def get_sales_target_template(amount: float, frequency: str) -> dict:
    """Template for sales target rule."""
    return {
        "rule_config": {
            "type": "sales_target",
            "target_amount": float(amount),
            "frequency": frequency,
            "channels": ["dashboard"],
            "trigger_on": "sales_milestone"
        }
    }

def get_monthly_report_template(day: int, send_time: str) -> dict:
    """Template for monthly report rule."""
    return {
        "rule_config": {
            "type": "monthly_report",
            "send_date": int(day),
            "send_time": send_time,
            "channels": ["email", "dashboard"],
            "include_metrics": ["sales", "orders", "customers", "products"]
        }
    }

# ==========================================
# UI & CSS (From v5)
# ==========================================

def set_error(msg: str):
    """Persist error message across reruns."""
    try:
        st.session_state["last_error"] = msg
    except Exception:
        pass
    st.error(msg)

def clear_error():
    """Clear persisted error message."""
    if "last_error" in st.session_state:
        del st.session_state["last_error"]

def format_dt(dt: Optional[datetime]) -> str:
    """Format datetime: 'Oct 27, 2025 09:15 PM'."""
    if not dt:
        return "N/A"
    try:
        return dt.strftime("%b %d, %Y %I:%M %p")
    except Exception:
        return str(dt)

# Page configuration
st.set_page_config(
    page_title="VyaapaarAI Dashboard",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for mobile responsiveness
st.markdown("""
<style>
/* Mobile-first responsive design */
@media (max-width: 768px) {
    .main .block-container {
        padding: 1rem;
        max-width: 100%;
    }
    .stMetric {
        font-size: 0.8rem;
    }
    [data-testid="stDataFrame"] {
        font-size: 0.8rem;
    }
}

/* Compact metric cards */
.metric-card {
    background: white;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #1f77b4;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    margin-bottom: 1rem;
}

/* Button styling */
.stButton > button {
    width: 100%;
    border-radius: 0.5rem;
    border: none;
    background: linear-gradient(90deg, #1f77b4, #17becf);
    color: white;
    font-weight: 600;
}

/* Alert badges */
.alert-badge {
    display: inline-block;
    padding: 0.25rem 0.5rem;
    border-radius: 0.25rem;
    font-size: 0.75rem;
    font-weight: 600;
}

.alert-critical { background: #dc3545; color: white; }
.alert-high { background: #fd7e14; color: white; }
.alert-medium { background: #ffc107; color: black; }
.alert-low { background: #6c757d; color: white; }

/* Status indicators */
.status-active { color: #28a745; font-weight: bold; }
.status-inactive { color: #dc3545; font-weight: bold; }
.status-pending { color: #ffc107; font-weight: bold; }

/* Input styling */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
.stNumberInput > div > div > input {
    border-radius: 0.5rem;
    border: 1px solid #ddd;
}

/* Sidebar styling */
[data-testid="stSidebar"] {
    padding: 1rem;
}

/* Order notification */
.order-notification {
    position: fixed;
    top: 80px;
    right: 20px;
    background: white;
    border: 3px solid #28a745;
    border-radius: 10px;
    padding: 20px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    z-index: 9999;
    animation: slideIn 0.3s ease-out;
}

@keyframes slideIn {
    from { transform: translateX(400px); opacity: 0; }
    to { transform: translateX(0); opacity: 1; }
}

/* Hide Streamlit branding (keep header visible so sidebar toggle is accessible) */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: visible;}
</style>
""", unsafe_allow_html=True)

# Persistent error banner
if st.session_state.get("last_error"):
    col_e1, col_e2 = st.columns([6, 1])
    with col_e1:
        st.error(st.session_state["last_error"])
    with col_e2:
        if st.button("Dismiss", key="dismiss_error"):
            clear_error()

# ==========================================
# API & UTILITY FUNCTIONS (From v4_admin)
# ==========================================

def _parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime string safely, handling Z suffix and timezone-naive inputs."""
    if not dt_str:
        return None
    try:
        normalized = str(dt_str).replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid datetime format: {dt_str} - {e}")
        return None

def extract_response_data(response: dict, key: str, default=None):
    """Safely extract data from API response dictionary."""
    if not isinstance(response, dict):
        logger.error(f"API response was not a dictionary: {type(response)}")
        return default
    data = response.get(key)
    if data is None:
        if key not in response:
            logger.warning(f"API response missing expected key '{key}'")
        return default
    return data

def is_low_stock(item: dict) -> bool:
    """Check if inventory item is below reorder level."""
    try:
        quantity = float(item.get("quantity", 0))
        reorder_level = float(item.get("reorder_level", 0))
        return reorder_level > 0 and quantity < reorder_level
    except (TypeError, ValueError):
        logger.warning(f"Could not compare stock levels for item {item.get('product_id')}")
        return False

def make_api_request(endpoint: str, method: str = "GET", data: dict = None, token: str = None):
    """Make API request to FastAPI backend with improved error handling."""
    try:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        
        # Ensure endpoint starts with a slash
        if not endpoint.startswith('/'):
            endpoint = f'/{endpoint}'

        url = f"{API_BASE_URL}{endpoint}" # API_BASE_URL is "http://localhost:8000"
        logger.debug(f"API Request: {method} {url} Data: {data}")
        
        with httpx.Client(timeout=30.0) as client:
            if method == "GET":
                response = client.get(url, headers=headers)
            elif method == "POST":
                response = client.post(url, json=data, headers=headers)
            elif method == "PUT":
                response = client.put(url, json=data, headers=headers)
            elif method == "DELETE":
                response = client.delete(url, headers=headers)
            else:
                st.error(f"Unsupported HTTP method: {method}")
                return None
            
            logger.debug(f"API Response Status: {response.status_code}")
            response.raise_for_status()
            
            if response.status_code == 204:
                return {"status": "success", "detail": "Operation successful (No Content)"}
            
            try:
                return response.json()
            except json.JSONDecodeError:
                logger.warning(f"API response for {method} {url} not JSON. Status: {response.status_code}")
                return {"status": "success", "content": response.text}
    
    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        try:
            error_detail = e.response.json().get("detail", e.response.text)
        except json.JSONDecodeError:
            error_detail = e.response.text
        
        logger.error(f"HTTP Error {status_code} for {method} {url}: {error_detail}", exc_info=False)
        
        if status_code in [401, 403]:
            st.error(f"Authentication Error ({status_code}): {error_detail}. Please log in again.")
            for key in list(st.session_state.keys()):
                # Keep merchant_id if it exists, clear others
                if key not in ["merchant_id"]:
                    del st.session_state[key]
            st.session_state.logged_in = False
            st.rerun() # This is what causes the login loop
        else:
            st.error(f"API Error ({status_code}): {error_detail}")
            return None
    
    except httpx.TimeoutException:
        logger.error(f"Request timed out: {method} {url}")
        st.error("Request timed out. The backend server might be busy or unavailable.")
        return None
    except httpx.ConnectError:
        logger.critical(f"Connection Error: Could not connect to API at {API_BASE_URL}")
        st.error(f"Connection Error: Cannot connect to the API backend ({API_BASE_URL}). Is it running?")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during API request to {endpoint}: {e}", exc_info=True)
        st.error(f"An unexpected error occurred: {e}")
        return None

@st.cache_data(ttl=60, show_spinner="Fetching data...")
def cached_get(endpoint: str, token: str):
    """Cached read-only API GET."""
    return make_api_request(endpoint, method="GET", token=token)

def _paginate(items, page, page_size):
    """Helper for simple list pagination."""
    total = len(items) if items else 0
    page_size = max(1, page_size)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end] if items else []
    start_display = start + 1 if total > 0 else 0
    end_display = min(end, total)
    return page_items, start_display, end_display, total, total_pages

def _stream_response(text: str):
    """Generator to stream response text character by character"""
    for char in (text or ""):
        yield char
        time.sleep(0.01) # Add a small delay for streaming effect

# ==========================================
# ORDER NOTIFICATION SYSTEM (From v5)
# ==========================================

def check_new_orders():
    """Check for new pending orders and trigger notification."""
    if not st.session_state.get("logged_in"):
        return
    
    try:
        # Always check specifically for pending confirmations, independent of UI filter
        _statuses = ["pending_confirmation", "pending"]
        _statuses_param = ",".join(_statuses)
        # MERGE FIX: Ensure correct endpoint with /api prefix
        _endpoint = f"/api/orders?statuses={_statuses_param}"
        
        # Only filter by merchant_id if one is set (for merchant role)
        merchant_id = st.session_state.get("merchant_id")
        if merchant_id:
            _endpoint += f"&merchant_id={merchant_id}"
        
        orders_response = make_api_request(_endpoint, token=st.session_state.auth_token)
        if orders_response:
            orders = extract_response_data(orders_response, "orders", [])
            new_count = len(orders)
            
            old_count = st.session_state.get("pending_orders_count", 0)
            if new_count > old_count and new_count > 0:
                st.session_state["show_order_notification"] = True
                st.session_state["new_order_data"] = orders[0]
            
            st.session_state["pending_orders_count"] = new_count
    except Exception as e:
        logger.error(f"Error checking orders: {e}")

def show_order_notification_popup():
    """Display order notification with accept/decline options."""
    if not st.session_state.get("show_order_notification"):
        return
    
    order_data = st.session_state.get("new_order_data")
    if not order_data:
        return
    
    with st.expander("🔔 NEW ORDER RECEIVED!", expanded=True):
        st.markdown("### 📦 New Order Alert")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown(f"**Order ID:** `{order_data.get('order_id', 'N/A')}`")
            st.markdown(f"**Customer:** {order_data.get('customer_name', order_data.get('customer_phone', 'Unknown'))}")
            st.markdown(f"**Phone:** {order_data.get('customer_phone', 'N/A')}")
            st.markdown(f"**Total Amount:** ₹{order_data.get('total_amount', 0):.2f}")
            
            st.markdown("**Items Requested:**")
            items = order_data.get('items', [])
            for idx, item in enumerate(items, 1):
                st.markdown(f"{idx}. **{item.get('product_name')}**: {item.get('quantity')} {item.get('unit', 'pcs')} @ ₹{item.get('unit_price', 0):.2f}")
        
        with col2:
            st.metric("Items", order_data.get('item_count', len(items)))
        
        col_accept, col_decline, col_close = st.columns([1, 1, 1])
        
        merchant_id = st.session_state.get("merchant_id", DEFAULT_MERCHANT_ID)
        
        with col_accept:
            if st.button("✅ ACCEPT ORDER", key="notif_accept", use_container_width=True, type="primary"):
                with st.spinner("Accepting order..."):
                    accept_data = {"merchant_id": merchant_id}
                    # MERGE FIX: Ensure correct endpoint with /api prefix
                    response = make_api_request(
                        f"/api/orders/{order_data['order_id']}/accept",
                        "POST",
                        accept_data,
                        st.session_state.auth_token
                    )
                
                if response and response.get("status") == "success":
                    st.success("✅ Order accepted! Inventory updated.")
                    st.session_state["show_order_notification"] = False
                    st.cache_data.clear()
                    st.rerun()
        
        with col_decline:
            if st.button("❌ DECLINE", key="notif_decline", use_container_width=True):
                st.session_state["show_decline_reason_form"] = True
        
        with col_close:
            if st.button("Close", key="notif_close", use_container_width=True):
                st.session_state["show_order_notification"] = False
                st.rerun()
        
        # Decline reason form
        if st.session_state.get("show_decline_reason_form"):
            with st.form("decline_reason_form"):
                reason = st.text_input("Decline Reason*", placeholder="e.g., Out of stock, closed today")
                submit_decline = st.form_submit_button("Confirm Decline")
                
                if submit_decline and reason:
                    with st.spinner("Declining order..."):
                        decline_data = {
                            "merchant_id": merchant_id,
                            "reason": reason
                        }
                        # MERGE FIX: Ensure correct endpoint with /api prefix
                        response = make_api_request(
                            f"/api/orders/{order_data['order_id']}/decline",
                            "POST",
                            decline_data,
                            st.session_state.auth_token
                        )
                    
                    if response and response.get("status") == "success":
                        st.warning("Order declined. Inventory restored.")
                        st.session_state["show_order_notification"] = False
                        st.session_state["show_decline_reason_form"] = False
                        st.cache_data.clear()
                        st.rerun()

def render_pending_order_alert():
    """(From v5) Display the global pending order bar"""
    try:
        merchant_id = st.session_state.get("merchant_id", DEFAULT_MERCHANT_ID)
        headers = {}
        token = st.session_state.get("auth_token") or st.session_state.get("token", "")
        if token:
            headers["Authorization"] = f"Bearer {token}"

        # MERGE FIX: Use /api prefix
        resp = requests.get(
            f"{API_BASE_URL}/api/orders/pending",
            params={"merchant_id": merchant_id},
            headers=headers,
            timeout=10,
        )
        if resp.status_code != 200:
            return
        data = resp.json() if resp.text else {}
        orders = data.get("orders", [])
        if not orders:
            return # No pending orders, don't show anything

        # Show the alert for the first pending order
        order = orders[0]
        order_id = order.get("order_id") or order.get("_id", "N/A")
        customer_phone = order.get("customer_phone", "N/A")
        total_price = order.get("total_amount", order.get("total_price", 0))
        items = order.get("items", [])
        items_count = len(items)
        created_at = order.get("created_at", "N/A")

        c1, c2, c3 = st.columns([0.55, 0.22, 0.23])
        with c1:
            st.markdown(
                f"""
                <div style="background-color:#FF5757;padding:15px 20px;border-radius:8px;color:white;font-weight:600;border-left:5px solid #FF0000;">
                🔔 <b>NEW ORDER: {order_id}</b><br>
                <small>From: {customer_phone} | ₹{total_price} | {items_count} items</small>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with c2:
            if st.button("✅ Accept", key=f"accept_top_{order_id}", use_container_width=True):
                # MERGE FIX: Use /api prefix
                r = requests.post(
                    f"{API_BASE_URL}/api/orders/accept",
                    params={"order_id": order_id, "merchant_id": merchant_id},
                    headers=headers,
                    timeout=10,
                )
                if r.status_code == 200:
                    st.success(f"✅ Order {order_id} accepted!")
                    st.balloons()
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"❌ Failed to accept: HTTP {r.status_code}")
        with c3:
            if st.button("❌ Decline", key=f"decline_top_{order_id}", use_container_width=True):
                # MERGE FIX: Use /api prefix
                rj = requests.post(
                    f"{API_BASE_URL}/api/orders/reject",
                    params={"order_id": order_id, "merchant_id": merchant_id},
                    headers=headers,
                    timeout=10,
                )
                if rj.status_code == 200:
                    st.warning(f"❌ Order {order_id} declined")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"❌ Failed to decline: HTTP {rj.status_code}")

        with st.expander("📋 View Order Details"):
            st.write(f"**Order ID:** {order_id}")
            st.write(f"**Customer:** {customer_phone}")
            st.write(f"**Total:** ₹{total_price}")
            st.write(f"**Created:** {created_at}")
            st.write("**Items:**")
            for idx, it in enumerate(items, 1):
                st.write(f"{idx}. {it.get('product_name','N/A')} x {it.get('quantity',0)} @ ₹{it.get('unit_price', it.get('price',0))}/unit")
        st.markdown("---")
    except requests.exceptions.ConnectionError:
        st.error("❌ Cannot connect to backend for pending order check")
    except Exception as e:
        logger.error(f"Error in pending alert: {e}")
        st.warning(f"⚠️ Error loading pending orders: {e}")

# ==========================================
# AUTH & LOGIN (From v4_admin)
# ==========================================

def login_page():
    """Render login page"""
    st.markdown("<div style='text-align: center;'>", unsafe_allow_html=True)
    st.markdown("# 🚀 VyaapaarAI Dashboard")
    st.markdown("### Your WhatsApp Business Assistant")
    st.markdown("</div>", unsafe_allow_html=True)
    st.divider()
    
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        with st.form("login_form"):
            st.markdown("#### Please Login")
            username = st.text_input("Username", placeholder="Enter username", key="login_user")
            password = st.text_input("Password", type="password", placeholder="Enter password", key="login_pass")
            submit = st.form_submit_button("🔐 Login", use_container_width=True)
            
            if submit:
                if username and password:
                    login_data = {"username": username, "password": password}
                    with st.spinner("Authenticating..."):
                        # Auth endpoint is at the root
                        response = make_api_request("/auth/login", "POST", login_data)
                    
                    if response and isinstance(response, dict) and "access_token" in response:
                        # Store all auth info
                        st.session_state.logged_in = True
                        st.session_state.auth_token = response["access_token"]
                        st.session_state.username = username
                        role = response.get("role")
                        st.session_state.role = role # Store the user's role

                        if role == "merchant":
                            st.session_state.merchant_id = response.get("merchant_id")
                        else:
                            st.session_state.merchant_id = None # Admin
                        
                        st.success("✅ Login successful! Loading dashboard...")
                        time.sleep(1) # Give time for success message to show
                        st.rerun()
                else:
                    st.warning("Please enter both username and password.")
    
    with col2:
        st.info(f"API: `{API_BASE_URL}`", icon="ℹ️")

# ========================================================
# 🧑‍💼 MERCHANT PAGES (Ported from v5_merchant)
# ========================================================

def overview_page():
    """Render overview/analytics page"""
    st.markdown("# 📊 Business Overview")
    
    # MERGE FIX: Add /api prefix
    overview_data = cached_get(f"/api/overview?merchant_id={st.session_state.merchant_id}", st.session_state.auth_token)
    if not overview_data:
        set_error("Could not load overview data.")
        return
    
    # Metrics row
    st.markdown("---")
    cols = st.columns(4)
    with cols[0]:
        st.metric("Total Messages", overview_data.get("total_messages", 0),
                  delta=f"{overview_data.get('today_messages', 0)} today")
    with cols[1]:
        st.metric("Unique Customers", overview_data.get("unique_customers", 0))
    with cols[2]:
        st.metric("Products", overview_data.get("total_products", 0))
    with cols[3]:
        top_intent_data = overview_data.get("top_intents", [])
        top_intent = top_intent_data[0] if top_intent_data else {}
        st.metric("Top Intent", str(top_intent.get("intent", "N/A")).replace("_", " ").title(),
                  delta=f"{top_intent.get('count', 0)} mentions")
    st.markdown("---")
    
    # Charts section
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("### 📈 Daily Activity (Last 7 Days)")
        daily_activity = extract_response_data(overview_data, "daily_activity", [])
        if daily_activity:
            try:
                df_activity = pd.DataFrame(daily_activity)
                df_activity['date'] = pd.to_datetime(df_activity['date'])
                df_activity = df_activity.sort_values(by="date")
                fig_line = px.line(df_activity, x="date", y="messages", title="Messages per Day", markers=True,
                                   labels={"date": "Date", "messages": "Messages"})
                fig_line.update_layout(height=350, showlegend=False, margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig_line, use_container_width=True)
            except Exception as e:
                set_error(f"Error plotting daily activity: {e}")
                st.dataframe(daily_activity)
        else:
            st.info("No activity data for the last 7 days.")
    
    with col2:
        st.markdown("### 🎯 Top Intents")
        top_intents = extract_response_data(overview_data, "top_intents", [])
        if top_intents:
            try:
                df_intents = pd.DataFrame(top_intents)
                df_intents['intent_display'] = df_intents['intent'].apply(lambda x: str(x).replace("_", " ").title())
                fig_pie = px.pie(df_intents, values="count", names="intent_display", title="Intent Distribution")
                fig_pie.update_layout(height=350, showlegend=True, margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig_pie, use_container_width=True)
            except Exception as e:
                st.error(f"Error plotting intents: {e}")
                st.dataframe(top_intents)
        else:
            st.info("No intent data available.")
    
    st.divider()
    
    # Recent activity
    st.markdown("### ⚡ Recent Messages (Last 5)")
    merchant_id = st.session_state.get("merchant_id", DEFAULT_MERCHANT_ID)
    # MERGE FIX: Add /api prefix
    messages_data = cached_get(f"/api/messages?limit=5&merchant_id={merchant_id}", st.session_state.auth_token)
    messages = extract_response_data(messages_data, "messages", [])
    
    if messages:
        for msg in messages:
            with st.container():
                col1, col2, col3 = st.columns([3, 5, 2])
                with col1:
                    from_display = msg.get('customer_name') or msg.get('customer_phone') or msg.get('user_phone', 'Unknown')
                    st.write(f"**From:** `{from_display}`")
                    st.write(f"**Intent:** `{str(msg.get('intent', 'Unknown')).title()}`")
                    st.caption(f"Direction: {str(msg.get('direction', 'inbound')).title()}")
                with col2:
                    incoming = (
                        msg.get('message_text')
                        or msg.get('incoming_text')
                        or msg.get('processed_text')
                        or f"({msg.get('message_type', 'text')})"
                    )
                    st.caption("Incoming:")
                    st.markdown(f"> {incoming[:100]}{'...' if len(incoming)>100 else ''}")
                    reply = msg.get('reply_text', '')
                    if reply:
                        st.caption("Reply:")
                        st.markdown(f"> _{reply[:100]}{'...' if len(reply)>100 else ''}_")
                with col3:
                    timestamp_str = msg.get('timestamp')
                    if timestamp_str:
                        dt = _parse_iso_datetime(timestamp_str)
                        if dt:
                            st.write(f"**{format_dt(dt)}**")
                    else:
                        st.write("N/A")
                st.divider()
    else:
        st.info("No recent messages.")

def orders_page():
    """ (From v5) Render orders page"""
    st.markdown("# 📦 Orders")
    st.markdown("---")

    try:
        merchant_id = st.session_state.get("merchant_id", DEFAULT_MERCHANT_ID)
        
        # MERGE FIX: Endpoint is already correct in v5
        api_url = f"{API_BASE_URL}/api/orders?merchant_id={merchant_id}"
        logger.info(f"Fetching orders from: {api_url}")

        token = st.session_state.get("auth_token", "")
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        response = requests.get(api_url, timeout=10, headers=headers)

        if response.status_code == 200:
            data = response.json() if response.text else {}
            orders = data.get("orders", [])
            st.info(f"📊 Total orders: {len(orders)}")

            if orders:
                for order in orders:
                    order_id = order.get("order_id", order.get("_id", "N/A"))
                    status = order.get("status", "unknown")
                    total = order.get("total_price", order.get("total_amount", 0))
                    customer = order.get("customer_phone", order.get("customer_name", "N/A"))

                    with st.expander(
                        f"Order {order_id} | {status} | ₹{total} | {customer}",
                        expanded=False
                    ):
                        st.write(f"**Customer:** {customer}")
                        st.write(f"**Status:** {status}")
                        st.write(f"**Total:** ₹{total}")
                        st.write(f"**Created:** {order.get('created_at')}")

                        st.subheader("Items:")
                        for item in order.get("items", []):
                            st.write(
                                f"- {item.get('product_name')} x {item.get('quantity')} @ ₹{item.get('unit_price', item.get('price'))}"
                            )
            else:
                st.warning("No orders found")

        elif response.status_code == 404:
            st.error("❌ 404: Endpoint not found")
        else:
            st.error(f"❌ HTTP {response.status_code}: {response.text[:100]}")

    except requests.exceptions.ConnectionError:
        st.error("❌ Cannot connect to backend")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")

def messages_page():
    """Render messages management page"""
    st.markdown("# 💬 Messages")
    
    with st.expander("🚀 Quick Send Message"):
        with st.form("send_message_form"):
            col1, col2 = st.columns([2, 1])
            with col1:
                recipient = st.text_input("Recipient Phone", placeholder="Include country code e.g., 91...")
                message = st.text_area("Message", placeholder="Type message...", height=100)
            with col2:
                st.markdown("<br/>"*2, unsafe_allow_html=True)
                send_button = st.form_submit_button("📤 Send Message", use_container_width=True)
            
            if send_button:
                if recipient and message:
                    if not validate_phone_number(recipient):
                        st.error("Invalid phone number format.")
                    else:
                        with st.spinner("Sending message..."):
                            send_data = {
                                "phone": recipient, 
                                "message": message,
                                "merchant_id": st.session_state.merchant_id # Add merchant_id
                            }
                            # MERGE FIX: Add /api prefix
                            response = make_api_request("/api/send_message", "POST", send_data, st.session_state.auth_token)
                        if response and response.get("status") == "success":
                            st.success(f"✅ Message sent! Details: {response.get('detail', '')}")
                            st.cache_data.clear()
                else:
                    st.warning("Please enter both phone number and message.")
    
    st.divider()
    st.markdown("### 📋 Recent Conversations")
    col1, col2 = st.columns([3, 1])
    with col1:
        phone_filter = st.text_input("🔍 Filter by Phone", placeholder="Enter full number (e.g., 91...)")
    with col2:
        limit = st.selectbox("Show", [10, 25, 50, 100], index=1, label_visibility="collapsed")
    
    merchant_id = st.session_state.get("merchant_id", DEFAULT_MERCHANT_ID)
    # MERGE FIX: Add /api prefix
    endpoint = f"/api/messages?limit={limit}&merchant_id={merchant_id}"
    if phone_filter:
        formatted_phone = format_phone_number(phone_filter)
        if formatted_phone:
            endpoint += f"&phone={formatted_phone}"
            st.caption(f"Filtering for: `{formatted_phone}`")
        else:
            st.warning("Invalid phone filter format. Showing all.")
    
    messages_data = cached_get(endpoint, st.session_state.auth_token)
    messages = extract_response_data(messages_data, "messages", [])
    
    if messages:
        page_size = st.selectbox("Rows per page", [10, 25, 50], index=0, key="msg_page_size")
        total_items = len(messages)
        total_pages = max(1, (total_items + page_size - 1) // page_size)
        current_page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1, key="msg_page")
        page_items, start_i, end_i, total_i, total_pages = _paginate(messages, current_page, page_size)
        st.caption(f"Showing {start_i}-{end_i} of {total_i} messages (Page {current_page}/{total_pages})")
        
        display_data = []
        for msg in page_items:
            dt_str = "N/A"
            timestamp_str = msg.get('timestamp')
            if timestamp_str:
                dt = _parse_iso_datetime(timestamp_str)
                if dt:
                    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            display_data.append({
                "Time": dt_str,
                "From": msg.get('customer_name') or msg.get('customer_phone') or msg.get('user_phone', 'N/A'),
                "Intent": str(msg.get('intent', 'Unknown')).title(),
                "Incoming": msg.get('message_text') or msg.get('processed_text') or msg.get('incoming_text') or f"({msg.get('message_type', 'text')})",
                "Reply": msg.get('reply_text', 'N/A'),
                "Direction": str(msg.get('direction', 'inbound')).title()
            })
        
        df = pd.DataFrame(display_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No messages found matching the criteria.")

def products_page():
    """Render products management page"""
    st.markdown("# 📦 Products Catalog")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🗂️ Product List")
        if st.button("🔄 Refresh List", key="refresh_prod_list"):
            st.cache_data.clear()
            st.rerun()
        
        merchant_id = st.session_state.get("merchant_id", "demo")
        token = st.session_state.get("auth_token", "")
        
        try:
            # MERGE FIX: v5 endpoint is fine, but needs /api
            response = requests.get(
                f"{API_BASE_URL}/api/inventory/products",
                params={"merchant_id": merchant_id, "sort": "Name"},
                headers={"Authorization": f"Bearer {token}"} if token else {},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                products = data.get("products", [])
                # Map to products page format
                products = [
                    {
                        "_id": p.get("_id") or p.get("id"),
                        "name": p.get("product_name") or p.get("name", "Unnamed"),
                        "sku": p.get("sku", "-"),
                        "price": p.get("price", 0),
                        "category": p.get("category", "-"),
                        "brand": p.get("brand", "-"),
                        "tax": p.get("tax", 0)
                    }
                    for p in products
                ]
            else:
                st.error(f"Failed to load products: {response.status_code}")
                products = []
        except Exception as e:
            logger.error(f"Error fetching products: {e}", exc_info=True)
            st.error(f"Error loading products: {e}")
            products = []
        
        if products:
            page_size = st.selectbox("Rows/page", [10, 25, 50], index=0, key="prod_page_size")
            total_items = len(products)
            total_pages = max(1, (total_items + page_size - 1) // page_size)
            current_page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1, key="prod_page")
            page_items, start_i, end_i, _, _ = _paginate(products, current_page, page_size)
            st.caption(f"Showing {start_i}-{end_i} of {total_items} products (Page {current_page}/{total_pages})")
            
            for p in page_items:
                with st.container():
                    row = st.columns([4, 3, 2, 2, 2])
                    with row[0]:
                        st.write(f"**{p.get('name','Unnamed')}**")
                        st.caption(f"SKU: {p.get('sku','-')}")
                    with row[1]:
                        st.write(f"₹ {p.get('price','-')}")
                    with row[2]:
                        st.write(p.get('category','-'))
                    with row[3]:
                        st.write(p.get('brand','-'))
                    with row[4]:
                        if st.button("Manage", key=f"inv_{p.get('_id')}"):
                            st.session_state["prefill_product_id"] = p.get("_id")
                            st.session_state["nav_target"] = "📚 Inventory"
                            st.rerun()
                st.divider()
        else:
            st.info("No products found.")
    
    with col2:
        st.markdown("### ➕ Add / Update Product")
        st.caption("Provide SKU to update, otherwise a new product is created.")
        
        with st.form("add_product_form"):
            name = st.text_input("Product Name*", key="prod_name")
            sku = st.text_input("SKU (Unique ID)*", key="prod_sku")
            price = st.number_input("Price (Rs.)*", min_value=0.0, format="%.2f", step=0.50, key="prod_price")
            tax = st.number_input("Tax (%)", min_value=0.0, max_value=100.0, value=0.0, format="%.2f", key="prod_tax")
            category = st.text_input("Category", key="prod_category")
            brand = st.text_input("Brand", key="prod_brand")
            description = st.text_area("Description", height=80, key="prod_desc")
            image_url = st.text_input("Image URL", placeholder="https://...", key="prod_image")
            submitted = st.form_submit_button("💾 Save Product", use_container_width=True)
            
            if submitted:
                if not name or not sku:
                    set_error("Product Name and SKU are required.")
                else:
                    product_payload = {
                        "name": name, "sku": sku, "price": float(price), "tax": float(tax),
                        "category": category or None, "brand": brand or None,
                        "description": description or None, "image_url": image_url or None,
                        "merchant_id": st.session_state.merchant_id # Add merchant_id
                    }
                    with st.spinner("Saving product..."):
                        # MERGE FIX: Add /api prefix
                        response = make_api_request("/api/products", "POST", product_payload, st.session_state.auth_token)
                    if response and (response.get("product_sku") or response.get("status") == "success"):
                        st.success(f"✅ Product '{name}' ({sku}) saved successfully!")
                        st.cache_data.clear()
                        st.rerun()
    
    # ===== BULK UPLOAD (From v5) =====
    st.divider()
    st.markdown("### 📥 Bulk Upload Products (CSV)")
    with st.expander("Click to expand bulk upload"):
        st.info("Upload a CSV file with columns: name, sku, price, tax (optional), category (optional), brand (optional), description (optional), image_url (optional)")
        
        uploaded_file = st.file_uploader(
            "Choose CSV file", 
            type="csv", 
            key="product_csv_upload",
            help="Required columns: name, sku, price. Optional: tax, category, brand, description, image_url"
        )
        
        if uploaded_file:
            try:
                df = pd.read_csv(uploaded_file)
                
                st.write("**Preview (First 5 rows):**")
                st.dataframe(df.head(), use_container_width=True)
                
                required_cols = ["name", "sku", "price"]
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    st.error(f"❌ Missing required columns: {', '.join(missing_cols)}")
                    st.info("CSV must have at least: name, sku, price")
                else:
                    st.success(f"✅ Found {len(df)} products ready to upload")
                    
                    if st.button("📤 Upload All Products", use_container_width=True, type="primary"):
                        products_list = df.to_dict(orient='records')
                        
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        success_count = 0
                        failed_products = []
                        
                        with st.spinner(f"Uploading {len(products_list)} products..."):
                            for idx, prod in enumerate(products_list):
                                try:
                                    name = str(prod.get("name", "")).strip()
                                    sku = str(prod.get("sku", "")).strip()
                                    
                                    if not name or not sku:
                                        failed_products.append(f"Row {idx+1}: Empty name/SKU")
                                        continue
                                    
                                    try:
                                        price = float(prod.get("price", 0))
                                        if price < 0:
                                            failed_products.append(f"Row {idx+1}: Invalid price {price}")
                                            continue
                                    except (ValueError, TypeError):
                                        failed_products.append(f"Row {idx+1}: Price must be numeric")
                                        continue
                                    
                                    payload = {
                                        "name": name,
                                        "sku": sku,
                                        "price": price,
                                        "tax": float(prod.get("tax", 0)) if "tax" in prod else 0.0,
                                        "category": str(prod.get("category", "")) if "category" in prod else None,
                                        "brand": str(prod.get("brand", "")) if "brand" in prod else None,
                                        "description": str(prod.get("description", "")) if "description" in prod else None,
                                        "image_url": str(prod.get("image_url", "")) if "image_url" in prod else None,
                                        "merchant_id": st.session_state.merchant_id # Add merchant_id
                                    }
                                    
                                    # MERGE FIX: Add /api prefix
                                    response = make_api_request(
                                        "/api/products", 
                                        "POST", 
                                        payload, 
                                        st.session_state.auth_token
                                    )
                                    
                                    if response and response.get("status") == "success":
                                        success_count += 1
                                    else:
                                        failed_products.append(prod.get("name", f"Row {idx+1}"))
                                    
                                    progress = (idx + 1) / len(products_list)
                                    progress_bar.progress(progress)
                                    status_text.text(f"Uploading: {idx + 1}/{len(products_list)}")
                                
                                except Exception as e:
                                    logger.error(f"CSV row {idx}: {e}")
                                    try:
                                        prod_name = prod.get("name", f"Row {idx+1}")
                                    except:
                                        prod_name = f"Row {idx+1}"
                                    failed_products.append(f"{prod_name}: {str(e)[:50]}")
                        
                        progress_bar.empty()
                        status_text.empty()
                        
                        if success_count == len(products_list):
                            st.success(f"🎉 Successfully uploaded all {success_count} products!")
                        else:
                            st.warning(f"⚠️ Uploaded {success_count}/{len(products_list)} products")
                            if failed_products:
                                with st.expander("View failed uploads"):
                                    for failed in failed_products:
                                        st.write(f"❌ {failed}")
                        
                        st.cache_data.clear()
                        st.rerun()
            
            except Exception as e:
                st.error(f"❌ Error reading CSV: {str(e)}")
                st.info("Please check your CSV file format and try again.")
        
        else:
            st.markdown("**Sample CSV Format:**")
            sample_df = pd.DataFrame({
                "name": ["Milk", "Rice", "Bread"],
                "sku": ["SKU-MILK-001", "SKU-RICE-001", "SKU-BREAD-001"],
                "price": [60.00, 80.00, 40.00],
                "tax": [5, 5, 5],
                "category": ["Dairy", "Grains", "Bakery"],
                "brand": ["Farm Fresh", "Golden", "HomeMade"]
            })
            st.dataframe(sample_df, use_container_width=True)
            
            csv_buffer = sample_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Sample CSV",
                data=csv_buffer,
                file_name="sample_products.csv",
                mime="text/csv",
                use_container_width=True
            )

def inventory_page():
    """Render inventory management page"""
    st.header("📦 Inventory Management")
    st.write("Connect products and manage stock levels")

    inv_tab1, inv_tab2, inv_tab3, inv_tab4, inv_tab5 = st.tabs(
        ["➕ Add Product", "📋 View Products", "📊 Stock Levels", "✏️ Update Stock", "⚠️ Low Stock Alerts"]
    )

    # --- TAB 1: ADD NEW PRODUCT ---
    with inv_tab1:
        st.subheader("➕ Add New Product to Inventory")
        
        with st.form("add_product_form_inv", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                product_name = st.text_input("Product Name*", placeholder="e.g., Premium Basmati Rice")
                category = st.selectbox("Category*", ["Grains", "Vegetables", "Fruits", "Dairy", "Spices", "Beverages", "Other"])
                unit = st.selectbox("Unit*", ["kg", "L", "piece", "box", "pack", "dozen"])
            
            with col2:
                price = st.number_input("Price per Unit (₹)*", min_value=0.0, step=0.5, placeholder="e.g., 80.50")
                stock_qty = st.number_input("Initial Stock Quantity*", min_value=0.0, step=0.5, placeholder="e.g., 100")
                supplier = st.text_input("Supplier Name", placeholder="e.g., XYZ Suppliers")
            
            description = st.text_area("Description", placeholder="Product details, quality info, etc.", height=100)
            
            col1, col2 = st.columns(2)
            with col1:
                min_stock = st.number_input("Minimum Stock Alert Level*", min_value=0.0, step=0.5, value=10.0)
            with col2:
                reorder_qty = st.number_input("Reorder Quantity*", min_value=0.0, step=0.5, value=50.0)
            
            submitted = st.form_submit_button("➕ Add Product", use_container_width=True)
            
            if submitted:
                if not product_name or not category or not price:
                    st.error("❌ Please fill all required fields (*)")
                else:
                    with st.spinner("Adding product..."):
                        product_data = {
                            "product_name": product_name,
                            "category": category,
                            "unit": unit,
                            "price": price,
                            "stock_qty": stock_qty,
                            "supplier": supplier,
                            "description": description,
                            "min_stock": min_stock,
                            "reorder_qty": reorder_qty,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "merchant_id": st.session_state.get("merchant_id", "demo")
                        }
                        
                        try:
                            # MERGE FIX: v5 endpoint is already correct
                            response = requests.post(
                                f"{API_BASE_URL}/api/inventory/add-product",
                                json=product_data,
                                headers={"Authorization": f"Bearer {st.session_state['auth_token']}"}
                            )
                            
                            if response.status_code == 201:
                                st.success("✅ Product added successfully!")
                                st.balloons()
                            else:
                                st.error(f"❌ Error: {response.json().get('detail', 'Unknown error')}")
                        
                        except Exception as e:
                            st.error(f"❌ Connection error: {str(e)}")
    
    # --- TAB 2: VIEW ALL PRODUCTS ---
    with inv_tab2:
        st.subheader("📋 View All Products in Inventory")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            search_term = st.text_input("🔍 Search products:", placeholder="Leave empty to see all", value="")
        with col2:
            filter_category = st.selectbox("Filter by category:", ["All", "Grains", "Vegetables", "Fruits", "Dairy", "Spices", "Beverages", "Bakery", "Cooking", "Other"])
        with col3:
            sort_by = st.selectbox("Sort by:", ["Name", "Price (Low-High)", "Price (High-Low)", "Stock", "Recently Added"])
        
        if st.button("🔄 Refresh Products", use_container_width=True, key="refresh_inv_products"):
            st.cache_data.clear()
            st.rerun()
        
        with st.spinner("Loading products..."):
            try:
                merchant_id = st.session_state.get("merchant_id", "demo")
                token = st.session_state.get("auth_token", "")
                
                params = {"merchant_id": merchant_id, "sort": sort_by}
                if search_term.strip():
                    params["search"] = search_term
                if filter_category != "All":
                    params["category"] = filter_category
                
                logger.info(f"Fetching products with params: {params}")
                
                # MERGE FIX: v5 endpoint is already correct
                response = requests.get(
                    f"{API_BASE_URL}/api/inventory/products",
                    params=params,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    products = data.get("products", [])
                    
                    if products:
                        st.success(f"✅ Found {len(products)} product(s)")
                        df_products = []
                        for p in products:
                            df_products.append({
                                "Product": p.get("product_name"),
                                "Category": p.get("category"),
                                "Price (₹)": f"{p.get('price', 0)}",
                                "Unit": p.get("unit"),
                                "Stock": f"{p.get('stock_qty', 0)} {p.get('unit')}",
                                "Supplier": p.get("supplier", "N/A"),
                                "Min": p.get("min_stock", 0),
                                "Status": "✅ OK" if p.get('stock_qty', 0) > p.get('min_stock', 0) else "⚠️ LOW"
                            })
                        st.dataframe(pd.DataFrame(df_products), use_container_width=True, hide_index=True)
                    else:
                        st.warning("❌ No products found")
                else:
                    st.error(f"❌ Error: {response.json().get('detail', 'Unknown error')}")
            
            except requests.exceptions.ConnectionError:
                st.error("❌ Cannot connect to API")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # --- TAB 3: STOCK LEVELS DASHBOARD ---
    with inv_tab3:
        st.subheader("📊 Stock Levels & Analytics")
        
        if st.button("🔄 Refresh Stock Report", use_container_width=True, key="refresh_stock_report"):
            st.cache_data.clear()
            st.rerun()
        
        with st.spinner("Loading stock report..."):
            try:
                merchant_id = st.session_state.get("merchant_id", "demo")
                token = st.session_state.get("auth_token", "")
                
                # MERGE FIX: v5 endpoint is already correct
                response = requests.get(
                    f"{API_BASE_URL}/api/inventory/stock-report",
                    params={"merchant_id": merchant_id},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10
                )
                
                if response.status_code == 200:
                    report = response.json()
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("📦 Total Products", report.get("total_products", 0), delta=f"₹{report.get('total_value', 0):,.0f}")
                    with col2:
                        st.metric("✅ In Stock", report.get("in_stock", 0), delta_color="off")
                    with col3:
                        st.metric("⚠️ Low Stock", report.get("low_stock", 0), delta_color="inverse")
                    with col4:
                        st.metric("🚫 Out of Stock", report.get("out_of_stock", 0), delta_color="inverse")
                    
                    st.divider()
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**📊 Stock by Category**")
                        by_cat = report.get("by_category", {})
                        if by_cat:
                            chart_df = pd.DataFrame({"Category": list(by_cat.keys()), "Quantity": list(by_cat.values())})
                            st.bar_chart(chart_df.set_index("Category"), use_container_width=True)
                        else:
                            st.info("No category data")
                    with col2:
                        st.write("**💰 Value by Category**")
                        val_by_cat = report.get("value_by_category", {})
                        if val_by_cat:
                            chart_df = pd.DataFrame({"Category": list(val_by_cat.keys()), "Value": list(val_by_cat.values())})
                            st.bar_chart(chart_df.set_index("Category"), use_container_width=True, color="#FF6B6B")
                        else:
                            st.info("No value data")
                    
                    st.divider()
                    
                    low_stock = report.get("low_stock_products", [])
                    if low_stock:
                        st.warning(f"⚠️ **{len(low_stock)} Product(s) - Low Stock!**")
                        low_stock_df = []
                        for p in low_stock:
                            low_stock_df.append({
                                "Product": p.get("product_name"),
                                "Current": f"{p.get('stock_qty')} {p.get('unit')}",
                                "Min Level": p.get("min_stock"),
                                "Reorder": f"{p.get('reorder_qty')} {p.get('unit')}",
                                "Category": p.get("category")
                            })
                        st.dataframe(pd.DataFrame(low_stock_df), use_container_width=True, hide_index=True)
                    else:
                        st.success("✅ All products above minimum level!")
                else:
                    st.error(f"❌ Error: {response.json().get('detail', 'Unknown error')}")
            
            except requests.exceptions.ConnectionError:
                st.error("❌ Cannot connect to API")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # --- TAB 4: UPDATE STOCK ---
    with inv_tab4:
        st.subheader("✏️ Update Stock Levels")
        
        try:
            # MERGE FIX: v5 endpoint is already correct
            response = requests.get(
                f"{API_BASE_URL}/api/inventory/products",
                params={"merchant_id": st.session_state.get("merchant_id", "demo")},
                headers={"Authorization": f"Bearer {st.session_state['auth_token']}"}
            )
            
            if response.status_code == 200:
                products = response.json().get("products", [])
                product_names = [p["product_name"] for p in products]
                
                if product_names:
                    selected_product = st.selectbox("Select Product*", product_names, help="Choose which product to update")
                    current_product = next((p for p in products if p["product_name"] == selected_product), None)
                    
                    if current_product:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.info(f"**Current Stock:** {current_product['stock_qty']} {current_product['unit']}")
                            st.write(f"**Price:** ₹{current_product['price']}")
                        with col2:
                            st.info(f"**Min Level:** {current_product['min_stock']}")
                            st.write(f"**Category:** {current_product['category']}")
                        
                        st.divider()
                        
                        with st.form("update_stock_form"):
                            operation = st.radio("Operation*", ["Set New Stock", "Add to Stock", "Deduct from Stock"], help="Choose how to update")
                            quantity = st.number_input("Quantity*", min_value=0.0, step=0.5, placeholder="Enter quantity")
                            reason = st.text_input("Reason for Update", placeholder="e.g., New purchase, Sale, Damage, Inventory count")
                            notes = st.text_area("Additional Notes", placeholder="Any additional information", height=80)
                            
                            submitted = st.form_submit_button("📤 Update Stock", use_container_width=True)
                            
                            if submitted:
                                if quantity == 0:
                                    st.error("❌ Quantity cannot be 0")
                                else:
                                    with st.spinner("Updating..."):
                                        update_data = {
                                            "product_id": current_product["_id"],
                                            "operation": operation,
                                            "quantity": quantity,
                                            "reason": reason,
                                            "notes": notes,
                                            "timestamp": datetime.now(timezone.utc).isoformat(),
                                            "merchant_id": st.session_state.get("merchant_id", "demo")
                                        }
                                        
                                        try:
                                            # MERGE FIX: v5 endpoint is already correct
                                            response = requests.post(
                                                f"{API_BASE_URL}/api/inventory/update-stock",
                                                json=update_data,
                                                headers={"Authorization": f"Bearer {st.session_state['auth_token']}"}
                                            )
                                            if response.status_code == 200:
                                                st.success("✅ Stock updated successfully!")
                                                st.balloons()
                                            else:
                                                st.error(f"❌ Error: {response.json().get('detail')}")
                                        except Exception as e:
                                            st.error(f"❌ Error: {str(e)}")
                else:
                    st.warning("⚠️ No products found. Add products first!")
        
        except Exception as e:
            st.error(f"❌ Error loading products: {str(e)}")
    
    # --- TAB 5: LOW STOCK ALERTS ---
    with inv_tab5:
        st.subheader("⚠️ Low Stock Alerts & Notifications")
        
        try:
            # MERGE FIX: v5 endpoint is already correct
            response = requests.get(
                f"{API_BASE_URL}/api/inventory/alerts",
                params={"merchant_id": st.session_state.get("merchant_id", "demo")},
                headers={"Authorization": f"Bearer {st.session_state['auth_token']}"}
            )
            
            if response.status_code == 200:
                alerts = response.json().get("alerts", [])
                
                if alerts:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("🔴 Critical", len([a for a in alerts if a.get("severity") == "critical"]))
                    with col2:
                        st.metric("🟡 Warning", len([a for a in alerts if a.get("severity") == "warning"]))
                    with col3:
                        st.metric("🟢 Info", len([a for a in alerts if a.get("severity") == "info"]))
                    
                    st.divider()
                    
                    for alert in alerts:
                        severity = alert.get("severity", "info")
                        if severity == "critical":
                            st.error(f"🔴 **CRITICAL:** {alert.get('message')}")
                        elif severity == "warning":
                            st.warning(f"🟡 **WARNING:** {alert.get('message')}")
                        else:
                            st.info(f"🟢 **INFO:** {alert.get('message')}")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.caption(f"Product: {alert.get('product_name')}")
                        with col2:
                            st.caption(f"Time: {alert.get('timestamp', 'N/A')[:10]}")
                else:
                    st.success("✅ No alerts! All stock levels are healthy.")
        
        except Exception as e:
            st.error(f"❌ Error loading alerts: {str(e)}")

def business_rules_page():
    """Render business rules configuration page"""
    st.markdown("# ⚙️ Business Rules")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📋 Configured Rules")
        if st.button("🔄 Refresh Rules", key="refresh_rules_list"):
            st.cache_data.clear()
            st.rerun()
        
        # MERGE FIX: Add /api prefix and merchant_id
        rules_response = cached_get(f"/api/rules?merchant_id={st.session_state.merchant_id}", st.session_state.auth_token)
        rules = extract_response_data(rules_response, "rules", [])
        
        if rules:
            for rule in rules:
                with st.container():
                    col_a, col_b, col_c = st.columns([3, 2, 1])
                    with col_a:
                        rule_type = rule.get("rule_type", "unknown").replace("_", " ").title()
                        st.markdown(f"**{rule_type}**")
                    with col_b:
                        enabled = rule.get("enabled", False)
                        st.markdown(f"{'✅ Active' if enabled else '❌ Disabled'}")
                    with col_c:
                        if st.button("Delete", key=f"del_{rule.get('_id')}"):
                            # MERGE FIX: Add /api prefix and merchant_id
                            response = make_api_request(f"/api/rules/{rule.get('_id')}?merchant_id={st.session_state.merchant_id}", "DELETE", token=st.session_state.auth_token)
                            if response and response.get("status") == "success":
                                st.success("Rule deleted!")
                                st.cache_data.clear()
                                st.rerun()
                    st.divider()
        else:
            st.info("No rules configured yet.")
    
    with col2:
        st.markdown("### ➕ Create New Rule")
        with st.form("create_rule_form"):
            rule_type = st.selectbox(
                "Rule Type",
                ["low_stock_alert", "restock_reminder", "sales_target", "monthly_report"],
                format_func=lambda x: x.replace("_", " ").title()
            )
            
            rule_config = {}
            if rule_type == "low_stock_alert":
                threshold = st.number_input("Alert Threshold (%)", min_value=1, max_value=99, value=20, step=5)
                rule_config = get_low_stock_alert_template(threshold)['rule_config']
            elif rule_type == "restock_reminder":
                freq = st.selectbox("Reminder Frequency", ["daily", "weekly", "once"], index=0)
                rule_config = get_restock_reminder_template(freq)['rule_config']
            elif rule_type == "sales_target":
                amount = st.number_input("Target Amount (Rs.)", min_value=1.0, value=50000.0, step=1000.0)
                freq = st.selectbox("Target Frequency", ["monthly", "weekly", "daily"], index=0)
                rule_config = get_sales_target_template(amount, freq)['rule_config']
            elif rule_type == "monthly_report":
                day = st.number_input("Send Report on Day", min_value=1, max_value=28, value=1, step=1)
                time_input = st.time_input("Send Time", value=datetime.strptime("09:00", "%H:%M").time())
                rule_config = get_monthly_report_template(day, time_input.strftime("%H:%M"))['rule_config']
            
            submit_rule = st.form_submit_button("💾 Create Rule", use_container_width=True)
            
            if submit_rule:
                payload = {
                    "rule_type": rule_type, 
                    "rule_config": rule_config, 
                    "enabled": True,
                    "merchant_id": st.session_state.merchant_id # Add merchant_id
                }
                with st.spinner("Creating rule..."):
                    # MERGE FIX: Add /api prefix
                    response = make_api_request("/api/rules/create", "POST", payload, st.session_state.auth_token)
                if response and response.get("status") == "success":
                    st.success(f"✅ Rule '{rule_type.replace('_',' ').title()}' created!")
                    st.cache_data.clear()
                    st.rerun()

def alerts_page():
    """Render alerts management page"""
    st.header("⚠️ Alerts & Notifications")
    st.write("Monitor and manage all business alerts")
    
    alert_tab1, alert_tab2, alert_tab3, alert_tab4 = st.tabs(
        ["🔴 Critical", "🟡 Warnings", "🟢 Info", "📊 Statistics"]
    )
    
    # --- TAB 1: CRITICAL ALERTS ---
    with alert_tab1:
        st.subheader("🔴 CRITICAL ALERTS - URGENT ACTION NEEDED")
        if st.button("🔄 Refresh Critical Alerts", use_container_width=True):
            st.rerun()
        
        with st.spinner("Loading critical alerts..."):
            try:
                # MERGE FIX: v5 endpoint is already correct
                response = requests.get(
                    f"{API_BASE_URL}/api/alerts/critical",
                    params={"merchant_id": st.session_state.get("merchant_id", "demo")},
                    headers={"Authorization": f"Bearer {st.session_state.get('auth_token', '')}"},
                    timeout=10
                )
                if response.status_code == 200:
                    alerts = response.json().get("alerts", [])
                    st.metric("🔴 Critical Count", len(alerts))
                    if alerts:
                        for alert in alerts:
                            with st.container(border=True):
                                col1, col2 = st.columns([3, 1])
                                with col1:
                                    st.error(f"🔴 {alert.get('title', 'Alert')}")
                                    st.write(f"**Product:** {alert.get('product_name', 'N/A')}")
                                    st.write(f"**Message:** {alert.get('message', 'N/A')}")
                                    if alert.get('action_items'):
                                        st.write("**Actions:**")
                                        for action in alert['action_items']:
                                            st.write(f"• {action}")
                                    st.caption(f"Created: {alert.get('created_at', 'N/A')[:10]}")
                                with col2:
                                    col_a, col_b = st.columns(2)
                                    with col_a:
                                        if st.button("✅", key=f"ack_{alert.get('id')}", help="Acknowledge"):
                                            requests.post(
                                                f"{API_BASE_URL}/api/alerts/acknowledge",
                                                json={"alert_id": alert.get("id")},
                                                headers={"Authorization": f"Bearer {st.session_state.get('auth_token', '')}"},
                                                timeout=10
                                            )
                                            st.success("Acknowledged!")
                                            st.rerun()
                                    with col_b:
                                        if st.button("✔️", key=f"res_{alert.get('id')}", help="Resolve"):
                                            requests.post(
                                                f"{API_BASE_URL}/api/alerts/resolve",
                                                json={"alert_id": alert.get("id")},
                                                headers={"Authorization": f"Bearer {st.session_state.get('auth_token', '')}"},
                                                timeout=10
                                            )
                                            st.success("Resolved!")
                                            st.rerun()
                else:
                    st.success("✅ No critical alerts!")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # --- TAB 2: WARNINGS ---
    with alert_tab2:
        st.subheader("🟡 WARNINGS - Important")
        if st.button("🔄 Refresh Warnings", use_container_width=True):
            st.rerun()
        with st.spinner("Loading warnings..."):
            try:
                # MERGE FIX: v5 endpoint is already correct
                response = requests.get(
                    f"{API_BASE_URL}/api/alerts/warnings",
                    params={"merchant_id": st.session_state.get("merchant_id", "demo")},
                    headers={"Authorization": f"Bearer {st.session_state.get('auth_token', '')}"},
                    timeout=10
                )
                if response.status_code == 200:
                    alerts = response.json().get("alerts", [])
                    st.metric("🟡 Warning Count", len(alerts))
                    if alerts:
                        for alert in alerts:
                            with st.container(border=True):
                                col1, col2 = st.columns([3, 1])
                                with col1:
                                    st.warning(f"🟡 {alert.get('title', 'Alert')}")
                                    st.write(f"**Message:** {alert.get('message', 'N/A')}")
                                    st.caption(f"Created: {alert.get('created_at', 'N/A')[:10]}")
                                with col2:
                                    if st.button("✔️ Done", key=f"warn_{alert.get('id')}"):
                                        requests.post(
                                            f"{API_BASE_URL}/api/alerts/resolve",
                                            json={"alert_id": alert.get("id")},
                                            headers={"Authorization": f"Bearer {st.session_state.get('auth_token', '')}"},
                                            timeout=10
                                        )
                                        st.rerun()
                else:
                    st.info("ℹ️ No warnings")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # --- TAB 3: INFO ---
    with alert_tab3:
        st.subheader("🟢 INFO - Just FYI")
        with st.spinner("Loading info..."):
            try:
                # MERGE FIX: v5 endpoint is already correct
                response = requests.get(
                    f"{API_BASE_URL}/api/alerts/info",
                    params={"merchant_id": st.session_state.get("merchant_id", "demo")},
                    headers={"Authorization": f"Bearer {st.session_state.get('auth_token', '')}"},
                    timeout=10
                )
                if response.status_code == 200:
                    alerts = response.json().get("alerts", [])
                    if alerts:
                        for alert in alerts:
                            st.info(f"ℹ️ **{alert.get('title', 'Alert')}**\n\n{alert.get('message', 'N/A')}")
                    else:
                        st.success("✅ No info alerts")
                else:
                    st.error(f"❌ Error: {response.status_code}")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # --- TAB 4: STATISTICS ---
    with alert_tab4:
        st.subheader("📊 Alert Statistics")
        with st.spinner("Loading stats..."):
            try:
                # MERGE FIX: v5 endpoint is already correct
                response = requests.get(
                    f"{API_BASE_URL}/api/alerts/stats",
                    params={"merchant_id": st.session_state.get("merchant_id", "demo")},
                    headers={"Authorization": f"Bearer {st.session_state.get('auth_token', '')}"},
                    timeout=10
                )
                if response.status_code == 200:
                    stats = response.json()
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("🔴 Critical", stats.get("critical", 0))
                    with col2:
                        st.metric("🟡 Warnings", stats.get("warnings", 0))
                    with col3:
                        st.metric("🟢 Info", stats.get("info", 0))
                    with col4:
                        st.metric("📋 Total Pending", stats.get("total_pending", 0))
                    st.divider()
                    st.write("**Alert Breakdown:**")
                    st.json(stats)
                else:
                    st.error(f"❌ Error: {response.status_code}")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

def ai_knowledge_page():
    """Render AI knowledge page"""
    st.header("🧠 AI Knowledge Base")
    st.write("Upload documents for AI to learn from customer interactions")

    merchant_id = st.session_state.get("merchant_id", "demo")
    token = st.session_state.get("auth_token") or st.session_state.get("token", "")

    with st.expander("📤 Upload New Document", expanded=True):
        col1, col2 = st.columns([3, 1])
        with col1:
            uploaded_file = st.file_uploader("Choose file (PDF, DOCX, or TXT)", type=["pdf", "docx", "doc", "txt"], help="Max 25MB. Documents will be split into chunks and indexed.")
        with col2:
            st.write("")
            st.write("")
            upload_btn = st.button("📤 Upload", use_container_width=True)

        if upload_btn and uploaded_file:
            with st.spinner(f"Processing {uploaded_file.name}..."):
                try:
                    files = {"file": (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type or "application/octet-stream")}
                    data = {"merchant_id": merchant_id}
                    # MERGE FIX: v5 endpoint is already correct
                    response = requests.post(
                        f"{API_BASE_URL}/api/knowledge/upload",
                        files=files,
                        data=data,
                        headers={"Authorization": f"Bearer {token}"} if token else {},
                        timeout=30
                    )
                    if response.status_code == 200:
                        st.success(f"✅ Document uploaded!")
                        st.rerun()
                    else:
                        st.error(f"Upload failed: {response.json().get('detail', 'Unknown error')}")
                except Exception as e:
                    st.error(f"Error uploading: {e}")

    st.subheader("📚 Your Knowledge Documents")
    try:
        # MERGE FIX: v5 endpoint is already correct
        response = requests.get(
            f"{API_BASE_URL}/api/knowledge/documents",
            params={"merchant_id": merchant_id},
            headers={"Authorization": f"Bearer {token}"} if token else {},
            timeout=10
        )
        if response.status_code == 200:
            docs = response.json().get("documents", [])
            if docs:
                st.success(f"📊 Total documents: {len(docs)}")
                for doc in docs:
                    with st.container(border=True):
                        col1, col2 = st.columns([4, 1])
                        with col1:
                            st.write(f"**📄 {doc.get('filename')}**")
                            col_a, col_b, col_c = st.columns(3)
                            with col_a:
                                st.caption(f"📦 {doc.get('chunks_count', 0)} chunks")
                            with col_b:
                                file_type = str(doc.get('file_type', 'unknown')).split('/')[-1].upper()
                                st.caption(f"📋 {file_type}")
                            with col_c:
                                uploaded_date = str(doc.get('uploaded_at', 'N/A'))[:10]
                                st.caption(f"📅 {uploaded_date}")
                        with col2:
                            if st.button("🗑️ Delete", key=f"del_{doc.get('_id')}", use_container_width=True):
                                with st.spinner("Deleting..."):
                                    # MERGE FIX: v5 endpoint is already correct
                                    del_response = requests.delete(
                                        f"{API_BASE_URL}/api/knowledge/document/{doc.get('doc_id')}",
                                        params={"merchant_id": merchant_id},
                                        headers={"Authorization": f"Bearer {token}"} if token else {},
                                        timeout=10
                                    )
                                    if del_response.status_code == 200:
                                        st.success("✅ Deleted!")
                                        st.rerun()
                                    else:
                                        st.error("❌ Delete failed")
            else:
                st.info("📭 No documents uploaded yet.")
        else:
            st.error(f"Error loading documents: {response.json().get('detail')}")
    except Exception as e:
        st.error(f"Error: {e}")

    st.subheader("🔍 Test Knowledge Search")
    query = st.text_input("❓ Ask a question:", placeholder="e.g., What's our return policy?")
    if query and st.button("🔍 Search Knowledge"):
        with st.spinner("Searching..."):
            try:
                # MERGE FIX: v5 endpoint is already correct
                response = requests.post(
                    f"{API_BASE_URL}/api/knowledge/search",
                    json={"merchant_id": merchant_id, "query": query, "top_k": 3},
                    headers={"Authorization": f"Bearer {token}"} if token else {},
                    timeout=10
                )
                if response.status_code == 200:
                    result = response.json()
                    if result.get("results"):
                        st.success("✅ Found relevant knowledge!")
                        for idx, context in enumerate(str(result["results"]).split("\n"), 1):
                            if context.strip():
                                st.write(f"**{idx}.** {context}")
                    else:
                        st.warning("⚠️ No matching knowledge found.")
                else:
                    st.error(f"Search failed: {response.json().get('detail')}")
            except Exception as e:
                st.error(f"Error: {e}")

def chat_page():
    """ (From v5) General-purpose AI chatbot with merchant context"""
    st.header("🤖 AI Assistant")
    st.markdown("Ask me anything about your business, products, policies, or general questions.")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    st.markdown("---")
    chat_container = st.container(height=400) # Give container a fixed height
    with chat_container:
        for message in st.session_state.chat_history:
            if message.get("role") == "user":
                with st.chat_message("user", avatar="👤"):
                    st.markdown(message.get("content", ""))
            else:
                with st.chat_message("assistant", avatar="🤖"):
                    st.markdown(message.get("content", ""))

    user_input = st.chat_input("Type your question...", key="chat_input_box")

    if user_input:
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        with st.spinner("🤔 Thinking..."):
            try:
                # MERGE FIX: v5 endpoint is already correct
                response = requests.post(
                    f"{API_BASE_URL}/api/chat",
                    params={
                        "merchant_id": st.session_state.merchant_id,
                        "user_message": user_input,
                    },
                    json=(st.session_state.chat_history[:-1] or []),
                    headers={"Authorization": f"Bearer {st.session_state.auth_token}"},
                    timeout=30,
                )
                if response.status_code == 200:
                    result = response.json()
                    assistant_response = result.get("response", "No response received")
                    st.session_state.chat_history.append({"role": "assistant", "content": assistant_response})
                else:
                    st.error(f"API Error: {response.status_code}")
                    assistant_response = "Sorry, I ran into an error."
                    st.session_state.chat_history.append({"role": "assistant", "content": assistant_response})
            except Exception as e:
                st.error(f"❌ Error: {e}")
                assistant_response = "Sorry, I couldn't connect to the chat service."
                st.session_state.chat_history.append({"role": "assistant", "content": assistant_response})
        st.rerun()

def settings_page():
    """Render settings page"""
    st.markdown("# ⚙️ Settings")
    
    with st.expander("API Configuration", expanded=True):
        st.write(f"**Role:** `{st.session_state.get('role', 'N/A')}`")
        st.write(f"**Username:** `{st.session_state.get('username', 'N/A')}`")
        st.write(f"**Merchant ID:** `{st.session_state.get('merchant_id', 'N/A')}`")
        st.write(f"**API Base URL:** `{API_BASE_URL}`")
        st.write(f"**Auth Token:** {'✅ Present' if st.session_state.get('auth_token') else '❌ Missing'}")
    
    st.divider()
    st.markdown("### 🔍 Health Check")
    if st.button("Run Health Check", use_container_width=True):
        # MERGE FIX: Add /api prefix
        resp = make_api_request("/api/health", token=st.session_state.get("auth_token"))
        if resp:
            st.success("✅ Health check passed")
            st.json(resp)
        else:
            st.error("❌ Health check failed")

# ========================================================
# 👑 ADMIN PAGES (From v4_admin)
# ========================================================

def admin_dashboard_page():
    """Render Admin-only page for system-wide stats"""
    st.markdown("# 👑 Admin Dashboard")
    
    with st.spinner("Loading system-wide stats..."):
        # Admin routes are at the root
        stats_data = make_api_request("/admin/stats", token=st.session_state.auth_token)
    
    if not stats_data:
        st.error("Failed to load admin stats.")
        return
        
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Merchants", stats_data.get("total_merchants", 0))
    col2.metric("Total Messages (All Time)", stats_data.get("total_messages", 0))
    col3.metric("Messages Today (System-wide)", stats_data.get("today_messages", 0))
    
    st.divider()
    
    st.markdown("### 📥 All Recent Messages")
    with st.spinner("Loading recent messages..."):
        messages_data = make_api_request("/admin/messages?limit=20", token=st.session_state.auth_token)
    
    if messages_data and "messages" in messages_data:
        messages = messages_data["messages"]
        if messages:
            display_data = []
            for msg in messages:
                timestamp = msg.get('timestamp', '')
                dt_str = "N/A"
                if timestamp:
                    dt = _parse_iso_datetime(timestamp)
                    dt_str = format_dt(dt) if dt else timestamp
                
                display_data.append({
                    "Timestamp": dt_str,
                    "Merchant ID": msg.get('merchant_id', 'N/A'),
                    "Customer": msg.get('user_phone', 'N/A'),
                    "Intent": msg.get('intent', 'N/A'),
                    "Incoming": msg.get('processed_text') or msg.get('incoming_text') or "(Media)",
                })
            st.dataframe(pd.DataFrame(display_data), use_container_width=True)
        else:
            st.info("No messages found in the system yet.")
    else:
        st.error("Failed to load messages.")


def admin_merchant_management_page():
    """Render Admin-only page for managing merchants"""
    st.markdown("# 🧑‍💼 Merchant Management")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📋 All Merchants")
        with st.spinner("Loading merchants..."):
            merchants_data = make_api_request("/admin/merchants", token=st.session_state.auth_token)
        
        if merchants_data and "merchants" in merchants_data:
            merchants = merchants_data["merchants"]
            if merchants:
                display_data = []
                for m in merchants:
                    display_data.append({
                        "Username": m.get("username"),
                        "Full Name": m.get("full_name"),
                        "Phone": m.get("phone"),
                        "Active": "✅" if m.get("is_active") else "❌",
                        "WA Phone ID": m.get("details", {}).get("whatsapp_phone_id", "N/A"),
                        "ID": m.get("_id")
                    })
                
                df = pd.DataFrame(display_data)
                st.dataframe(df, use_container_width=True)
                
                st.markdown("---")
                st.markdown("### ❌ Danger Zone: Delete Merchant")
                delete_id = st.text_input("Enter Merchant ID to delete (from table above)")
                if st.button(f"DELETE Merchant {delete_id}", type="primary"):
                    if delete_id:
                        st.warning(f"**This is permanent.** All data (messages, products, inventory) for merchant `{delete_id}` will be deleted.")
                        if st.button("Confirm Deletion", type="primary"):
                            with st.spinner(f"Deleting merchant {delete_id}..."):
                                response = make_api_request(f"/admin/merchant/{delete_id}", "DELETE", token=st.session_state.auth_token)
                            if response and response.get("status") == "accepted":
                                st.success("✅ Merchant deletion initiated in background.")
                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error("❌ Failed to initiate deletion.")
                    else:
                        st.error("Please enter a Merchant ID.")

            else:
                st.info("No merchants found. Create one!")
        else:
            st.error("Failed to load merchants.")

    with col2:
        st.markdown("### ➕ Create New Merchant")
        with st.form("create_merchant_form"):
            full_name = st.text_input("Full Name / Business Name*")
            username = st.text_input("Login Username*")
            password = st.text_input("Password*", type="password")
            phone = st.text_input("Contact Phone*")
            whatsapp_phone_id = st.text_input("WhatsApp Business Phone ID*")
            
            submit = st.form_submit_button("Create Merchant Account", use_container_width=True)
            
            if submit:
                if not all([full_name, username, password, phone, whatsapp_phone_id]):
                    st.error("Please fill in all required fields.")
                else:
                    payload = {
                        "full_name": full_name,
                        "username": username,
                        "password": password,
                        "phone": phone,
                        "whatsapp_phone_id": whatsapp_phone_id,
                        "details": {"created_by": st.session_state.username}
                    }
                    with st.spinner("Creating merchant..."):
                        response = make_api_request("/admin/merchant", "POST", payload, st.session_state.auth_token)
                    
                    if response and response.get("status") == "success":
                        st.success(f"✅ Merchant '{username}' created successfully!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"❌ Failed to create merchant. Check logs. {response.get('detail', '')}")

# ========================================================
# 💳 PORTAL ROUTERS (The "main" functions)
# ========================================================

def merchant_portal():
    """
    (From v5) Main dashboard application router FOR MERCHANTS
    """
    # Check for new orders to update badge
    try:
        check_new_orders()
    except Exception as e:
        logger.warning(f"Failed to check new orders: {e}")

    # --- Sidebar ---
    with st.sidebar:
        st.markdown(f"### Welcome, {st.session_state.username}! 👋")

        # MERGE FIX: Add /api prefix and merchant_id
        merchant_id = st.session_state.get("merchant_id", DEFAULT_MERCHANT_ID)
        alerts_data = make_api_request(f"/api/alerts?unread_only=true&merchant_id={merchant_id}", token=st.session_state.auth_token)
        unread_count = len(extract_response_data(alerts_data, "alerts", []))
        st.session_state.unread_alerts = unread_count

        pending = st.session_state.get("pending_orders_count", 0)

        nav_options = {
            "📊 Overview": overview_page,
            "📋 Orders": orders_page,
            "💬 Messages": messages_page,
            "📦 Products": products_page,
            "📚 Inventory": inventory_page,
            "⚙️ Business Rules": business_rules_page,
            "🔔 Alerts": alerts_page,
            "🧠 AI Knowledge": ai_knowledge_page,
            "🤖 AI Chatbot": chat_page,
            "⚙️ Settings": settings_page,
        }

        # Proper badge handling
        nav_display_options = []
        for key in nav_options.keys():
            if key == "🔔 Alerts":
                nav_display_options.append(f"🔔 Alerts ({unread_count})" if unread_count > 0 else key)
            elif key == "📋 Orders":
                nav_display_options.append(f"📋 Orders ({pending})" if pending > 0 else key)
            else:
                nav_display_options.append(key)

        page_selection = st.radio("Navigation", nav_display_options, index=0, label_visibility="collapsed")

        selected_key = page_selection
        if " (" in page_selection:
            selected_key = page_selection[:page_selection.rfind(" (")]

        if selected_key not in nav_options:
            selected_key = "📊 Overview"

        st.markdown("---")
        st.markdown("**Quick Stats**")
        # MERGE FIX: Add /api prefix
        overview_data = cached_get(f"/api/overview?merchant_id={st.session_state.merchant_id}", st.session_state.auth_token)
        if overview_data:
            st.sidebar.metric("Messages Today", overview_data.get("today_messages", 0))
            st.sidebar.metric("Total Customers", overview_data.get("unique_customers", 0))
        else:
            st.caption("Overview data unavailable.")

        st.markdown("---")
        if st.button("🚪 Logout", use_container_width=True):
            logger.info(f"User {st.session_state.username} logged out.")
            # Auth route is at root
            make_api_request("/auth/logout", "POST", {"token": st.session_state.auth_token}, token=st.session_state.auth_token)
            keys_to_del = [k for k in st.session_state.keys()]
            for key in keys_to_del:
                del st.session_state[key]
            st.session_state.logged_in = False
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.caption("VyaapaarAI v6.0")

    # Show global pending order alert bar at the top on ALL pages
    try:
        render_pending_order_alert()
    except Exception as e:
        logger.error(f"Failed to render pending alert: {e}")

    # Keep popup on Overview (optional)
    if selected_key == "📊 Overview":
        try:
            show_order_notification_popup()
        except Exception as e:
            logger.error(f"Failed to render popup: {e}")

    # --- Render selected page ---
    page_func = nav_options.get(selected_key)
    if page_func:
        try:
            page_func()
        except Exception as page_err:
            logger.error(f"Error rendering page '{selected_key}': {page_err}", exc_info=True)
            st.error(f"An error occurred while loading the {selected_key} page. Please check logs or try refreshing.")
    else:
        st.error(f"Page '{selected_key}' not found!")

def admin_portal():
    """
    (From v4) Main entry point for the Admin view
    """
    with st.sidebar:
        st.markdown(f"### 👑 Admin Portal")
        st.markdown(f"Logged in as **{st.session_state.username}**")
        
        page = st.radio(
            "Admin Navigation",
            [
                "🚀 System Dashboard",
                "🧑‍💼 Merchant Management",
                "⚙️ Settings" # Use the shared settings page
            ],
            index=0,
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        if st.button("🚪 Logout", use_container_width=True):
            # Auth route is at root
            make_api_request("/auth/logout", "POST", {"token": st.session_state.auth_token})
            for key in SESSION_STATE_KEYS:
                st.session_state[key] = None if key != "logged_in" else False
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.markdown("**VyaapaarAI v6.0 (Admin)**")

    # Render selected admin page
    if page == "🚀 System Dashboard":
        admin_dashboard_page()
    elif page == "🧑‍💼 Merchant Management":
        admin_merchant_management_page()
    elif page == "⚙️ Settings":
        settings_page() # Use the shared settings page

# ========================================================
# 🚀 APP ENTRYPOINT
# ========================================================

def app_router():
    """
    Main app router. Shows login page or routes to the
    correct dashboard based on the user's role.
    """
    if not st.session_state.get("logged_in"):
        login_page()
        return

    role = st.session_state.get("role")
    
    if role == "admin":
        admin_portal()
    elif role == "merchant":
        merchant_portal() # This is the renamed `main()` from v5
    else:
        # Logged in but no role?
        st.error(f"Error: Logged in as '{st.session_state.get('username', 'UNKNOWN')}' but no valid role ('admin' or 'merchant') was found.")
        st.json(st.session_state.to_dict()) # Show session state for debugging
        if st.button("Logout"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

if __name__ == "__main__":
    app_router() # Call the main router
