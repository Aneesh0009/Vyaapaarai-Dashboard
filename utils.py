# utils.py - Utility Functions for VyaapaarAI
"""
Helper functions for logging, validation, file handling, and other utilities
"""

import os
import re
import logging
import logging.handlers
import tempfile
from datetime import datetime, timezone, time # Import time
from typing import Dict, Any, Optional, List
from pathlib import Path

import httpx
from dotenv import load_dotenv
import psutil # Import psutil
import platform # Import platform

# Load environment variables
load_dotenv()

# FIX: Define logger at the module level so all functions can access it
logger = logging.getLogger(__name__)

def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> None:
    """
    Setup logging configuration for the application
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
    """
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Configure logging format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Get log level from env, fallback to arg
    log_level = os.getenv("LOG_LEVEL", log_level).upper()
    numeric_level = getattr(logging, log_level, logging.INFO)
    
    # Configure handlers
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    handlers.append(console_handler)
    
    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
    else:
        # Default rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            logs_dir / "vyaapaarai.log",
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
    
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )
    
    # Set specific logger levels for noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("chromadb").setLevel(logging.WARNING)
    logging.getLogger("sentence_transformers").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.INFO)
    
    # Use the module-level logger
    logger.info(f"Logging configured with level: {log_level}")

def validate_phone_number(phone: str) -> bool:
    """
    Validate phone number format.
    
    Args:
        phone: Phone number string
        
    Returns:
        True if valid, False otherwise
    """
    if not phone or not isinstance(phone, str):
        return False
    
    # Remove all non-digit characters, except leading +
    digits_only = re.sub(r'[^\d+]', '', phone.strip())
    
    # Remove leading + for length check
    check_digits = digits_only
    if check_digits.startswith('+'):
        check_digits = check_digits[1:]

    # Check for reasonable length (e.g., 10 to 15 digits)
    if not (10 <= len(check_digits) <= 15):
        return False
    
    return True

def format_phone_number(phone: str) -> Optional[str]:
    """
    Format phone number for WhatsApp API (E.164 format without '+')
    
    Args:
        phone: Raw phone number string
        
    Returns:
        Formatted phone number or None if invalid
    """
    if not validate_phone_number(phone):
        return None
    
    # Remove all non-digit characters
    digits_only = re.sub(r'\D', '', phone.strip())
    
    # Case 1: 10 digits (assume India)
    if len(digits_only) == 10:
        # Check if it's a valid Indian mobile number (starts with 6, 7, 8, or 9)
        if digits_only[0] in ['6', '7', '8', '9']:
            return "91" + digits_only
    
    # Case 2: 11 digits (assume 0 + 10-digit Indian number)
    elif len(digits_only) == 11 and digits_only.startswith("0"):
        if digits_only[1] in ['6', '7', '8', '9']:
            return "91" + digits_only[1:]
            
    # Case 3: 12 digits (assume 91 + 10-digit Indian number)
    elif len(digits_only) == 12 and digits_only.startswith("91"):
        if digits_only[2] in ['6', '7', '8', '9']:
            return digits_only
            
    # Case 4: Already includes country code (e.g., > 10 digits and not starting with 0)
    elif len(digits_only) > 10 and not digits_only.startswith("0"):
        return digits_only
    
    # If none of the above, it's likely invalid or non-Indian
    logger.warning(f"Could not format phone number: {phone}")
    return None

def save_temp_file(content: bytes, extension: str = ".tmp") -> str:
    """
    Save content to a temporary file
    
    Args:
        content: File content as bytes
        extension: File extension (including the dot)
        
    Returns:
        Path to temporary file
    """
    # Use tempfile.mkstemp to get a unique filename securely
    fd, temp_file_path = tempfile.mkstemp(suffix=extension)
    
    try:
        with os.fdopen(fd, 'wb') as temp_file:
            temp_file.write(content)
        return temp_file_path
    except Exception as e:
        logger.error(f"Failed to write to temp file {temp_file_path}: {e}")
        # Clean up if write fails
        cleanup_temp_file(temp_file_path)
        raise

def cleanup_temp_file(file_path: str) -> None:
    """
    Clean up temporary file if it exists
    
    Args:
        file_path: Path to temporary file
    """
    if not file_path:
        return
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
    except Exception as e:
        logger.warning(f"Failed to cleanup temp file {file_path}: {str(e)}")

def parse_whatsapp_webhook(webhook_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse WhatsApp webhook payload and extract actionable messages
    
    Args:
        webhook_data: Raw webhook payload from WhatsApp
        
    Returns:
        List of parsed message objects
    """
    messages = []
    
    try:
        logger.info(f"🔍 [WHATSAPP_WEBHOOK] Raw webhook data: {webhook_data}")
        entries = webhook_data.get("entry", [])
        logger.info(f"🔍 [WHATSAPP_WEBHOOK] Found {len(entries)} entries")
        
        for entry_idx, entry in enumerate(entries, 1):
            logger.info(f"🔍 [WHATSAPP_WEBHOOK] Processing entry {entry_idx}: {entry}")
            changes = entry.get("changes", [])
            logger.info(f"🔍 [WHATSAPP_WEBHOOK] Found {len(changes)} changes in entry")
            
            for change_idx, change in enumerate(changes, 1):
                logger.info(f"🔍 [WHATSAPP_WEBHOOK] Processing change {change_idx}: {change}")
                value = change.get("value", {})
                logger.info(f"🔍 [WHATSAPP_WEBHOOK] Change value keys: {list(value.keys())}")
                
                # Check for messages
                if "messages" in value:
                    logger.info("✅ [WHATSAPP_WEBHOOK] Found 'messages' in value")
                    message_list = value["messages"]
                    logger.info(f"🔍 [WHATSAPP_WEBHOOK] Processing {len(message_list)} messages")
                    
                    for msg_idx, message in enumerate(message_list, 1):
                        logger.info(f"🔍 [WHATSAPP_WEBHOOK] Processing message {msg_idx}: {message}")
                        
                        # We only care about new, incoming messages
                        if message.get("type") is None:
                            logger.warning(f"⚠️ [WHATSAPP_WEBHOOK] Skipping message with no type: {message}")
                            continue
                            
                        logger.info(f"✅ [WHATSAPP_WEBHOOK] Processing message type: {message.get('type')}")
                            
                        parsed_message = {
                            "id": message.get("id"),
                            "from": message.get("from"),
                            "timestamp": message.get("timestamp"),
                            "type": message.get("type"),
                            "text": None,
                            "media": None
                        }
                        
                        # Extract text content
                        if message["type"] == "text":
                            parsed_message["text"] = message.get("text", {}).get("body")
                            logger.info(f"📝 [WHATSAPP_WEBHOOK] Text message: {parsed_message['text']}")
                        
                        # Extract media content
                        elif message["type"] in ["image", "audio", "video", "document"]:
                            media_type = message["type"]
                            media_data = message.get(media_type, {})
                            parsed_message["media"] = {
                                "type": media_type,
                                "id": media_data.get("id"),
                                "mime_type": media_data.get("mime_type"),
                                "caption": media_data.get("caption")
                            }
                            logger.info(f"🖼️ [WHATSAPP_WEBHOOK] Media message: {parsed_message['media']}")
                        
                        # Handle interactive replies (buttons)
                        elif message["type"] == "interactive":
                            interactive_data = message.get("interactive", {})
                            if interactive_data.get("type") == "button_reply":
                                parsed_message["text"] = interactive_data.get("button_reply", {}).get("title")
                                logger.info(f"🔘 [WHATSAPP_WEBHOOK] Button reply: {parsed_message['text']}")
                            elif interactive_data.get("type") == "list_reply":
                                parsed_message["text"] = interactive_data.get("list_reply", {}).get("title")
                                logger.info(f"📋 [WHATSAPP_WEBHOOK] List reply: {parsed_message['text']}")
                        
                        # Add to list only if it has content
                        if parsed_message["text"] or parsed_message["media"]:
                            messages.append(parsed_message)
                            logger.info("✅ [WHATSAPP_WEBHOOK] Successfully parsed message")
                        else:
                            logger.warning(f"⚠️ [WHATSAPP_WEBHOOK] Empty message content: {parsed_message}")
                
                # Also handle status updates if needed
                elif "statuses" in value:
                    logger.info("ℹ️ [WHATSAPP_WEBHOOK] Processing status updates")
                    for status in value["statuses"]:
                        logger.info(f"📊 [WHATSAPP_WEBHOOK] Status update: {status.get('status')} for {status.get('recipient_id')}")
                else:
                    logger.warning(f"⚠️ [WHATSAPP_WEBHOOK] No 'messages' or 'statuses' found in value. Available keys: {list(value.keys())}")
                        
    except Exception as e:
        logger.error(f"❌ [WHATSAPP_WEBHOOK] Error parsing webhook: {str(e)}", exc_info=True)
    
    logger.info(f"✅ [WHATSAPP_WEBHOOK] Total messages parsed: {len(messages)}")
    return messages

def retry_with_backoff(max_retries: int = 3, backoff_factor: float = 1.0):
    """
    Decorator for retrying functions with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Factor for exponential backoff (e.g., 1.0 -> 1s, 2s, 4s, 8s)
    """
    import functools
    import time
    import asyncio
    
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries.")
                        break
                    
                    wait_time = backoff_factor * (2 ** attempt)
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} for {func.__name__} failed, "
                        f"retrying in {wait_time:.2f}s. Error: {str(e)}"
                    )
                    await asyncio.sleep(wait_time)
            
            raise last_exception  # Re-raise the last exception
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries.")
                        break
                    
                    wait_time = backoff_factor * (2 ** attempt)
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} for {func.__name__} failed, "
                        f"retrying in {wait_time:.2f}s. Error: {str(e)}"
                    )
                    time.sleep(wait_time)
            
            raise last_exception # Re-raise the last exception
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator
# --- ADDED MISSING FUNCTIONS ---

def calculate_business_hours(
    dt: datetime, 
    local_tz: str = "Asia/Kolkata", 
    start_time: time = time(9, 0), 
    end_time: time = time(18, 0), 
    work_days: List[int] = [0, 1, 2, 3, 4, 5]
) -> Dict[str, Any]:
    """
    Check if a given datetime is within business hours in a specific timezone.
    
    Args:
        dt: The datetime object (assumed to be in UTC)
        local_tz: The target timezone (e.g., "Asia/Kolkata" for IST)
        start_time: Business start time
        end_time: Business end time
        work_days: List of weekdays (0=Monday, 6=Sunday)
        
    Returns:
        Dictionary with business hours info
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        # Fallback for Python < 3.9
        from backports.zoneinfo import ZoneInfo
        
    try:
        # Convert UTC datetime to local timezone
        local_datetime = dt.astimezone(ZoneInfo(local_tz))
        
        current_time = local_datetime.time()
        current_weekday = local_datetime.weekday()
        
        is_work_day = current_weekday in work_days
        is_work_time = (start_time <= current_time < end_time)
        
        return {
            "is_business_time": is_work_day and is_work_time,
            "is_work_day": is_work_day,
            "local_time": current_time.strftime("%H:%M:%S"),
            "local_weekday": current_weekday,
            "timezone": local_tz
        }
        
    except Exception as e:
        logger.error(f"Error calculating business hours: {e}", exc_info=True)
        # Fallback: assume it's always business hours if calculation fails
        return {
            "is_business_time": True,
            "error": str(e)
        }

def extract_entities_from_text(text: str) -> Dict[str, List[str]]:
    """
    Extract common entities (email, phone, price) using regex
    
    Args:
        text: Input text
        
    Returns:
        Dictionary of extracted entities
    """
    entities = {
        "emails": [],
        "phones": [],
        "prices": []
    }
    
    # Regex patterns
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    # Simple phone pattern (India focused)
    phone_pattern = r'\b(?:\+?91)?[ -]?[6-9]\d{9}\b'
    # Price pattern (₹, Rs., INR)
    price_pattern = r'\b(?:₹|Rs\.?|INR)\s*[\d,]+(?:\.\d{1,2})?\b'
    
    try:
        entities["emails"] = re.findall(email_pattern, text)
        
        # Format phones as they are found
        raw_phones = re.findall(phone_pattern, text)
        entities["phones"] = [format_phone_number(p) for p in raw_phones if format_phone_number(p)]
        
        entities["prices"] = re.findall(price_pattern, text)
        
    except Exception as e:
        logger.warning(f"Error during regex entity extraction: {e}")
        
    return entities

def get_system_info() -> Dict[str, Any]:
    """Get basic system information"""
    try:
        return {
            "platform": platform.system(),
            "platform_release": platform.release(),
            "cpu_percent": psutil.cpu_percent(interval=None),
            "memory_percent": psutil.virtual_memory().percent
        }
    except Exception as e:
        logger.warning(f"Could not get system info: {e}")
        return {"error": str(e)}

# --- END ADDED FUNCTIONS ---

# Environment validation
def validate_environment() -> Dict[str, bool]:
    """
    Validate required environment variables
    
    Returns:
        Dictionary with validation results
    """
    required_vars = [
        "MONGO_URI",
        "WHATSAPP_PHONE_NUMBER_ID", 
        "WHATSAPP_ACCESS_TOKEN",
        "WHATSAPP_WEBHOOK_VERIFY_TOKEN",
        "WHATSAPP_APP_SECRET", # Added check for webhook secret
        "GEMINI_API_KEY",
        "JWT_SECRET"
    ]
    
    results = {}
    missing = []
    for var in required_vars:
        value = os.getenv(var)
        is_present = bool(value and len(value.strip()) > 0)
        results[var] = is_present
        if not is_present:
            missing.append(var)
            
    if missing:
        logger.critical(f"Missing critical environment variables: {', '.join(missing)}")
    else:
        logger.info("All critical environment variables are present.")
        
    return results

if __name__ == "__main__":
    # Test utilities
    setup_logging("DEBUG")
    print("VyaapaarAI Utilities Module Test")
    print("================================")
    
    # Test phone validation
    print("\n--- Phone Validation ---")
    test_phones = ["+91 98765 43210", "9876543210", "invalid", "91-9876543210", "09876543210", "12345"]
    for phone in test_phones:
        is_valid = validate_phone_number(phone)
        formatted = format_phone_number(phone)
        print(f"Phone: {phone:<18} -> Valid: {is_valid:<5}, Formatted: {formatted}")
    
    # Test environment validation
    print("\n--- Environment Validation ---")
    env_status = validate_environment()
    for var, status in env_status.items():
        print(f"  {var:<30}: {'✓ Present' if status else '✗ MISSING'}")

    # --- ADDED TESTS ---
    print("\n--- Business Hours Test ---")
    utc_now = datetime.now(timezone.utc)
    business_info = calculate_business_hours(utc_now)
    print(f"Current UTC time: {utc_now.isoformat()}")
    print(f"Business hours info: {business_info}")
    
    print("\n--- Entity Extraction Test ---")
    test_text = "Hi, call me at 9876543210 or +91-8765432109. My email is test@example.com. Price is ₹1,500."
    entities = extract_entities_from_text(test_text)
    print(f"Entities in '{test_text}':")
    print(json.dumps(entities, indent=2))
    # --- END ADDED TESTS ---

    print("\nUtilities module initialized successfully!")