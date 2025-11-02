"""
ai_pipeline_v6.py (Unified Merchant & Admin Pipeline)

Merges the advanced merchant pipeline (v5) with admin functionalities (v4)
into a single, role-aware, high-performance module.

FEATURES:
- Unified Context: Handles both 'MERCHANT' and 'ADMIN' roles.
- Role-Based Routing: Directs requests to merchant or admin logic paths.
- Advanced Merchant Flow: Retains all v5 features (Redis, fuzzy matching,
  advanced scenarios, state machine, cart logic).
- Admin Insight Flow: Adds admin-specific intents for analytics,
  merchant management, and reporting.
- Shared Resources: Uses a single Redis cache, Gemini model pool,
  and rate limiter for efficiency.
- Unified Analytics: Logs analytics with role-awareness.
"""

from typing import Dict, List, Optional, Tuple, Any, AsyncGenerator
import logging
import re
from datetime import datetime, timezone, timedelta
import asyncio
import json
from difflib import SequenceMatcher
from enum import Enum
import google.generativeai as genai
import hashlib
import time
from functools import lru_cache
import os
from bson import ObjectId

# Attempt to import utils, assume they exist per v6 plan
try:
    from utils import log_performance, sanitize_text
except ImportError:
    logging.warning("utils.py not found. Using basic sanitization.")
    def sanitize_text(text: str) -> str:
        return text.strip()
    def log_performance(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper

# Import advanced scenario handlers from v5
# These are crucial for the merchant flow
try:
    from ai_pipeline_advanced_scenarios_v35 import (
        AdvancedAIPipelineHandler,
        AdvancedCartStateMachine,
        BlindSpotResolver,
        handle_blind_spot_request,
        handle_scenario_request,
        RequestAmbiguity,
        ScenarioType,
        FuzzyIntentMatcher as V5FuzzyMatcher
    )
except ImportError:
    logging.critical("CRITICAL: ai_pipeline_advanced_scenarios_v35.py not found.")
    # Create dummy classes to avoid crashing
    class AdvancedAIPipelineHandler: pass
    class AdvancedCartStateMachine: pass
    class BlindSpotResolver: pass
    class V5FuzzyMatcher: pass
    async def handle_blind_spot_request(*args): return None, None
    async def handle_scenario_request(*args): return "Scenario handler not found."

logger = logging.getLogger(__name__)


# ==========================================
# 1. UNIFIED CONVERSATION CONTEXT & ROLE
# ==========================================

class ConversationRole(Enum):
    """Defines the role of the user in the conversation."""
    MERCHANT = 'merchant'
    ADMIN = 'admin'

class ConversationState(Enum):
    """Conversation states for MERCHANT multi-turn dialogue"""
    IDLE = "idle"
    BROWSING = "browsing"
    PRODUCT_SELECTED = "product_selected"
    QUANTITY_PENDING = "quantity_pending"
    MODIFYING_CART = "modifying_cart"
    CONFIRMING_ORDER = "confirming_order"
    ORDER_PLACED = "order_placed"
    ERROR = "error"

class ConversationContext:
    """
    Unified context for both Merchants (customers) and Admins.
    Retains the advanced state machine from v5 for merchants.
    """
    
    def __init__(self, conversation_id: str, merchant_id: str, 
                 user_ref: str, role: ConversationRole):
        self.conversation_id = conversation_id
        self.merchant_id = merchant_id  # The merchant being interacted with
        self.user_ref = user_ref        # Customer phone or Admin ID
        self.role = role
        
        # Conversation state (primarily for merchant flow)
        self.state = ConversationState.IDLE
        self.message_history: List[Dict] = []
        self.max_history = 50
        
        # Merchant operation context
        self.last_product_discussed: Optional[Dict] = None
        self.pending_quantity: Optional[float] = None
        self.pending_modifications: Dict[str, Any] = {}
        
        # Merchant cart context
        self.cart_items_snapshot: List[Dict] = []
        self.last_cart_action: Optional[Dict] = None
        
        # Intent tracking
        self.last_intent: Optional[str] = None
        self.intent_confidence: float = 0.0
        
        # Timestamps
        self.created_at = datetime.now(timezone.utc)
        self.last_updated_at = datetime.now(timezone.utc)
        self.last_activity_at = datetime.now(timezone.utc)
    
    def update_state(self, new_state: ConversationState, reason: str = ""):
        """Transition to new state (for merchant flow)"""
        if self.role != ConversationRole.MERCHANT:
            return  # State machine is for merchants
        
        old_state = self.state
        self.state = new_state
        self.last_updated_at = datetime.now(timezone.utc)
        logger.debug(f"State: {old_state.value} -> {new_state.value} ({reason})")
    
    def add_message(self, role: str, content: str):
        """Add to message history with auto-pruning"""
        self.message_history.append({
            "role": role,
            "content": content,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        if len(self.message_history) > self.max_history:
            self.message_history = self.message_history[-self.max_history:]
        self.last_activity_at = datetime.now(timezone.utc)
    
    def get_recent_context(self, num_messages: int = 10) -> str:
        """Get recent messages for Gemini context"""
        messages = self.message_history[-num_messages:]
        return "\n".join([f"{m['role']}: {m['content']}" for m in messages])
    
    def to_dict(self) -> Dict:
        """Serialize context for storage (now includes role/user_ref)"""
        return {
            "conversation_id": self.conversation_id,
            "merchant_id": self.merchant_id,
            "user_ref": self.user_ref,
            "role": self.role.value,
            "state": self.state.value,
            "message_history": self.message_history,
            "last_product_discussed": self.last_product_discussed,
            "pending_quantity": self.pending_quantity,
            "pending_modifications": self.pending_modifications,
            "cart_items_snapshot": self.cart_items_snapshot,
            "last_cart_action": self.last_cart_action,
            "last_intent": self.last_intent,
            "intent_confidence": self.intent_confidence,
            "created_at": self.created_at.isoformat(),
            "last_updated_at": self.last_updated_at.isoformat()
        }


# ==========================================
# 2. SHARED RESOURCES (Cache, Rate Limiter)
# ==========================================

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis not installed. Falling back to in-memory cache.")

class RedisCache:
    """Redis-based cache from v5, robustly handles serialization."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.client: Optional[redis.Redis] = None
    
    async def connect(self):
        if not REDIS_AVAILABLE:
            logger.warning("Redis not available, skipping connection")
            return
        try:
            self.client = await redis.from_url(self.redis_url, decode_responses=True)
            await self.client.ping()
            logger.info("Connected to Redis")
        except Exception as e:
            logger.error(f"Redis connection failed: {e}. Caching disabled.")
            self.client = None
    
    async def get(self, key: str) -> Optional[Dict]:
        if not self.client: return None
        try:
            value = await self.client.get(key)
            if value:
                return json.loads(value)
        except Exception as e:
            logger.error(f"Cache get error: {e}")
        return None
    
    async def set(self, key: str, value: Dict, ttl: int = 3600):
        if not self.client: return
        try:
            def _json_converter(o: Any):
                if isinstance(o, ObjectId): return str(o)
                if isinstance(o, datetime): return o.isoformat()
                if isinstance(o, Enum): return o.value
                raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

            await self.client.setex(key, ttl, json.dumps(value, default=_json_converter))
        except Exception as e:
            logger.error(f"Cache set error: {e}")
    
    async def close(self):
        if self.client:
            await self.client.close()

class RateLimiter:
    """Token bucket rate limiter from v5"""
    def __init__(self, max_requests: int = 20, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: Dict[str, List[float]] = {}
    
    async def is_allowed(self, key: str) -> bool:
        now = time.time()
        if key not in self.requests:
            self.requests[key] = []
        
        self.requests[key] = [t for t in self.requests[key] if now - t < self.time_window]
        
        if len(self.requests[key]) < self.max_requests:
            self.requests[key].append(now)
            return True
        
        return False

# Retain v5's FuzzyIntentMatcher for merchant product typos
class FuzzyIntentMatcher(V5FuzzyMatcher):
    """Inherits from v5's advanced fuzzy matcher"""
    pass


# ==========================================
# 3. UNIFIED AI PIPELINE (v6)
# ==========================================

class UnifiedAIPipelineV6:
    """
    Unified AI Pipeline (v6)
    Orchestrates both Merchant and Admin conversational flows.
    """
    
    def __init__(self, db_module, cart_manager, order_manager, 
                 inventory_manager, knowledge_detector, 
                 gemini_api_key: str = None,
                 redis_url: str = "redis://localhost:6379/0"):
        
        # --- Core Modules (from v5) ---
        self.db = db_module
        self.cart = cart_manager
        self.orders = order_manager
        self.inventory = inventory_manager
        self.knowledge = knowledge_detector
        
        # --- Gemini Setup ---
        self.gemini_api_key = gemini_api_key or os.getenv("GEMINI_API_KEY")
        if self.gemini_api_key:
            genai.configure(api_key=self.gemini_api_key)
            self.model = genai.GenerativeModel("gemini-2.0-flash") # Use 2.0-flash
        else:
            logger.critical("GEMINI_API_KEY not configured. AI Pipeline will fail.")
            self.model = None
        
        # --- Shared Resources ---
        self.cache = RedisCache(redis_url)
        self.rate_limiter = RateLimiter(max_requests=30, time_window=60)
        self.connection_pool = asyncio.Semaphore(10) # Increased pool size
        
        # --- In-memory context cache (fallback) ---
        self._contexts: Dict[str, ConversationContext] = {}
        
        # --- Analytics (v6 style) ---
        self.analytics_log: List[Dict] = []
        
        # --- Merchant Flow Handlers (from v5) ---
        self.advanced_handler = AdvancedAIPipelineHandler()
        self.cart_state_machine = AdvancedCartStateMachine()
        self.blind_spot_resolver = BlindSpotResolver()
        self.fuzzy_matcher = FuzzyIntentMatcher()
        
        logger.info("Unified AI Pipeline (v6) initialized.")
    
    async def connect(self):
        """Initialize connections (call during app startup)"""
        await self.cache.connect()
    
    async def close(self):
        """Cleanup (call during app shutdown)"""
        await self.cache.close()

    # ==========================================
    # 4. UNIFIED CONTEXT MANAGEMENT
    # ==========================================
    
    async def get_or_create_context(self, conversation_id: str, merchant_id: str, 
                                     user_ref: str, role: ConversationRole) -> ConversationContext:
        """Get context from cache or DB or create new, now role-aware."""
        
        # 1. Try Redis cache (role-aware key)
        cache_key = f"ctx:{role.value}:{conversation_id}"
        cached = await self.cache.get(cache_key)
        if cached:
            logger.debug(f"Context loaded from Redis cache for {role.value}")
            ctx = self._dict_to_context(cached)
            self._contexts[conversation_id] = ctx # Update in-memory
            return ctx
        
        # 2. Try in-memory cache
        if conversation_id in self._contexts:
            logger.debug(f"Context loaded from memory for {role.value}")
            return self._contexts[conversation_id]
        
        # 3. Try database
        try:
            stored_context = await self.db.db["conversation_contexts"].find_one(
                {"conversation_id": conversation_id}
            )
            if stored_context:
                logger.debug(f"Context loaded from database")
                ctx = self._dict_to_context(stored_context)
                self._contexts[conversation_id] = ctx
                await self.cache.set(cache_key, stored_context, ttl=3600) # Cache it
                return ctx
        except Exception as e:
            logger.error(f"Error loading context from DB: {e}")
        
        # 4. Create new context
        logger.debug(f"Creating new {role.value} context")
        ctx = ConversationContext(conversation_id, merchant_id, user_ref, role)
        self._contexts[conversation_id] = ctx
        return ctx
    
    def _dict_to_context(self, data: Dict) -> ConversationContext:
        """Reconstruct ConversationContext from dict (handles v5->v6 upgrade)"""
        ctx = ConversationContext(
            data["conversation_id"],
            data["merchant_id"],
            # Handle old v5 format ("customer_phone") vs new "user_ref"
            data.get("user_ref") or data.get("customer_phone", "unknown"),
            # Handle old v5 format (no role) vs new "role"
            ConversationRole(data.get("role", "merchant"))
        )
        ctx.state = ConversationState(data.get("state", "idle"))
        ctx.message_history = data.get("message_history", [])
        ctx.last_product_discussed = data.get("last_product_discussed")
        ctx.pending_quantity = data.get("pending_quantity")
        ctx.cart_items_snapshot = data.get("cart_items_snapshot", [])
        return ctx
    
    async def persist_context_async(self, context: ConversationContext):
        """Persist context (non-blocking, role-aware key)"""
        cache_key = f"ctx:{context.role.value}:{context.conversation_id}"
        context_dict = context.to_dict()
        
        await self.cache.set(cache_key, context_dict, ttl=3600)
        asyncio.create_task(self._persist_to_db(context.conversation_id, context_dict))
    
    async def _persist_to_db(self, conversation_id: str, context_dict: Dict):
        """Background task to persist to database"""
        try:
            await self.db.db["conversation_contexts"].update_one(
                {"conversation_id": conversation_id},
                {"$set": context_dict},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error persisting context to DB: {e}")

    # ==========================================
    # 5. ANALYTICS (v6)
    # ==========================================
    
    def _log_analytic_event(self, role: ConversationRole, intent: str, response_time: float):
        """Logs an analytic event in the v6 format."""
        self.analytics_log.append({
            "role": role.value,
            "intent": intent,
            "response_time": response_time,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    def get_analytics(self) -> Dict:
        """Get aggregated performance analytics (v6 spec)."""
        total_requests = len(self.analytics_log)
        if total_requests == 0:
            avg_time = 0
        else:
            avg_time = sum(a['response_time'] for a in self.analytics_log) / total_requests

        intent_breakdown = {}
        for a in self.analytics_log:
            intent_breakdown[a['intent']] = intent_breakdown.get(a['intent'], 0) + 1

        return {
            'total_requests': total_requests,
            'avg_response_time_sec': avg_time,
            'role_breakdown': {
                'merchant': sum(1 for a in self.analytics_log if a['role'] == ConversationRole.MERCHANT.value),
                'admin': sum(1 for a in self.analytics_log if a['role'] == ConversationRole.ADMIN.value)
            },
            'intent_breakdown': intent_breakdown,
            'contexts_in_memory': len(self._contexts),
        }

    # ==========================================
    # 6. MAIN ORCHESTRATOR (v6)
    # ==========================================
    
    async def process_message(
        self,
        conversation_id: str,
        merchant_id: str,       # Merchant being acted upon
        user_ref: str,          # Customer Phone or Admin ID
        role: ConversationRole, # The role of the user
        message_text: str,
        customer_name: Optional[str] = None, # For merchant greetings
    ) -> str:
        """
        Main entry point. Gets context and routes based on role.
        """
        # 1. GET CONTEXT (Unified)
        try:
            context = await self.get_or_create_context(
                conversation_id, merchant_id, user_ref, role
            )
        except Exception as e:
            logger.critical(f"Failed to get context: {e}", exc_info=True)
            return "Sorry, I'm having trouble retrieving your session. Please try again."

        # 2. SANITIZE
        message_text = sanitize_text(message_text)

        # 3. ROUTE BASED ON ROLE
        if role == ConversationRole.MERCHANT:
            return await self._handle_merchant_message(
                context, message_text, customer_name
            )
        elif role == ConversationRole.ADMIN:
            return await self._handle_admin_message(
                context, message_text
            )
        else:
            logger.error(f"Unknown role provided: {role}")
            return "Error: Invalid user role specified."

    # ==========================================
    # 7. ADMIN FLOW HANDLERS (v6)
    # ==========================================

    async def _handle_admin_message(
        self,
        context: ConversationContext,
        message_text: str
    ) -> str:
        """Handles all logic for Admin users."""
        start_time = time.time()
        intent = "unknown"
        
        try:
            context.add_message("admin", message_text)
            
            # 1. Get Admin Intents
            admin_intents = [
                "view_merchant_stats", "list_pending_merchants", "approve_merchant",
                "generate_report", "summarize_conversations", "merchant_overview",
                "revenue_report", "general_help"
            ]
            
            # 2. Query Gemini for Admin Intent
            ai_response = await self._query_gemini_admin(context, message_text, admin_intents)
            intent = ai_response.get("intent", "unknown")
            
            # 3. Route to Admin Handlers
            if intent == "view_merchant_stats":
                response = await self._handle_admin_view_stats(context, ai_response)
            elif intent == "list_pending_merchants":
                response = await self._handle_admin_list_pending(context, ai_response)
            elif intent == "approve_merchant":
                response = await self._handle_admin_approve_merchant(context, ai_response)
            elif intent == "generate_report" or intent == "revenue_report":
                response = await self._handle_admin_generate_report(context, ai_response)
            elif intent == "summarize_conversations":
                response = await self._handle_admin_summarize_convos(context, ai_response)
            elif intent == "merchant_overview":
                response = await self._handle_admin_view_stats(context, ai_response) # Alias
            else: # general_help or unknown
                response = "I am the Admin AI. You can ask for:\n" \
                           "- 'Merchant stats for [merchant_id]'\n" \
                           "- 'List pending merchants'\n" \
                           "- 'Generate revenue report for last 7 days'\n" \
                           "- 'Summarize conversations for [merchant_id]'"

            # 4. Save and Log
            context.add_message("assistant", response)
            await self.persist_context_async(context)
            
            response_time = time.time() - start_time
            self._log_analytic_event(ConversationRole.ADMIN, intent, response_time)
            
            return response

        except Exception as e:
            logger.error(f"Error in admin handler: {e}", exc_info=True)
            response_time = time.time() - start_time
            self._log_analytic_event(ConversationRole.ADMIN, intent, response_time)
            return "An error occurred while processing your admin request."

    async def _query_gemini_admin(self, context: ConversationContext, text: str, intents: List[str]) -> Dict:
        """Uses Gemini to get intent for an Admin request."""
        if not self.model:
            return {"intent": "unknown", "confidence": 0.0}
        
        if not await self.rate_limiter.is_allowed(context.user_ref):
            logger.warning(f"Rate limit exceeded for admin {context.user_ref}")
            return {"intent": "unknown", "confidence": 0.0}

        intent_list_str = ", ".join(intents)
        recent_context = context.get_recent_context(5)
        
        prompt = f"""You are a powerful Admin Assistant AI for an e-commerce platform.
Analyze the admin's request and identify the primary intent and any relevant entities.

RECENT CONVERSATION:
{recent_context}

Latest Admin Request: "{text}"

Possible Intents: [{intent_list_str}]

Return ONLY a JSON object with the following structure:
{{
  "intent": "intent_name",
  "confidence": <0.0-1.0>,
  "merchant_id_target": "merchant_id or 'all' or null",
  "timeframe": "today|7_days|30_days|null",
  "report_type": "revenue|onboarding|conversations|null"
}}"""

        try:
            async with self.connection_pool:
                response = await asyncio.to_thread(
                    self.model.generate_content,
                    prompt,
                    generation_config=genai.types.GenerationConfig(
                        candidate_count=1,
                        max_output_tokens=200,
                        temperature=0.1,
                        response_mime_type="application/json"
                    ),
                    request_options={"timeout": 20}
                )
            
            # Gemini-2.0-flash with JSON mime type should return clean JSON
            result = json.loads(response.text)
            
            if result.get("intent") not in intents:
                result["intent"] = "unknown"
                result["confidence"] = 0.1
                
            logger.debug(f"Admin AI Response: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Admin Gemini error: {e}", exc_info=True)
            return {"intent": "unknown", "confidence": 0.0}

    # --- Admin Handler Stubs (to be implemented) ---

    async def _handle_admin_view_stats(self, context: ConversationContext, entities: Dict) -> str:
        merchant_target = entities.get("merchant_id_target") or context.merchant_id
        # TODO: Add DB query logic
        return f"[Stub] Fetching stats for merchant: {merchant_target}"

    async def _handle_admin_list_pending(self, context: ConversationContext, entities: Dict) -> str:
        # TODO: Add DB query logic
        return f"[Stub] Fetching list of pending merchants..."

    async def _handle_admin_approve_merchant(self, context: ConversationContext, entities: Dict) -> str:
        merchant_target = entities.get("merchant_id_target")
        if not merchant_target:
            return "Please specify which merchant to approve."
        # TODO: Add DB update logic
        return f"[Stub] Approving merchant: {merchant_target}"

    async def _handle_admin_generate_report(self, context: ConversationContext, entities: Dict) -> str:
        report_type = entities.get("report_type", "revenue")
        timeframe = entities.get("timeframe", "7_days")
        # TODO: Add DB aggregation logic
        return f"[Stub] Generating {report_type} report for last {timeframe}."

    async def _handle_admin_summarize_convos(self, context: ConversationContext, entities: Dict) -> str:
        merchant_target = entities.get("merchant_id_target") or context.merchant_id
        # TODO: Add logic to fetch conversations and summarize
        return f"[Stub] Summarizing conversations for merchant: {merchant_target}"


    # ==========================================
    # 8. MERCHANT FLOW HANDLERS (from v5)
    # ==========================================

    async def _handle_merchant_message(
        self,
        context: ConversationContext,
        message_text: str,
        customer_name: Optional[str] = None
    ) -> str:
        """
        Main entry point for MERCHANT logic.
        This is the complete, advanced process_message from v5,
        now encapsulated as a private method.
        """
        start_time = time.time()
        intent = "unknown"
        response = ""
        
        try:
            # 1. ADD MESSAGE TO HISTORY (already done in main func, but good to be explicit)
            context.add_message("user", message_text)
            
            # 2. GREETING FAST-PATH (from v5)
            greeting_words = {"hello", "hi", "hey", "namaste"}
            message_lower = message_text.lower().strip()
            is_greeting = (
                message_lower in greeting_words or
                any(g in message_lower and len(message_lower) < 20 for g in greeting_words)
            )
            if is_greeting:
                intent = "greeting"
                response = f"Hi {customer_name or 'there'}!\nWelcome to our store! " \
                           "How can I help?\n\n" \
                           "Try: 'Add 2kg rice' or 'Show cart'"
                
                # Fast-path exit
                context.add_message("assistant", response)
                await self.persist_context_async(context)
                response_time = time.time() - start_time
                self._log_analytic_event(ConversationRole.MERCHANT, intent, response_time)
                return response

            # 3. BLIND SPOT DETECTION (from v5)
            blind_spot_entities, clarification = await handle_blind_spot_request(
                message_text, context, self.advanced_handler
            )
            if clarification:
                intent = "clarification_needed"
                response = clarification
                
                # Fast-path exit for clarification
                context.add_message("assistant", response)
                await self.persist_context_async(context)
                response_time = time.time() - start_time
                self._log_analytic_event(ConversationRole.MERCHANT, intent, response_time)
                return response
            
            # 4. DETECT INTENT & ENTITIES (Combined v5 call)
            ai_response = await self._get_ai_response_cached_merchant(
                message_text, context
            )
            intent = ai_response.get("intent", "general_query")
            entities = ai_response
            
            if blind_spot_entities:
                entities.update(blind_spot_entities)
                logger.debug(f"Merged entities: {entities}")

            context.last_intent = intent
            context.intent_confidence = ai_response.get("confidence", 0.0)

            # 5. SCENARIO HANDLING (from v5)
            if "scenario" in message_text.lower():
                response = await handle_scenario_request(
                    context.conversation_id, message_text, "custom",
                    context, self.advanced_handler
                )
                intent = "scenario_test"
            else:
                # 6. ROUTE TO HANDLER (v5 logic)
                response = await self._route_merchant_intent(
                    intent, context, entities, message_text
                )
            
            # 7. ADD RESPONSE TO HISTORY
            context.add_message("assistant", response)
            
            # 8. PERSIST ASYNC
            await self.persist_context_async(context)
            
            # 9. UPDATE ANALYTICS
            response_time = time.time() - start_time
            self._log_analytic_event(ConversationRole.MERCHANT, intent, response_time)
            
            logger.info(f"Merchant message processed in {response_time:.2f}s | Intent: {intent}")
            return response
            
        except Exception as e:
            logger.error(f"Error in merchant handler: {e}", exc_info=True)
            response_time = time.time() - start_time
            self._log_analytic_event(ConversationRole.MERCHANT, "handler_error", response_time)
            return "Sorry, I encountered an error. Please try again."

    @lru_cache(maxsize=1000)
    def _get_ai_response_cache_key_merchant(self, message_lower: str, state: str) -> str:
        """Generate cache key for the *merchant* AI response"""
        return f"ai_resp_merchant:{hashlib.md5(f'{message_lower}:{state}'.encode()).hexdigest()}"

    async def _get_ai_response_cached_merchant(
        self,
        message_text: str,
        context: ConversationContext
    ) -> Dict[str, Any]:
        """
        Combined Intent & Entity extraction for MERCHANTS (from v5).
        """
        cache_key = self._get_ai_response_cache_key_merchant(message_text.lower(), context.state.value)
        cached_result = await self.cache.get(cache_key)
        if cached_result:
            logger.debug(f"Merchant AI response from cache")
            return cached_result
        
        if not self.model:
            return {"intent": "general_query", "confidence": 0.0}
        
        if not await self.rate_limiter.is_allowed(context.user_ref): # Keyed by customer phone
            logger.warning(f"Rate limit exceeded for merchant customer {context.user_ref}")
            return {"intent": context.last_intent or "general_query", "confidence": 0.0}
        
        possible_intents = [
            "greeting", "product_search", "add_to_cart", "modify_quantity", 
            "remove_from_cart", "view_cart", "confirm_order", "order_status", 
            "cancel_order", "general_help", "general_query"
        ]
        
        intent_list_str = ", ".join(possible_intents)
        recent_context_str = context.get_recent_context(5)
        last_product_name = context.last_product_discussed.get("product_name", "None") if context.last_product_discussed else "None"
        
        prompt = f"""You are a WhatsApp shopping assistant. Analyze the user's message and return a single JSON object.

RECENT CONTEXT:
{recent_context_str}

Last Product Discussed: {last_product_name}
Current Cart: {[item.get('product_name') for item in context.cart_items_snapshot]}

Latest User Message: "{message_text}"
Current State: {context.state.value}

Possible Intents: [{intent_list_str}]

Return ONLY a JSON object with the following structure:
{{
  "intent": "intent_name",
  "confidence": <0.0-1.0>,
  "product_name": "name or null",
  "quantity": number or null,
  "unit": "kg|L|piece|pack|null"
}}"""

        try:
            async with self.connection_pool:
                response = await asyncio.to_thread(
                    self.model.generate_content,
                    prompt,
                    generation_config=genai.types.GenerationConfig(
                        candidate_count=1,
                        max_output_tokens=150,
                        temperature=0.0,
                        response_mime_type="application/json"
                    ),
                    request_options={"timeout": 20}
                )
            
            result = json.loads(response.text)
            
            if result.get("intent") not in possible_intents:
                result["intent"] = "general_query"
                result["confidence"] = 0.1
            
            logger.debug(f"Merchant AI Response: {result}")
            await self.cache.set(cache_key, result, ttl=1800) # 30 min cache
            return result
            
        except Exception as e:
            logger.error(f"Merchant AI call error: {e}", exc_info=True)
            return {"intent": context.last_intent or "general_query", "confidence": 0.0}

    # --- Merchant Intent Router (from v5) ---

    async def _route_merchant_intent(
        self,
        intent: str,
        context: ConversationContext,
        entities: Dict,
        message_text: str
    ) -> str:
        """Route to appropriate MERCHANT handler (from v5)"""
        
        handlers = {
            "greeting": self._handle_merchant_greeting,
            "product_search": self._handle_merchant_product_search,
            "add_to_cart": self._handle_merchant_add_to_cart,
            "modify_quantity": self._handle_merchant_modify_quantity,
            "remove_from_cart": self._handle_merchant_remove_from_cart,
            "view_cart": self._handle_merchant_view_cart,
            "confirm_order": self._handle_merchant_confirm_order,
            "order_status": self._handle_merchant_order_status,
            "general_help": self._handle_merchant_general_help,
        }
        
        if any(word in message_text.lower() for word in ["cancel", "reset", "clear", "start over"]):
            return await self._handle_merchant_cart_reset(context)
        
        handler = handlers.get(intent, self._handle_merchant_general_query)
        return await handler(context, entities, message_text)

    # --- Merchant Handlers (Copied from v5, renamed) ---
    # These methods are identical to v5, just renamed for encapsulation.
    
    async def _handle_merchant_greeting(self, context: ConversationContext, 
                               entities: Dict, message_text: str) -> str:
        context.update_state(ConversationState.IDLE, "Greeted")
        return """Hello! Welcome to our store!
- Search: "Do you have rice?"
- Order: "Add 5kg rice"
- Cart: "Show my cart"
- Pay: "Confirm order"
What would you like?"""
    
    async def _handle_merchant_product_search(self, context: ConversationContext,
                                     entities: Dict, message_text: str) -> str:
        product_query = str(entities.get("product_name") or "").strip()
        if not product_query: return "Which product are you looking for?"
        
        try:
            matches = await self._search_products_parallel(context.merchant_id, product_query)
            if not matches:
                return f"We don't have '{product_query}' right now."
            
            response = f"Found {len(matches)} items:\n\n"
            for idx, product in enumerate(matches[:3], 1):
                response += f"{idx}. **{product['product_name']}**\n"
                response += f"   Rs {product['price']:.2f}/{product.get('unit', 'piece')}\n"
                response += f"   Stock: {product.get('stock', 0)}\n\n"
            
            if matches:
                context.last_product_discussed = matches[0]
                context.update_state(ConversationState.PRODUCT_SELECTED, "Found products")
            return response
        except Exception as e:
            logger.error(f"Product search error: {e}")
            return "Error searching products."
    
    async def _search_products_parallel(self, merchant_id: str, query: str) -> List[Dict]:
        """Parallel search from v5"""
        async def search_inventory():
            try:
                product = await self._find_product_robustly(merchant_id, query)
                return [product] if product else []
            except: return []
        
        async def search_knowledge():
            try:
                return await self.knowledge.search_knowledge(merchant_id, f"product {query}", top_k=3)
            except: return None
        
        inv_results, _ = await asyncio.gather(search_inventory(), search_knowledge())
        return inv_results if isinstance(inv_results, list) else []
    
    async def _find_product_robustly(self, merchant_id: str, product_name: str) -> Optional[Dict]:
        """Robust product search from v5"""
        product = await self.inventory.get_product_by_name(merchant_id, product_name)
        if product: return product
        try:
            products_coll = self.db.db["products"]
            normalized_name = (product_name or "").strip()
            product = await products_coll.find_one({
                "merchant_id": merchant_id,
                "product_name": {"$regex": f"^{re.escape(normalized_name)}$", "$options": "i"}
            })
            if product:
                logger.warning(f"Found '{product_name}' in 'products', not 'inventory'.")
                return product
        except Exception as e:
            logger.error(f"Fallback product search error: {e}")
        return None

    async def _handle_merchant_add_to_cart(self, context: ConversationContext, entities: Dict, message_text: str = "") -> str:
        """v5's advanced add_to_cart (handles duplicates)"""
        product_name = str(entities.get("product_name") or "").strip()
        quantity = entities.get("quantity")
        unit = entities.get("unit", "piece")
        
        if not product_name or quantity is None:
            return "Please specify: product name and quantity. (e.g., 'Add 5kg rice')"
        
        try:
            product = await self._find_product_robustly(context.merchant_id, product_name)
            if not product:
                product_name, confidence = self.fuzzy_matcher.fix_product_typo(product_name)
                if confidence > 0.7:
                    product = await self._find_product_robustly(context.merchant_id, product_name)
                if not product:
                    return f"'{product_name}' not found."
            
            product_unique_id = product.get("product_id") or str(product.get("_id"))
            if not product_unique_id:
                return "Error: Product data is corrupted."

            current_cart_doc = await self.cart.get_cart(context.conversation_id)
            if not current_cart_doc:
                 cart_obj = await self.cart.get_or_create_cart(context.conversation_id)
                 current_cart_doc = cart_obj.to_dict()

            current_cart = self.cart._dict_to_cart(current_cart_doc)
            existing_item = None
            
            if current_cart and current_cart.items:
                for item in current_cart.items:
                    if item.product_id == product_unique_id:
                        existing_item = item
                        break
            
            item_data = {
                "product_id": product_unique_id,
                "product_name": product["product_name"],
                "quantity": quantity,
                "unit_price": product["price"],
                "unit": unit,
            }

            if existing_item:
                old_qty = existing_item.quantity
                await self.cart.add_item_safe(context.conversation_id, item_data)
                context.update_state(ConversationState.MODIFYING_CART, f"Updated qty")
                return f"Updated! {product['product_name']}: {old_qty}{unit} -> {quantity}{unit}\n" \
                       f"Price: Rs {product['price'] * quantity:.2f}"
            else:
                await self.cart.add_item_safe(context.conversation_id, item_data)
                context.update_state(ConversationState.IDLE, f"Added {product_name}")
                context.last_product_discussed = product
                return f"Added! {product['product_name']}: {quantity}{unit}\n" \
                       f"Price: Rs {product['price'] * quantity:.2f}"
        except Exception as e:
            logger.error(f"Add to cart error: {e}", exc_info=True)
            return "Error adding to cart."

    async def _handle_merchant_modify_quantity(self, context: ConversationContext,
                                      entities: Dict, message_text: str) -> str:
        """v5's context-aware modify quantity"""
        new_qty = entities.get("quantity")
        if new_qty is None:
            return "How much? (e.g., 'Change to 10kg')"
        
        try:
            product_name = str(entities.get("product_name") or "").strip()
            product = None
            if not product_name and context.last_product_discussed:
                product = context.last_product_discussed
            elif product_name:
                product = await self._find_product_robustly(context.merchant_id, product_name)
            
            if not product:
                return "Which product? (e.g., 'Change rice to 10kg')"

            product_unique_id = product.get("product_id") or str(product.get("_id"))
            if not product_unique_id: return "Error: Product data is corrupted."
            
            current_cart = self.cart._dict_to_cart(await self.cart.get_cart(context.conversation_id))
            if not current_cart: return "Cart is empty."
            
            for item in current_cart.items:
                if item.product_id == product_unique_id:
                    await self.cart.add_item_safe(context.conversation_id, {
                        "product_id": product_unique_id, "product_name": product["product_name"],
                        "quantity": new_qty, "unit_price": product["price"], "unit": item.unit or "piece",
                    })
                    context.update_state(ConversationState.MODIFYING_CART, "Modified qty")
                    return f"Updated: {new_qty}{item.unit or 'piece'}\nRs {product['price'] * new_qty:.2f}"
            
            return f"'{product['product_name']}' not in cart."
        except Exception as e:
            logger.error(f"Modify qty error: {e}", exc_info=True)
            return "Error modifying."

    async def _handle_merchant_remove_from_cart(self, context: ConversationContext,
                                       entities: Dict, message_text: str) -> str:
        """v5's remove from cart"""
        product_name = entities.get("product_name", "").strip()
        if not product_name and context.last_product_discussed:
            product_name = context.last_product_discussed.get("product_name", "")
        if not product_name:
            return "Which product? (e.g., 'Remove milk')"
        
        try:
            current_cart = self.cart._dict_to_cart(await self.cart.get_cart(context.conversation_id))
            if not (current_cart and current_cart.items): return "Cart is empty!"

            item_to_remove_id = None
            for item in current_cart.items:
                if product_name.lower() in item.product_name.lower():
                    item_to_remove_id = item.product_id
                    break
            
            if item_to_remove_id:
                await self.cart.remove_item(context.conversation_id, item_to_remove_id)
                context.update_state(ConversationState.IDLE, "Removed item")
                new_cart = self.cart._dict_to_cart(await self.cart.get_cart(context.conversation_id))
                return f"Removed '{product_name}'.\nCart items: {len(new_cart.items)}"
            
            return f"'{product_name}' not in cart."
        except Exception as e:
            logger.error(f"Remove error: {e}", exc_info=True)
            return "Error removing."

    async def _handle_merchant_view_cart(self, context: ConversationContext,
                                entities: Dict, message_text: str) -> str:
        """v5's view cart"""
        try:
            cart_summary = await self.cart.get_cart_summary(context.conversation_id)
            context.update_state(ConversationState.IDLE, "Viewed cart")
            return cart_summary + "\n\n'Confirm order' to checkout"
        except Exception as e:
            logger.error(f"View cart error: {e}", exc_info=True)
            return "Error loading cart."

    async def _handle_merchant_confirm_order(self, context: ConversationContext,
                                    entities: Dict, message_text: str) -> str:
        """v5's confirm order"""
        try:
            cart = self.cart._dict_to_cart(await self.cart.get_cart(context.conversation_id))
            if not (cart and cart.items):
                return "Your cart is empty!"
            
            is_valid, issues = await self.cart.validate_cart_with_inventory(
                context.conversation_id, self.inventory, context.merchant_id
            )
            if not is_valid:
                return f"Cannot order:\n" + "\n".join([f"- {issue}" for issue in issues])
            
            order = await self.orders.create_order_from_cart(
                context.conversation_id, context.merchant_id,
                context.user_ref, cart.to_dict() # Use user_ref as customer_phone
            )
            
            await self.cart.clear_cart(context.conversation_id)
            context.update_state(ConversationState.ORDER_PLACED, "Order placed")
            
            return f"Order Placed!\n\nOrder ID: `{order.get('order_id', 'N/A')}`\n" \
                   f"Total: Rs {order.get('total_amount', 0):.2f}\n" \
                   f"Thank you!"
        except Exception as e:
            logger.error(f"Order error: {e}", exc_info=True)
            return "Error placing order."

    async def _handle_merchant_order_status(self, context: ConversationContext,
                                   entities: Dict, message_text: str) -> str:
        """v5's order status"""
        try:
            orders = await self.orders.get_customer_orders(context.user_ref, limit=3)
            if not orders:
                return "No orders yet. Start shopping!"
            
            response = "Recent Orders:\n\n"
            for order in orders:
                response += f"- {order['order_id']} - {order['status'].upper()}\n"
                response += f"  Rs {order['total_amount']:.2f}\n\n"
            return response
        except Exception as e:
            logger.error(f"Status error: {e}", exc_info=True)
            return "Error retrieving orders."

    async def _handle_merchant_general_help(self, context: ConversationContext,
                                   entities: Dict, message_text: str) -> str:
        """v5's help"""
        return """Need help?
- Search: "Do you have rice?"
- Add: "I want 5kg rice"
- Modify: "Change to 10kg"
- Remove: "Remove milk"
- View: "Show my cart"
- Checkout: "Confirm order"
- Orders: "Show my orders"
"""
    
    async def _handle_merchant_general_query(self, context: ConversationContext,
                                    entities: Dict, message_text: str) -> str:
        """v5's RAG/Knowledge query"""
        try:
            rag_response = await self.knowledge.search_knowledge(
                context.merchant_id, message_text, top_k=1
            )
            return rag_response or "I'm not sure. Type 'help' for options!"
        except Exception as e:
            logger.error(f"General query error: {e}", exc_info=True)
            return "I can only help with shopping. Try 'help' for options."

    async def _handle_merchant_cart_reset(self, context: ConversationContext) -> str:
        """v5's cart reset"""
        try:
            cart = self.cart._dict_to_cart(await self.cart.get_cart(context.conversation_id))
            if not (cart and cart.items):
                return "Your cart is already empty!"
            
            await self.cart.clear_cart(context.conversation_id)
            context.cart_items_snapshot = []
            context.last_product_discussed = None
            context.update_state(ConversationState.IDLE, "Cart reset")
            
            return "Order Cancelled. Your cart is now empty.\nStart shopping fresh!"
        except Exception as e:
            logger.error(f"Cart reset error: {e}", exc_info=True)
            return "Error resetting cart."

# ==========================================
# 9. SINGLETON HELPERS (from v5)
# ==========================================

_PIPELINE_SINGLETON: Optional[UnifiedAIPipelineV6] = None

def set_pipeline(pipeline: UnifiedAIPipelineV6) -> None:
    global _PIPELINE_SINGLETON
    _PIPELINE_SINGLETON = pipeline

def get_pipeline() -> Optional[UnifiedAIPipelineV6]:    
    return _PIPELINE_SINGLETON
