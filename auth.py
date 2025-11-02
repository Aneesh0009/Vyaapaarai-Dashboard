# auth_v6.py
# Unified Role-Aware Authentication (Admin + Merchant)
# Merges security from merchant_v5 (bcrypt, JWT)
# with role logic from admin_merchant_v4 (admin/merchant dependencies)
# and adds Redis-based blacklist and generic role requirements.

import os, logging, bcrypt, jwt, asyncio
# Use redis.asyncio for asynchronous operations
try:
    import redis.asyncio as redis
except ImportError:
    print("redis package not found. Please install with: pip install redis")
    # Fallback or error, but for this spec, we assume it's available.
    # As a simple fallback for testing without redis, we could mock it,
    # but for production, it's a hard requirement.
    pass 

from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# =====================================
# Environment & Config
# =====================================
load_dotenv()
# Set up a specific logger for this module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("auth_v6")

JWT_SECRET = os.getenv("JWT_SECRET", "default-insecure-secret-key-replace-me")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "12"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Instantiate the HTTPBearer security scheme
security = HTTPBearer()

# =====================================
# Redis Blacklist (Async)
# =====================================
class TokenBlacklist:
    """
    Asynchronous Token Blacklist service using Redis.
    
    This class manages a connection pool and provides methods
    to blacklist tokens and check if they are blacklisted.
    """
    def __init__(self, url=REDIS_URL):
        self.url = url
        self.client = None
        logger.info(f"TokenBlacklist initialized for Redis at {url}")

    async def connect(self):
        """
        Establishes the asynchronous connection to Redis.
        Should be called during application startup.
        """
        try:
            self.client = await redis.from_url(self.url, decode_responses=True)
            await self.client.ping()
            logger.info("Successfully connected to Redis for token blacklist.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis at {self.url}: {e}")
            # In a real app, you might want to retry or exit if Redis is critical.
            self.client = None # Ensure client is None if connection fails

    async def blacklist(self, token: str, exp_timestamp: float):
        """
        Adds a token to the blacklist with an expiry time.
        
        The expiry is set to match the token's 'exp' claim to prevent
        Redis from filling up with indefinitely expired tokens.
        
        Args:
            token: The JWT token string to blacklist.
            exp_timestamp: The 'exp' timestamp (in seconds) from the token.
        """
        if not self.client:
            logger.warning("Redis client not connected. Cannot blacklist token.")
            return

        try:
            # Calculate remaining time to live (TTL) in seconds
            now_ts = datetime.now(timezone.utc).timestamp()
            ttl = int(exp_timestamp - now_ts)
            
            if ttl > 0:
                # Use setex to set the key with an automatic expiration
                await self.client.setex(f"bl:{token}", ttl, "1")
                logger.info(f"Token blacklisted with {ttl}s TTL.")
            else:
                logger.warning("Attempted to blacklist already-expired token.")
        except Exception as e:
            logger.error(f"Failed to blacklist token in Redis: {e}")

    async def is_blacklisted(self, token: str) -> bool:
        """
        Checks if a token exists in the Redis blacklist.
        
        Args:
            token: The JWT token string to check.
            
        Returns:
            True if the token is blacklisted, False otherwise.
        """
        if not self.client:
            logger.warning("Redis client not connected. Assuming token is not blacklisted.")
            # Fail-safe: if Redis is down, we might fail open (allow tokens).
            # Depending on security posture, you might want to fail closed (deny).
            return False
            
        try:
            # check if the key exists
            return bool(await self.client.exists(f"bl:{token}"))
        except Exception as e:
            logger.error(f"Failed to check blacklist in Redis: {e}")
            # Fail-safe:
            return False

# =====================================
# Token Utilities
# =====================================
async def create_token(payload: Dict[str, Any], expiry_hours: int = JWT_EXPIRY_HOURS) -> str:
    """
    Creates a new JWT token.
    
    Args:
        payload: The data to include in the token (e.g., username, role).
        expiry_hours: How many hours the token should be valid for.
        
    Returns:
        A JWT token string.
        
    Raises:
        HTTPException (500) if token generation fails.
    """
    now = datetime.now(timezone.utc)
    exp = now + timedelta(hours=expiry_hours)
    
    # Update payload with standard JWT claims
    # We use timestamps (seconds since epoch) for iat/exp
    payload.update({
        "iat": now.timestamp(), 
        "exp": exp.timestamp(), 
        "iss": "vyaapaarai_v6" # Issuer
    })
    
    try:
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
        logger.info(f"Created JWT for user={payload.get('username')} role={payload.get('role')}")
        return token
    except Exception as e:
        logger.error(f"JWT creation failed: {e}")
        raise HTTPException(status_code=500, detail="Token generation failed")

async def decode_token(token: str, blacklist: Optional[TokenBlacklist] = None) -> Dict[str, Any]:
    """
    Decodes and validates a JWT token.
    
    Checks for:
    - Signature validity
    - Expiry
    - Blacklist (if a blacklist service is provided)
    
    Args:
        token: The JWT token string.
        blacklist: An optional instance of TokenBlacklist.
        
    Returns:
        The decoded token payload as a dictionary.
        
    Raises:
        HTTPException (401) if the token is invalid, expired, or blacklisted.
    """
    try:
        # 1. Decode the token (verifies signature)
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        
        # 2. Check blacklist
        if blacklist and await blacklist.is_blacklisted(token):
            logger.warning(f"Blacklisted token used by: {payload.get('username')}")
            raise jwt.InvalidTokenError("Token has been blacklisted (logged out)")
            
        # 3. Check expiry (pyjwt.decode *should* do this, but we double-check)
        exp_ts = payload.get("exp")
        if not exp_ts or datetime.now(timezone.utc).timestamp() > exp_ts:
            logger.warning(f"Expired token used by: {payload.get('username')}")
            raise jwt.ExpiredSignatureError("Token has expired")
            
        return payload
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail=f"Invalid token: {e}",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except Exception as e:
        logger.error(f"Unexpected token decode error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )

# =====================================
# Role-Based Authorization
# =====================================

# Note: In a real app, you'd instantiate the blacklist
# and pass it to decode_token. For this module, we
# assume it will be injected or handled at a higher level.
# For demonstration, verify_token won't use the blacklist
# unless it's passed, which FastAPI can't do by default.
# A common pattern is to make blacklist a global or part of a class.

# Simplified verify_token for module structure.
# In a real FastAPI app, you'd integrate the blacklist instance.
# Example:
# app.state.blacklist = TokenBlacklist()
# app.state.blacklist.connect()
#
# async def verify_token_with_blacklist(
#     credentials: HTTPAuthorizationCredentials = Depends(security),
#     request: Request
# ):
#     blacklist = request.app.state.blacklist
#     token = credentials.credentials
#     payload = await decode_token(token, blacklist)
#     return payload

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """
    FastAPI dependency to get and decode a token from the Authorization header.
    
    NOTE: This basic version does not check the blacklist.
    See commented-out example above for blacklist integration in a full app.
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing token")
    token = credentials.credentials
    # Pass no blacklist by default
    payload = await decode_token(token, blacklist=None) 
    return payload

async def get_current_user(payload: Dict[str, Any] = Depends(verify_token)) -> Dict[str, Any]:
    """
    FastAPI dependency to get the current user's payload from a verified token.
    Ensures a 'username' is present in the token.
    """
    username = payload.get("username")
    if not username:
        raise HTTPException(status_code=401, detail="Invalid token payload: missing username")
    return payload

def require_role(required_roles: List[str]):
    """
    Factory function for a FastAPI dependency that requires specific roles.
    
    Args:
        required_roles: A list of role strings (e.g., ["admin", "support"]).
        
    Returns:
        An async dependency function.
    """
    async def role_dependency(payload: Dict[str, Any] = Depends(verify_token)) -> Dict[str, Any]:
        user_role = payload.get("role")
        if user_role not in required_roles:
            logger.warning(
                f"Auth failed: User '{payload.get('username')}' (Role: {user_role}) "
                f"tried to access resource requiring one of: {required_roles}"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail=f"Insufficient privileges: requires one of {required_roles}"
            )
        # If successful, return the payload for further use
        return payload
    return role_dependency

# =====================================
# Password Management (from v5)
# =====================================
def hash_password(password: str) -> str:
    """
    Hashes a plain-text password using bcrypt.
    
    Args:
        password: The plain-text password.
        
    Returns:
        A securely hashed password string.
        
    Raises:
        ValueError if hashing fails.
    """
    try:
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')
    except Exception as e:
        logger.error(f"Password hash failed: {e}")
        raise ValueError("Password hashing failed") from e

def verify_password(password: str, hashed: str) -> bool:
    """
    Verifies a plain-text password against a bcrypt hash.
    
    Args:
        password: The plain-text password to check.
        hashed: The stored hashed password.
        
    Returns:
        True if the password matches the hash, False otherwise.
    """
    if not password or not hashed:
        return False
    try:
        return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
    except Exception as e:
        # Log attempts with invalid hash formats, etc.
        logger.warning(f"Password verification error: {e}")
        return False

# =====================================
# Admin/Merchant Utilities (from v4)
# =====================================

# These are now convenience wrappers around the new `require_role`
# This promotes consistency and DRY (Don't Repeat Yourself)

async def require_admin(payload: Dict[str, Any] = Depends(require_role(["admin"]))):
    """
    FastAPI dependency to require 'admin' role.
    """
    return payload

async def require_merchant(payload: Dict[str, Any] = Depends(require_role(["merchant"]))):
    """
    FastAPI dependency to require 'merchant' role.
    """
    return payload

# You could also add others easily:
# async def require_support(payload: Dict[str, Any] = Depends(require_role(["support", "admin"]))):
#     """Requires 'support' or 'admin' role."""
#     return payload

# =====================================
# Audit Logging
# =====================================

async def record_login_event(username: str, role: str, success: bool, db=None):
    """
    Records a login event to the logger and optionally to a database.
    
    Args:
        username: The username attempting to log in.
        role: The role associated with the user (if known).
        success: Boolean indicating if the login was successful.
        db: An optional database connection (e.g., motor client)
            with an `insert_one` method.
    """
    event = {
        "username": username,
        "role": role,
        "timestamp": datetime.now(timezone.utc),
        "status": "success" if success else "failure"
    }
    
    # Log to standard logger
    if success:
        logger.info(f"Successful login: {event}")
    else:
        logger.warning(f"Failed login attempt: {event}")
    
    # Log to database if provided
    if db is not None:
        try:
            # Assumes a collection named 'login_audit'
            await db.login_audit.insert_one(event)
        except Exception as e:
            logger.error(f"Failed to write login audit to DB: {e}")

logger.info("auth_v6.py loaded successfully. Redis connection must be established by the main app.")
