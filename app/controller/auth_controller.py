"""Authentication / user-management logic.

Ports the behaviour of the standalone authentication service
(authentication/src/controller/*.js) into the dataset-store so that only a
backend + ml + frontend remain.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import bcrypt
import jwt as pyjwt
from bson.objectid import ObjectId

import internal.config as config
from db.users import UserDBManager, DuplicateUserError

users_dbm = UserDBManager()


# --------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------- #
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=10)).decode()


def check_password(password: str, hashed: Optional[str]) -> bool:
    if not hashed:
        return False
    try:
        return bcrypt.checkpw(password.encode(), hashed.encode())
    except ValueError:
        return False


def create_access_token(user: Dict[str, Any], **extra) -> str:
    """JWT access token carrying the same claims as the old Node service."""
    payload = {
        "id": str(user["_id"]),
        "provider": user.get("provider") or "local",
        "email": user.get("email"),
        "userName": user.get("userName"),
        "subscriptionLevel": user.get("subscriptionLevel", "standard"),
        **extra,
        "exp": datetime.now(timezone.utc) + timedelta(seconds=config.SERVER_TTL_SECONDS),
    }
    return pyjwt.encode(payload, config.SECRET_KEY, algorithm="HS256")


def create_refresh_token(user_id) -> str:
    expires = datetime.now(timezone.utc) + timedelta(
        seconds=config.SERVER_REFRESH_TTL_SECONDS
    )
    return pyjwt.encode(
        {"id": str(user_id), "exp": expires},
        config.SERVER_REFRESH_SECRET,
        algorithm="HS256",
    )


def decode_token(token: str, secret: str = None) -> Dict[str, Any]:
    """Verify and decode a JWT. Raises jwt.PyJWTError on failure."""
    return pyjwt.decode(token, secret or config.SECRET_KEY, algorithms=["HS256"])


def _public_user(user: Dict[str, Any]) -> Dict[str, Any]:
    """User document without secrets, safe to send to the frontend."""
    safe = {k: v for k, v in user.items() if k not in ("password", "refreshToken")}
    if "_id" in safe:
        safe["_id"] = str(safe["_id"])
    for field in ("createdAt",):
        if isinstance(safe.get(field), datetime):
            safe[field] = safe[field].isoformat()
    return safe


# --------------------------------------------------------------------- #
# Registration & login
# --------------------------------------------------------------------- #
class AuthError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def register_user(data: Dict[str, Any]) -> None:
    email = data.get("email")
    user_name = data.get("userName")
    password = data.get("password")

    if not email:
        raise AuthError(400, "please enter your email address")
    if not users_dbm.is_valid_email(email):
        raise AuthError(400, "email address not valid")
    if not user_name or not str(user_name).strip():
        raise AuthError(400, "please enter a username")
    if not password:
        raise AuthError(400, "please enter a password")
    if len(password) < 8:
        raise AuthError(400, "password needs at least 8 characters")

    try:
        users_dbm.create_user({
            "email": email,
            "userName": user_name,
            "password": hash_password(password),
            "refreshToken": None,
        })
    except DuplicateUserError as error:
        details = getattr(error, "details", None)
        if not isinstance(details, dict):
            args = getattr(error, "args", ())
            details = args[0] if args and isinstance(args[0], dict) else {"errmsg": str(error)}
        message = "This account already exists."
        key_field = next(iter(details.get("keyPattern", {}) or {}), "")
        detail_text = str(details.get("errmsg", ""))
        if key_field == "email" or "email" in detail_text:
            message = "This email address is already registered."
        elif key_field == "userName" or "username" in detail_text.lower():
            message = "This username is already taken."
        raise AuthError(409, message) from error

    # give the new account a refresh token like the Node service did
    created = users_dbm.get_by_email(email)
    users_dbm.update_user(created["_id"], {"refreshToken": create_refresh_token(created["_id"])})


def login(identifier: str, password: str) -> Dict[str, Any]:
    """Validate credentials and return fresh tokens."""
    if not identifier:
        raise AuthError(404, "User not found")

    if users_dbm.is_valid_email(identifier.lower()):
        user = users_dbm.get_by_email(identifier)
    else:
        user = users_dbm.get_by_user_name(identifier)

    if not user:
        raise AuthError(404, "User not found")
    if not check_password(password, user.get("password")):
        raise AuthError(404, "Incorrect password")

    refresh_token = _rotate_refresh_if_needed(user)
    return {
        "access_token": create_access_token(user),
        "refresh_token": refresh_token,
    }


def _rotate_refresh_if_needed(user: Dict[str, Any]) -> str:
    """Re-issue the refresh token when it is about to expire (< 5 minutes)."""
    stored = user.get("refreshToken")
    should_rotate = True
    if stored:
        try:
            decoded = pyjwt.decode(stored, config.SERVER_REFRESH_SECRET, algorithms=["HS256"])
            exp = decoded.get("exp")
            if exp and exp * 1000 - int(datetime.now(timezone.utc).timestamp() * 1000) > 5 * 60 * 1000:
                should_rotate = False
        except pyjwt.PyJWTError:
            should_rotate = True

    if not should_rotate:
        return stored

    new_refresh = create_refresh_token(user["_id"])
    users_dbm.update_user(user["_id"], {"refreshToken": new_refresh})
    return new_refresh


def get_current_user(authorization: Optional[str]) -> Optional[Dict[str, Any]]:
    """Resolve the user behind an access token (None => unauthorized).

    Accepts both "Bearer <token>" and a bare token for robustness.
    """
    if not authorization or not authorization.strip():
        return None
    try:
        decoded = decode_token(authorization.replace("Bearer ", "").strip())
    except pyjwt.PyJWTError:
        return None
    return users_dbm.get_by_id(decoded.get("id"))


# --------------------------------------------------------------------- #
# User management
# --------------------------------------------------------------------- #
def get_user(user: Dict[str, Any]) -> Dict[str, Any]:
    return _public_user(user)


def delete_user(user: Dict[str, Any], email: Optional[str]) -> str:
    if not email:
        raise AuthError(
            400,
            "This route deletes a user. To delete your user account, "
            "please provide your email address in the request body. "
            "Be careful, this action cannot be undone",
        )
    if email.lower() != user["email"]:
        raise AuthError(400, "Provided e-mail does not match user e-mail.")
    users_dbm.delete_user_by_id(user["_id"])
    return f"Deleted user with e-mail: {user['email']}"


def change_mail(user: Dict[str, Any], email: Optional[str]) -> str:
    if not email or not users_dbm.is_valid_email(email):
        raise AuthError(400, f"{email} is not a valid e-mail address")
    normalized = email.strip().lower()
    if users_dbm.is_taken("email", normalized, exclude_id=user["_id"]):
        raise AuthError(400, "E-mail already exists")
    try:
        users_dbm.update_user(user["_id"], {"email": normalized})
    except DuplicateUserError:
        raise AuthError(400, "E-mail already exists")
    return f"Changed e-mail address from {user['email']} to {normalized}"


def change_user_name(user: Dict[str, Any], user_name: Optional[str]) -> str:
    if not user_name or not str(user_name).strip():
        raise AuthError(400, "Please provide a username")
    normalized = str(user_name).strip()
    if users_dbm.is_taken("userName", normalized, exclude_id=user["_id"]):
        raise AuthError(400, "Username already exists")
    try:
        users_dbm.update_user(user["_id"], {"userName": normalized})
    except DuplicateUserError:
        raise AuthError(400, "Username already exists")
    return f"Changed username from {user['userName']} to {normalized}"


def change_password(user: Dict[str, Any], password: str, new_password: str) -> str:
    if not password or not new_password:
        raise AuthError(400, "Provide the current password and the new password")
    if not check_password(password, user.get("password")):
        raise AuthError(400, "Passwords do not match")
    users_dbm.update_user(user["_id"], {"password": hash_password(new_password)})
    return "Changed password"


def map_user_names_to_ids(user_names: List[Any]) -> List[Dict[str, Any]]:
    if not isinstance(user_names, list):
        raise AuthError(400, "Provide valid usernames in an array")
    result = users_dbm.ids_for_user_names([str(n) for n in user_names])
    if len(result) != len(user_names):
        raise AuthError(400, "Some users could not be found")
    return [{**r, "_id": str(r["_id"])} for r in result]


def map_ids_to_user_names(user_ids: List[Any]) -> List[Dict[str, Any]]:
    def is_object_id(value: str) -> bool:
        try:
            ObjectId(value)
            return True
        except Exception:
            return False

    if not isinstance(user_ids, list) or not all(is_object_id(uid) for uid in user_ids):
        raise AuthError(400, "Provide valid ids in an array")
    results = []
    for doc in users_dbm.names_for_ids([str(uid) for uid in user_ids]):
        if doc["userName"] is None:
            results.append({"_id": doc["_id"], "error": "User not found"})
        else:
            results.append({"_id": str(doc["_id"]), "userName": doc["userName"]})
    return results


def suggest_user_names(prefix: Optional[str]) -> List[str]:
    return users_dbm.suggest_user_names(prefix or "")
