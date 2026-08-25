"""MongoDB access layer for user accounts.

Replaces the mongoose `User` model of the standalone authentication service.
The documents keep the same shape so existing auth databases stay readable:

    {
        email: str (unique, lowercased),
        provider: str | None,
        providerId: str | None,
        userName: str (unique),
        password: bcrypt hash | None,
        refreshToken: str | None,
        role: 'user' | 'admin',
        subscriptionLevel: 'standard' | 'upgraded' | 'unlimited'
    }
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import re
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from internal.config import MONGO_URI, AUTH_DBNAME

EMAIL_REGEX = re.compile(
    r"^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$"
)

class DuplicateUserError(DuplicateKeyError):
    """Raised when a unique constraint (email / userName) is violated."""


class UserDBManager:
    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.users = self.mongo_client[AUTH_DBNAME]["users"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self.users.create_index("email", unique=True)
        self.users.create_index("userName", unique=True)

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #
    def get_by_id(self, user_id) -> Optional[Dict[str, Any]]:
        return self.users.find_one({"_id": ObjectId(user_id)})

    def get_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        return self.users.find_one({"email": email.lower()})

    def get_by_user_name(self, user_name: str) -> Optional[Dict[str, Any]]:
        return self.users.find_one({"userName": user_name})

    @staticmethod
    def is_valid_email(email: str) -> bool:
        return bool(email) and bool(EMAIL_REGEX.match(email))

    def ids_for_user_names(self, user_names: List[str]) -> List[Dict[str, Any]]:
        docs = self.users.find({"userName": {"$in": list(user_names)}})
        return [{"_id": doc["_id"], "userName": doc["userName"]} for doc in docs]

    def names_for_ids(self, user_ids: List[str]) -> List[Dict[str, Any]]:
        object_ids = [ObjectId(uid) for uid in user_ids]
        docs = {
            str(doc["_id"]): doc["userName"]
            for doc in self.users.find({"_id": {"$in": object_ids}}, {"userName": 1})
        }
        return [
            {"_id": uid, "userName": docs.get(str(uid))}
            for uid in object_ids
        ]

    def suggest_user_names(self, prefix: str, limit: int = 100) -> List[str]:
        escaped = re.escape(prefix or "")
        cursor = (
            self.users.find({"userName": {"$regex": f"^{escaped}"}})
            .limit(limit)
        )
        return [doc["userName"] for doc in cursor]

    # ------------------------------------------------------------------ #
    # Mutations
    # ------------------------------------------------------------------ #
    def _assert_not_taken(self, field: str, value: str) -> None:
        if self.users.find_one({field: value}, {"_id": 1}):
            raise DuplicateUserError({
                "keyPattern": {field: 1},
                "errmsg": f"E11000 duplicate key error: {field} already in use",
            })

    def is_taken(self, field: str, value: str, exclude_id=None) -> bool:
        query = {field: value}
        if exclude_id is not None:
            query["_id"] = {"$ne": ObjectId(exclude_id)}
        return self.users.find_one(query, {"_id": 1}) is not None

    def create_user(self, data: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        document = {
            "email": str(data["email"]).strip().lower(),
            "provider": data.get("provider"),
            "providerId": data.get("providerId"),
            "userName": str(data["userName"]).strip(),
            "password": data.get("password"),
            "refreshToken": data.get("refreshToken"),
            "role": data.get("role", "user"),
            "subscriptionLevel": data.get("subscriptionLevel", "standard"),
            "createdAt": now,
        }
        # explicit checks so behaviour is identical on real mongo and mongomock
        self._assert_not_taken("email", document["email"])
        self._assert_not_taken("userName", document["userName"])
        try:
            result = self.users.insert_one(document)
        except DuplicateKeyError as error:
            raise DuplicateUserError(error.details) from error
        document["_id"] = result.inserted_id
        return document

    def upsert_oauth_user(
        self, provider: str, provider_id: str, email: str, user_name: str
    ) -> Dict[str, Any]:
        """Find-or-create a user coming from an OAuth identity provider."""
        existing = self.users.find_one(
            {"provider": provider, "providerId": provider_id}
        )
        if existing:
            if not email:
                return existing
            try:
                self.users.update_one(
                    {"_id": existing["_id"]}, {"$set": {"email": email.lower()}}
                )
            except DuplicateKeyError:
                pass
            return self.get_by_id(existing["_id"])

        data = {
            "email": email or f"{provider}-{provider_id}@oauth.local",
            "provider": provider,
            "providerId": provider_id,
            "userName": user_name,
        }
        return self.create_user(data)

    def update_user(self, user_id, fields: Dict[str, Any]) -> None:
        self.users.update_one({"_id": ObjectId(user_id)}, {"$set": fields})

    def delete_user_by_email(self, email: str) -> int:
        result = self.users.delete_one({"email": email.lower()})
        return result.deleted_count

    def delete_user_by_id(self, user_id) -> int:
        result = self.users.delete_one({"_id": ObjectId(user_id)})
        return result.deleted_count
