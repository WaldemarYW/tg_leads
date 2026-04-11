import json
import os
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from tg_to_sheets import acquire_lock, release_lock, normalize_username


def normalize_target_group(value: Optional[str]) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("@"):
        return raw
    return raw.rstrip("/")


def display_username(username: Optional[str], username_norm: str) -> str:
    raw = str(username or "").strip()
    if not raw:
        return f"@{username_norm}"
    raw = raw.lstrip("@")
    return f"@{raw}"


class HrFilterStore:
    def __init__(self, path: str, cache_ttl_sec: float = 5.0):
        self.path = path
        self.lock_path = f"{path}.lock" if path else ""
        self.cache_ttl_sec = max(0.0, float(cache_ttl_sec or 0.0))
        self._cache = None
        self._cache_ts = 0.0

    def _load_data(self) -> dict:
        if not self.path or not os.path.exists(self.path):
            return {"rules": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {"rules": {}}
        if not isinstance(data, dict):
            return {"rules": {}}
        rules = data.get("rules")
        if not isinstance(rules, dict):
            data["rules"] = {}
        return data

    def _save_data(self, data: dict):
        if not self.path:
            return
        base = os.path.dirname(self.path)
        if base:
            os.makedirs(base, exist_ok=True)
        tmp_path = f"{self.path}.tmp.{os.getpid()}.{int(time.time() * 1000)}.{uuid.uuid4().hex}"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True)
        os.replace(tmp_path, self.path)
        self._cache = None
        self._cache_ts = 0.0

    def _get_rules_map(self, force: bool = False) -> Dict[str, dict]:
        now = time.time()
        if not force and self._cache is not None and (now - self._cache_ts) <= self.cache_ttl_sec:
            return self._cache
        rules = self._load_data().get("rules", {})
        if not isinstance(rules, dict):
            rules = {}
        self._cache = rules
        self._cache_ts = now
        return rules

    def list_rules(self, force: bool = False) -> List[dict]:
        rules = self._get_rules_map(force=force)
        items = []
        for username_norm, rule in rules.items():
            if not isinstance(rule, dict):
                continue
            target_group = normalize_target_group(rule.get("target_group_link"))
            if not username_norm or not target_group:
                continue
            items.append(
                {
                    "username_norm": username_norm,
                    "username_raw": display_username(rule.get("username_raw"), username_norm),
                    "target_group_link": target_group,
                    "updated_at": str(rule.get("updated_at") or "").strip(),
                }
            )
        items.sort(key=lambda item: item["username_norm"])
        return items

    def match_rule(self, username: Optional[str]) -> Optional[dict]:
        username_norm = normalize_username(username)
        if not username_norm:
            return None
        rule = self._get_rules_map().get(username_norm)
        if not isinstance(rule, dict):
            return None
        target_group = normalize_target_group(rule.get("target_group_link"))
        if not target_group:
            return None
        return {
            "username_norm": username_norm,
            "username_raw": display_username(rule.get("username_raw"), username_norm),
            "target_group_link": target_group,
            "updated_at": str(rule.get("updated_at") or "").strip(),
        }

    def upsert_rule(self, username: str, target_group_link: str) -> dict:
        username_norm = normalize_username(username)
        if not username_norm:
            raise ValueError("username_required")
        target_group = normalize_target_group(target_group_link)
        if not target_group:
            raise ValueError("target_group_required")
        if not acquire_lock(self.lock_path, ttl_sec=5):
            raise RuntimeError("hr_filter_lock_unavailable")
        try:
            data = self._load_data()
            rules = data.setdefault("rules", {})
            now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            username_display = display_username(username, username_norm)
            rules[username_norm] = {
                "username_raw": username_display,
                "target_group_link": target_group,
                "updated_at": now,
            }
            self._save_data(data)
            return {
                "username_norm": username_norm,
                "username_raw": username_display,
                "target_group_link": target_group,
                "updated_at": now,
            }
        finally:
            release_lock(self.lock_path)

    def delete_rule(self, username: str) -> bool:
        username_norm = normalize_username(username)
        if not username_norm:
            return False
        if not acquire_lock(self.lock_path, ttl_sec=5):
            return False
        try:
            data = self._load_data()
            rules = data.setdefault("rules", {})
            if username_norm not in rules:
                return False
            rules.pop(username_norm, None)
            self._save_data(data)
            return True
        finally:
            release_lock(self.lock_path)


class HrForwardDeduper:
    def __init__(self, path: str, retention_sec: float = 30 * 24 * 3600):
        self.path = path
        self.lock_path = f"{path}.lock" if path else ""
        self.retention_sec = max(3600.0, float(retention_sec or 0.0))

    def _load_data(self) -> dict:
        if not self.path or not os.path.exists(self.path):
            return {"claims": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {"claims": {}}
        if not isinstance(data, dict):
            return {"claims": {}}
        claims = data.get("claims")
        if not isinstance(claims, dict):
            data["claims"] = {}
        return data

    def _save_data(self, data: dict):
        if not self.path:
            return
        base = os.path.dirname(self.path)
        if base:
            os.makedirs(base, exist_ok=True)
        tmp_path = f"{self.path}.tmp.{os.getpid()}.{int(time.time() * 1000)}.{uuid.uuid4().hex}"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True)
        os.replace(tmp_path, self.path)

    def _prune_claims(self, claims: dict, now_ts: float):
        cutoff = now_ts - self.retention_sec
        expired = []
        for key, item in claims.items():
            if not isinstance(item, dict):
                expired.append(key)
                continue
            claimed_at = float(item.get("claimed_at_ts") or 0.0)
            if claimed_at and claimed_at >= cutoff:
                continue
            expired.append(key)
        for key in expired:
            claims.pop(key, None)

    def claim(self, source_chat_id: object, message_id: object, owner: str = "") -> bool:
        if source_chat_id in (None, "") or message_id in (None, ""):
            return False
        if not acquire_lock(self.lock_path, ttl_sec=5):
            return False
        try:
            data = self._load_data()
            claims = data.setdefault("claims", {})
            now_ts = time.time()
            self._prune_claims(claims, now_ts)
            key = f"{source_chat_id}:{message_id}"
            if key in claims:
                return False
            claims[key] = {
                "claimed_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "claimed_at_ts": now_ts,
                "owner": str(owner or "").strip(),
            }
            self._save_data(data)
            return True
        finally:
            release_lock(self.lock_path)
