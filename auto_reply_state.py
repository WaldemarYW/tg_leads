import json
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional, Tuple


def normalize_username(username: Optional[str]) -> str:
    return (username or "").strip().lstrip("@").lower()


def within_followup_window(dt: datetime, start_hour: int, end_hour: int) -> bool:
    return start_hour <= dt.hour < end_hour


def adjust_to_followup_window(dt: datetime, start_hour: int, end_hour: int) -> datetime:
    if within_followup_window(dt, start_hour, end_hour):
        return dt
    if dt.hour < start_hour:
        return dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    return (dt + timedelta(days=1)).replace(hour=start_hour, minute=0, second=0, microsecond=0)


class JsonStore:
    def __init__(self, path: str):
        self.path = path
        self.lock_path = f"{path}.lock" if path else ""

    def _acquire_lock(self, timeout_sec: float = 2.0, stale_sec: float = 10.0) -> bool:
        if not self.lock_path:
            return True
        deadline = time.time() + max(0.1, float(timeout_sec or 0))
        payload = f"{os.getpid()}:{time.time():.6f}"
        while time.time() < deadline:
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                try:
                    os.write(fd, payload.encode("utf-8"))
                finally:
                    os.close(fd)
                return True
            except FileExistsError:
                try:
                    age = time.time() - os.path.getmtime(self.lock_path)
                    if age >= max(1.0, float(stale_sec or 0)):
                        os.remove(self.lock_path)
                        continue
                except Exception:
                    pass
                time.sleep(0.02)
            except Exception:
                return False
        return False

    def _release_lock(self):
        if not self.lock_path:
            return
        try:
            os.remove(self.lock_path)
        except FileNotFoundError:
            pass
        except OSError:
            pass

    def load_dict(self) -> dict:
        if not self.path or not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def save_dict(self, data: dict):
        if not self.path:
            return
        base = os.path.dirname(self.path)
        if base:
            os.makedirs(base, exist_ok=True)
        if not self._acquire_lock():
            return
        try:
            tmp_path = f"{self.path}.tmp.{os.getpid()}.{int(time.time() * 1000)}.{uuid.uuid4().hex}"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=True)
            os.replace(tmp_path, self.path)
        except OSError:
            return
        finally:
            self._release_lock()


class FollowupState:
    def __init__(
        self,
        path: str,
        templates: list,
        start_hour: int,
        end_hour: int,
        test_user_id: Optional[str] = None,
    ):
        self.path = path
        self.templates = templates
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.test_user_id = str(test_user_id) if test_user_id else None
        self.store = JsonStore(path)
        self.data = self.store.load_dict()

    def _save(self):
        self.store.save_dict(self.data)

    def get(self, peer_id: int) -> dict:
        return self.data.get(str(peer_id), {})

    def clear(self, peer_id: int):
        key = str(peer_id)
        if key in self.data:
            del self.data[key]
            self._save()

    def schedule_from_now(self, peer_id: int, now: datetime):
        if not self.templates:
            return
        if self.test_user_id and str(peer_id) == self.test_user_id:
            return
        delay_sec, _ = self.templates[0]
        target = adjust_to_followup_window(now + timedelta(seconds=delay_sec), self.start_hour, self.end_hour)
        self.data[str(peer_id)] = {"stage": 0, "next_at": target.timestamp(), "last_sent_at": None}
        self._save()

    def mark_sent_and_advance(self, peer_id: int, now: datetime) -> Tuple[Optional[int], Optional[datetime]]:
        key = str(peer_id)
        state = self.data.get(key)
        if not state:
            return None, None
        stage = int(state.get("stage", 0))
        state["last_sent_at"] = now.timestamp()
        next_stage = stage + 1
        if next_stage >= len(self.templates):
            del self.data[key]
            self._save()
            return None, None
        delay_sec, _ = self.templates[next_stage]
        target = adjust_to_followup_window(now + timedelta(seconds=delay_sec), self.start_hour, self.end_hour)
        state["stage"] = next_stage
        state["next_at"] = target.timestamp()
        self.data[key] = state
        self._save()
        return next_stage, target


class StepState:
    def __init__(self, path: str, step_order: dict):
        self.path = path
        self.step_order = step_order
        self.store = JsonStore(path)
        self.data = self.store.load_dict()

    def _save(self):
        self.store.save_dict(self.data)

    def get(self, peer_id: int) -> Optional[str]:
        return self.data.get(str(peer_id))

    def set(self, peer_id: int, step: str):
        key = str(peer_id)
        existing = self.data.get(key)
        if existing and self.step_order.get(step, -1) < self.step_order.get(existing, -1):
            return
        self.data[key] = step
        self._save()

    def delete(self, peer_id: int):
        key = str(peer_id)
        if key in self.data:
            del self.data[key]
            self._save()


class LocalPauseStore:
    def __init__(self, path: str, now_factory=None):
        self.path = path
        self.store = JsonStore(path)
        self.now_factory = now_factory or datetime.now
        self.data = self.store.load_dict()

    def _save(self):
        self.store.save_dict(self.data)

    def get_status(self, peer_id: int, username: Optional[str]) -> Optional[str]:
        key = str(peer_id)
        status = self.data.get("by_peer_id", {}).get(key)
        if status:
            return status
        uname = normalize_username(username)
        if not uname:
            return None
        return self.data.get("by_username", {}).get(uname)

    def set_status(
        self,
        peer_id: int,
        username: Optional[str],
        name: Optional[str],
        chat_link: Optional[str],
        status: str,
        updated_by: str = "manual",
    ):
        del name, chat_link
        by_peer = self.data.setdefault("by_peer_id", {})
        by_user = self.data.setdefault("by_username", {})
        meta = self.data.setdefault("meta", {})
        by_peer[str(peer_id)] = status
        uname = normalize_username(username)
        if uname:
            by_user[uname] = status
        meta[str(peer_id)] = {"updated_at": self.now_factory().isoformat(timespec="seconds"), "updated_by": updated_by}
        self._save()

    def active_peer_ids(self) -> set:
        by_peer = self.data.get("by_peer_id", {})
        result = set()
        for key, status in by_peer.items():
            if status != "ACTIVE":
                continue
            try:
                result.add(int(key))
            except ValueError:
                continue
        return result
