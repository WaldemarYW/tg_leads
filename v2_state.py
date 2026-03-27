from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import asdict, fields
from typing import Dict, Iterable, Set

from flow_engine import PeerRuntimeState, canonical_checkpoint_name, canonical_step_name


class V2EnrollmentStore:
    def __init__(self, path: str):
        self.path = path
        self.lock_path = f"{path}.lock" if path else ""
        self.data = self._load()

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

    def _load(self) -> Set[int]:
        if not self.path or not os.path.exists(self.path):
            return set()
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError):
            return set()
        if not isinstance(raw, list):
            return set()
        out: Set[int] = set()
        for item in raw:
            try:
                out.add(int(item))
            except (TypeError, ValueError):
                continue
        return out

    def _save(self):
        base = os.path.dirname(self.path)
        if base:
            os.makedirs(base, exist_ok=True)
        if not self._acquire_lock():
            return
        try:
            tmp_path = f"{self.path}.tmp.{os.getpid()}.{int(time.time() * 1000)}.{uuid.uuid4().hex}"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(sorted(self.data), f, ensure_ascii=True)
            os.replace(tmp_path, self.path)
        finally:
            self._release_lock()

    def has(self, peer_id: int) -> bool:
        return int(peer_id) in self.data

    def add(self, peer_id: int):
        self.data.add(int(peer_id))
        self._save()

    def update_many(self, peer_ids: Iterable[int]):
        changed = False
        for peer_id in peer_ids:
            val = int(peer_id)
            if val not in self.data:
                self.data.add(val)
                changed = True
        if changed:
            self._save()


class V2RuntimeStore:
    def __init__(self, path: str):
        self.path = path
        self.lock_path = f"{path}.lock" if path else ""
        self.data: Dict[str, dict] = self._load()
        self._state_field_names = {f.name for f in fields(PeerRuntimeState)}

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

    def _load(self) -> Dict[str, dict]:
        if not self.path or not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
        return raw if isinstance(raw, dict) else {}

    def _save(self):
        base = os.path.dirname(self.path)
        if base:
            os.makedirs(base, exist_ok=True)
        if not self._acquire_lock():
            return
        try:
            tmp_path = f"{self.path}.tmp.{os.getpid()}.{int(time.time() * 1000)}.{uuid.uuid4().hex}"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=True)
            os.replace(tmp_path, self.path)
        finally:
            self._release_lock()

    def get(self, peer_id: int) -> PeerRuntimeState:
        key = str(int(peer_id))
        raw = self.data.get(key, {})
        if not isinstance(raw, dict):
            raw = {}
        sanitized = {k: v for k, v in raw.items() if k in self._state_field_names}
        migrated = dict(sanitized)
        for field_name in ("flow_step", "qa_gate_step", "step_wait_step", "resume_step_after_balance"):
            if field_name in migrated:
                migrated[field_name] = canonical_step_name(str(migrated.get(field_name) or ""))
        if "resume_checkpoint_after_balance" in migrated:
            migrated["resume_checkpoint_after_balance"] = canonical_checkpoint_name(
                str(migrated.get("resume_checkpoint_after_balance") or "")
            )
        merged = {"peer_id": int(peer_id), **migrated}
        if len(sanitized) != len(raw) or migrated != sanitized:
            self.data[key] = migrated
            self._save()
        return PeerRuntimeState(**merged)

    def set(self, state: PeerRuntimeState):
        key = str(int(state.peer_id))
        self.data[key] = asdict(state)
        self._save()

    def delete(self, peer_id: int):
        key = str(int(peer_id))
        if key in self.data:
            del self.data[key]
            self._save()
