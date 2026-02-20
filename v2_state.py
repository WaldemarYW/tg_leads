from __future__ import annotations

import json
import os
from dataclasses import asdict
from typing import Dict, Iterable, Set

from flow_engine import PeerRuntimeState


class V2EnrollmentStore:
    def __init__(self, path: str):
        self.path = path
        self.data = self._load()

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
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(sorted(self.data), f, ensure_ascii=True)

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
        self.data: Dict[str, dict] = self._load()

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
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=True)

    def get(self, peer_id: int) -> PeerRuntimeState:
        key = str(int(peer_id))
        raw = self.data.get(key, {})
        if not isinstance(raw, dict):
            raw = {}
        merged = {"peer_id": int(peer_id), **raw}
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
