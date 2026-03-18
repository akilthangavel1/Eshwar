from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable

from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection


DB_NAME = "job_tracker"
COLLECTION_NAME = "jobs"
SCHEMA_VERSION = 1


STATUSES = (
    "NEW",
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELED",
)

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "NEW": {"QUEUED", "CANCELED"},
    "QUEUED": {"RUNNING", "CANCELED"},
    "RUNNING": {"SUCCEEDED", "FAILED", "CANCELED"},
    "SUCCEEDED": set(),
    "FAILED": set(),
    "CANCELED": set(),
}


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    db_name: str = DB_NAME
    collection_name: str = COLLECTION_NAME


def get_config() -> MongoConfig:
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    return MongoConfig(uri=uri)


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def get_collection(config: MongoConfig | None = None) -> Collection:
    cfg = config or get_config()
    client = MongoClient(cfg.uri)
    db = client[cfg.db_name]
    collection = db[cfg.collection_name]
    ensure_indexes(collection)
    return collection


def ensure_indexes(collection: Collection) -> None:
    collection.create_index("status")
    collection.create_index("owner")
    collection.create_index([("created_at", -1)])


def normalize_status(value: str) -> str:
    status = value.strip().upper()
    if status not in STATUSES:
        raise ValueError(f"Invalid status: {value}")
    return status


def can_transition(from_status: str, to_status: str) -> bool:
    return to_status in ALLOWED_TRANSITIONS.get(from_status, set())


def parse_priority(value: str) -> int:
    raw = value.strip()
    if not raw:
        return 0
    if raw.startswith("-") and raw[1:].isdigit():
        return int(raw)
    if raw.isdigit():
        return int(raw)
    raise ValueError("Priority must be an integer")


def parse_tags(value: str) -> list[str]:
    raw = value.strip()
    if not raw:
        return []
    tags = [t.strip() for t in raw.split(",")]
    return [t for t in tags if t]


def create_job_doc(*, title: str, owner: str, priority: int, tags: list[str]) -> dict[str, Any]:
    title = title.strip()
    owner = owner.strip()
    if not title:
        raise ValueError("Title cannot be empty")
    if not owner:
        raise ValueError("Owner cannot be empty")

    now = utcnow()
    return {
        "schema_version": SCHEMA_VERSION,
        "title": title,
        "owner": owner,
        "priority": priority,
        "tags": tags,
        "status": "NEW",
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "finished_at": None,
        "error_message": None,
        "status_history": [{"status": "NEW", "at": now}],
    }


def object_id_from_input(value: str) -> ObjectId:
    raw = value.strip()
    if not ObjectId.is_valid(raw):
        raise ValueError("Invalid _id")
    return ObjectId(raw)


def update_job_fields(
    collection: Collection,
    job_id: ObjectId,
    *,
    title: str | None = None,
    owner: str | None = None,
    priority: int | None = None,
    tags: list[str] | None = None,
) -> tuple[int, int]:
    update_fields: dict[str, Any] = {}

    if title is not None:
        title = title.strip()
        if not title:
            raise ValueError("Title cannot be empty")
        update_fields["title"] = title

    if owner is not None:
        owner = owner.strip()
        if not owner:
            raise ValueError("Owner cannot be empty")
        update_fields["owner"] = owner

    if priority is not None:
        update_fields["priority"] = priority

    if tags is not None:
        update_fields["tags"] = tags

    if not update_fields:
        return (0, 0)

    update_fields["updated_at"] = utcnow()
    result = collection.update_one({"_id": job_id}, {"$set": update_fields})
    return (result.matched_count, result.modified_count)


def transition_job_status(
    collection: Collection,
    job_id: ObjectId,
    to_status: str,
    *,
    error_message: str | None = None,
) -> tuple[int, int]:
    to_status_norm = normalize_status(to_status)
    existing = collection.find_one({"_id": job_id})
    if existing is None:
        return (0, 0)

    from_status = existing.get("status", "NEW")
    if from_status == to_status_norm:
        return (1, 0)

    if not can_transition(from_status, to_status_norm):
        raise ValueError(f"Invalid transition: {from_status} -> {to_status_norm}")

    now = utcnow()
    set_fields: dict[str, Any] = {"status": to_status_norm, "updated_at": now}
    if to_status_norm == "RUNNING" and existing.get("started_at") is None:
        set_fields["started_at"] = now
    if to_status_norm in {"SUCCEEDED", "FAILED", "CANCELED"} and existing.get("finished_at") is None:
        set_fields["finished_at"] = now
    if to_status_norm == "FAILED":
        if error_message is not None:
            set_fields["error_message"] = error_message.strip() or None
    else:
        set_fields["error_message"] = None

    result = collection.update_one(
        {"_id": job_id, "status": from_status},
        {"$set": set_fields, "$push": {"status_history": {"status": to_status_norm, "at": now}}},
    )
    return (result.matched_count, result.modified_count)


def format_job(doc: dict[str, Any]) -> str:
    job_id = str(doc.get("_id", ""))
    title = doc.get("title", "")
    owner = doc.get("owner", "")
    priority = doc.get("priority", 0)
    tags = doc.get("tags", [])
    status = doc.get("status", "")
    created_at = doc.get("created_at")
    updated_at = doc.get("updated_at")
    started_at = doc.get("started_at")
    finished_at = doc.get("finished_at")
    return (
        f"_id={job_id} | title={title} | owner={owner} | priority={priority} | tags={tags} | "
        f"status={status} | created_at={created_at} | updated_at={updated_at} | "
        f"started_at={started_at} | finished_at={finished_at}"
    )


def print_table(rows: Iterable[Iterable[str]]) -> None:
    rows_list = [list(r) for r in rows]
    if not rows_list:
        return
    widths = [max(len(str(cell)) for cell in col) for col in zip(*rows_list)]
    for row in rows_list:
        line = " | ".join(str(cell).ljust(width) for cell, width in zip(row, widths))
        print(line)
