from __future__ import annotations

from typing import Any

from mongo_job_common import format_job, get_collection, normalize_status, object_id_from_input, print_table


def list_jobs(collection, *, owner: str | None, status: str | None, limit: int) -> None:
    query: dict[str, Any] = {}
    if owner:
        query["owner"] = owner
    if status:
        query["status"] = normalize_status(status)

    cursor = collection.find(query).sort("created_at", -1).limit(limit)
    docs = list(cursor)
    if not docs:
        print("No data found")
        return

    for doc in docs:
        print(format_job(doc))


def get_job(collection) -> None:
    job_id = object_id_from_input(input("Enter job _id: "))
    doc = collection.find_one({"_id": job_id})
    if doc is None:
        print("Job not found")
        return
    print(format_job(doc))
    history = doc.get("status_history", [])
    if history:
        print("Status history:")
        for item in history:
            print(f"- {item.get('status')} at {item.get('at')}")


def analytics(collection) -> None:
    counts = list(
        collection.aggregate(
            [
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]
        )
    )
    if not counts:
        print("No data found")
        return

    print("Jobs by status:")
    print_table([("status", "count")] + [(c["_id"], str(c["count"])) for c in counts])

    durations = list(
        collection.aggregate(
            [
                {"$match": {"started_at": {"$ne": None}, "finished_at": {"$ne": None}}},
                {"$project": {"status": 1, "duration_ms": {"$subtract": ["$finished_at", "$started_at"]}}},
                {"$group": {"_id": "$status", "avg_ms": {"$avg": "$duration_ms"}, "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]
        )
    )
    if durations:
        print("\nAverage duration (ms) by status:")
        print_table(
            [("status", "count", "avg_ms")]
            + [(d["_id"], str(d["count"]), str(int(d["avg_ms"]))) for d in durations]
        )

    owners = list(
        collection.aggregate(
            [
                {"$group": {"_id": "$owner", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ]
        )
    )
    if owners:
        print("\nTop owners:")
        print_table([("owner", "count")] + [(o["_id"], str(o["count"])) for o in owners])


def main() -> None:
    collection = get_collection()

    mode = input("Mode (list/get/analytics) [list]: ").strip().lower() or "list"
    if mode == "get":
        get_job(collection)
        return
    if mode == "analytics":
        analytics(collection)
        return

    owner = input("Filter owner (blank for all): ").strip() or None
    status = input("Filter status (blank for all): ").strip() or None
    limit_raw = input("Limit [20]: ").strip()
    limit = int(limit_raw) if limit_raw.isdigit() else 20
    list_jobs(collection, owner=owner, status=status, limit=limit)


if __name__ == "__main__":
    main()
