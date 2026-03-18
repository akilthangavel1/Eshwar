from __future__ import annotations

from mongo_job_common import (
    ALLOWED_TRANSITIONS,
    format_job,
    get_collection,
    object_id_from_input,
    parse_priority,
    parse_tags,
    transition_job_status,
    update_job_fields,
)


def main() -> None:
    collection = get_collection()

    job_id = object_id_from_input(input("Enter job _id to update: "))
    existing = collection.find_one({"_id": job_id})
    if existing is None:
        print("Job not found")
        return

    print(format_job(existing))

    mode = input("Update (fields/status) [fields]: ").strip().lower() or "fields"
    if mode == "status":
        current_status = existing.get("status", "NEW")
        allowed = sorted(ALLOWED_TRANSITIONS.get(current_status, set()))
        if not allowed:
            print(f"Job is in terminal status: {current_status}")
            return

        print(f"Allowed next statuses: {', '.join(allowed)}")
        next_status = input("Enter next status: ").strip()
        error_message = None
        if next_status.strip().upper() == "FAILED":
            error_message = input("Enter error message (optional): ").strip() or None

        matched, modified = transition_job_status(
            collection,
            job_id,
            next_status,
            error_message=error_message,
        )
        print(f"Matched: {matched}, Modified: {modified}")
        updated = collection.find_one({"_id": job_id})
        if updated is not None:
            print(format_job(updated))
        return

    current_title = existing.get("title", "")
    current_owner = existing.get("owner", "")
    current_priority = existing.get("priority", 0)
    current_tags = existing.get("tags", [])

    title_raw = input(f"Enter title [{current_title}]: ").strip()
    owner_raw = input(f"Enter owner [{current_owner}]: ").strip()
    priority_raw = input(f"Enter priority [{current_priority}]: ").strip()
    tags_raw = input(f"Enter tags (comma-separated) [{', '.join(current_tags)}]: ").strip()

    title = title_raw if title_raw else None
    owner = owner_raw if owner_raw else None
    priority = parse_priority(priority_raw) if priority_raw else None
    tags = parse_tags(tags_raw) if tags_raw else None

    matched, modified = update_job_fields(
        collection,
        job_id,
        title=title,
        owner=owner,
        priority=priority,
        tags=tags,
    )
    if matched == 0:
        print("Job not found")
        return
    if modified == 0:
        print("Nothing to update")
        return

    print(f"Matched: {matched}, Modified: {modified}")
    updated = collection.find_one({"_id": job_id})
    if updated is not None:
        print(format_job(updated))


if __name__ == "__main__":
    main()
