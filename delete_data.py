from __future__ import annotations

from mongo_job_common import get_collection, normalize_status, object_id_from_input


def main() -> None:
    collection = get_collection()

    mode = input("Delete by (id/owner/status) [id]: ").strip().lower() or "id"
    if mode == "owner":
        owner = input("Enter owner: ").strip()
        if not owner:
            raise ValueError("Owner cannot be empty")
        confirm = input("This deletes multiple jobs. Type YES to confirm: ").strip()
        if confirm != "YES":
            print("Cancelled")
            return
        result = collection.delete_many({"owner": owner})
        print(f"Deleted: {result.deleted_count}")
        return

    if mode == "status":
        status = normalize_status(input("Enter status: ").strip())
        confirm = input("This deletes multiple jobs. Type YES to confirm: ").strip()
        if confirm != "YES":
            print("Cancelled")
            return
        result = collection.delete_many({"status": status})
        print(f"Deleted: {result.deleted_count}")
        return

    job_id = object_id_from_input(input("Enter job _id to delete: "))
    result = collection.delete_one({"_id": job_id})
    print(f"Deleted: {result.deleted_count}")


if __name__ == "__main__":
    main()
