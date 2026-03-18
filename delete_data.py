from bson import ObjectId
from pymongo import MongoClient


def main() -> None:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sample_db"]
    collection = db["sample_collection"]

    mode = input("Delete by (id/name) [id]: ").strip().lower()
    if mode == "name":
        name = input("Enter name to delete: ").strip()
        if not name:
            raise ValueError("Name cannot be empty")
        result = collection.delete_many({"name": name})
        print(f"Deleted: {result.deleted_count}")
        return

    doc_id_raw = input("Enter document _id to delete: ").strip()
    if not ObjectId.is_valid(doc_id_raw):
        raise ValueError("Invalid _id")
    doc_id = ObjectId(doc_id_raw)

    result = collection.delete_one({"_id": doc_id})
    print(f"Deleted: {result.deleted_count}")


if __name__ == "__main__":
    main()
