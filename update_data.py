from bson import ObjectId
from pymongo import MongoClient


def main() -> None:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sample_db"]
    collection = db["sample_collection"]

    doc_id_raw = input("Enter document _id to update: ").strip()
    if not ObjectId.is_valid(doc_id_raw):
        raise ValueError("Invalid _id")

    doc_id = ObjectId(doc_id_raw)
    existing = collection.find_one({"_id": doc_id})
    if existing is None:
        print("Document not found")
        return

    current_name = existing.get("name", "")
    current_age = existing.get("age", "")
    current_city = existing.get("city", "")

    name = input(f"Enter name [{current_name}]: ").strip()
    age_raw = input(f"Enter age [{current_age}]: ").strip()
    city = input(f"Enter city [{current_city}]: ").strip()

    update_fields: dict[str, object] = {}

    if name:
        update_fields["name"] = name

    if age_raw:
        if not age_raw.isdigit():
            raise ValueError("Age must be a number")
        age = int(age_raw)
        if age <= 0:
            raise ValueError("Age must be greater than 0")
        update_fields["age"] = age

    if city:
        update_fields["city"] = city

    if not update_fields:
        print("Nothing to update")
        return

    result = collection.update_one({"_id": doc_id}, {"$set": update_fields})
    print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")


if __name__ == "__main__":
    main()
