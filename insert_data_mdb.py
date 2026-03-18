from pymongo import MongoClient


def main() -> None:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sample_db"]
    collection = db["sample_collection"]

    name = input("Enter name: ").strip()
    if not name:
        raise ValueError("Name cannot be empty")

    result = collection.insert_one({"name": name})
    print(f"Inserted document id: {result.inserted_id}")


if __name__ == "__main__":
    main()
