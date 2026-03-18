from pymongo import MongoClient


def main() -> None:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sample_db"]
    collection = db["sample_collection"]

    name = input("Enter name: ").strip()
    if not name:
        raise ValueError("Name cannot be empty")

    age_raw = input("Enter age: ").strip()
    if not age_raw.isdigit():
        raise ValueError("Age must be a number")
    age = int(age_raw)
    if age <= 0:
        raise ValueError("Age must be greater than 0")

    city = input("Enter city: ").strip()
    if not city:
        raise ValueError("City cannot be empty")

    result = collection.insert_one({"name": name, "age": age, "city": city})
    print(f"Inserted document id: {result.inserted_id}")


if __name__ == "__main__":
    main()
