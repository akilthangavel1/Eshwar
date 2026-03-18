from pymongo import MongoClient


def main() -> None:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sample_db"]
    collection = db["sample_collection"]

    documents = list(collection.find())
    if not documents:
        print("No data found")
        return

    for doc in documents:
        doc_id = str(doc.get("_id", ""))
        name = doc.get("name", "")
        age = doc.get("age", "")
        city = doc.get("city", "")
        print(f"_id={doc_id} | name={name} | age={age} | city={city}")


if __name__ == "__main__":
    main()
