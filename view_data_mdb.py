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
        print(doc)


if __name__ == "__main__":
    main()
