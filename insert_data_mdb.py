from mongo_job_common import create_job_doc, get_collection, parse_priority, parse_tags


def main() -> None:
    collection = get_collection()

    title = input("Enter job title: ").strip()
    owner = input("Enter owner: ").strip()
    priority = parse_priority(input("Enter priority [0]: "))
    tags = parse_tags(input("Enter tags (comma-separated) []: "))

    job = create_job_doc(title=title, owner=owner, priority=priority, tags=tags)
    result = collection.insert_one(job)
    print(f"Created job id: {result.inserted_id}")


if __name__ == "__main__":
    main()
