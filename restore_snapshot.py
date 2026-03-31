from qdrant_client import QdrantClient

client = QdrantClient(url="http://localhost:6333", check_compatibility=False)

client.recover_snapshot(
    collection_name="vacancies",
    location="file:///qdrant/snapshots/vacancies_backup.snapshot",
    wait=True,
)

print("Collection restored")