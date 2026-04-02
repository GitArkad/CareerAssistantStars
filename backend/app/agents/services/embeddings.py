from sentence_transformers import SentenceTransformer
from app.config.settings import EMBEDDING_MODEL

_model = None


def get_model():
    global _model
    if _model is None:
        print(f">>> LOADING EMBEDDING MODEL: {EMBEDDING_MODEL}")
        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def get_embedding(text: str) -> list[float]:
    model = get_model()
    embedding = model.encode(text, normalize_embeddings=True)
    return embedding.tolist()