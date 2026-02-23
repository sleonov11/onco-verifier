import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_loader import load_chunks
from src.normalizer import normalize_chunk
from src.retrieval import OptimizedGuidelineRetriever
from src.config import logger
from rank_bm25 import BM25Okapi


def main():
    data_dir = Path("data")
    if not data_dir.exists():
        logger.error(f"Directory {data_dir} not found")
        return

    # Собираем все .json и .jsonl, кроме golden_cases.json
    files = list(data_dir.glob("*.json")) + list(data_dir.glob("*.jsonl"))
    files = [f for f in files if f.name != "golden_cases.json"]

    if not files:
        logger.warning("No chunk files found (excluding golden_cases.json)")
        return

    all_chunks = []
    for file in files:
        logger.info(f"Loading {file.name}...")
        raw_chunks = load_chunks(file)
        normalized = []
        for c in raw_chunks:
            norm = normalize_chunk(c)
            if norm:
                normalized.append(norm)
        logger.info(f"  → {len(raw_chunks)} raw, {len(normalized)} normalized")
        all_chunks.extend(normalized)

    logger.info(f"Total chunks after normalization: {len(all_chunks)}")
    if not all_chunks:
        logger.error("No valid chunks to index")
        return

    texts = [c["text"] for c in all_chunks]
    metadatas = [c["metadata"] for c in all_chunks]
    ids = [c["id"] for c in all_chunks]

    retriever = OptimizedGuidelineRetriever()

    batch_size = 100
    for i in range(0, len(texts), batch_size):
        end = min(i + batch_size, len(texts))
        retriever.collection.add(
            documents=texts[i:end],
            metadatas=metadatas[i:end],
            ids=ids[i:end]
        )
        logger.info(f"Indexed {end}/{len(texts)} chunks in Chroma")

    retriever.doc_texts = texts
    retriever.doc_ids = ids
    retriever.doc_metadatas = metadatas
    retriever.bm25_index = BM25Okapi([t.split() for t in texts])
    if hasattr(retriever, '_build_metadata_index'):
        retriever._build_metadata_index()

    logger.info("All data indexed successfully!")


if __name__ == "__main__":
    main()