import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import argparse
from src.data_loader import load_chunks
from src.normalizer import normalize_chunk
from src.retrieval import OptimizedGuidelineRetriever
from src.config import logger
from rank_bm25 import BM25Okapi


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunks", required=True, help="Путь к .json или .jsonl")
    args = parser.parse_args()

    logger.info(f"Loading from {args.chunks}")
    raw_chunks = load_chunks(args.chunks)
    logger.info(f"Loaded {len(raw_chunks)} raw chunks")

    # Нормализация
    chunks = []
    for c in raw_chunks:
        norm = normalize_chunk(c)
        if norm:
            chunks.append(norm)
    logger.info(f"Normalized {len(chunks)} chunks")

    if not chunks:
        logger.error("No valid chunks to index")
        return

    texts = [c["text"] for c in chunks]
    metadatas = [c["metadata"] for c in chunks]
    ids = [c["id"] for c in chunks]

    retriever = OptimizedGuidelineRetriever()

    batch_size = 100
    for i in range(0, len(texts), batch_size):
        end = min(i + batch_size, len(texts))
        retriever.collection.add(
            documents=texts[i:end],
            metadatas=metadatas[i:end],
            ids=ids[i:end]
        )
        logger.info(f"Indexed {end}/{len(texts)}")

    # BM25
    retriever.doc_texts = texts
    retriever.doc_ids = ids
    retriever.doc_metadatas = metadatas
    retriever.bm25_index = BM25Okapi([t.split() for t in texts])

    logger.info("Done!")


if __name__ == "__main__":
    main()