import sys
import json
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_loader import load_chunks
from src.normalizer import normalize_chunk
from src.retrieval import OptimizedGuidelineRetriever
from src.config import logger
from rank_bm25 import BM25Okapi


def find_chunk_files(root_dir: Path) -> list:
    """Рекурсивно ищет .json и .jsonl файлы, исключая служебные"""
    files = []
    for ext in ('*.json', '*.jsonl'):
        for f in root_dir.rglob(ext):
            if f.name != 'golden_cases.json':
                files.append(f)
    return files


def main():
    project_root = Path(__file__).parent.parent.parent
    labeled_dir = project_root / 'data' / 'labeled'

    if not labeled_dir.exists():
        logger.error(f"Directory {labeled_dir} does not exist")
        return

    files = find_chunk_files(labeled_dir)
    logger.info(f"Found {len(files)} chunk files in {labeled_dir}")

    all_chunks = []
    for file in files:
        logger.info(f"Loading {file.relative_to(project_root)}...")
        try:
            raw_chunks = load_chunks(file)
            normalized = []
            for c in raw_chunks:
                norm = normalize_chunk(c)
                if norm:
                    normalized.append(norm)
            logger.info(f"  → {len(raw_chunks)} raw, {len(normalized)} normalized")
            all_chunks.extend(normalized)
        except json.JSONDecodeError as e:
            logger.error(f" File {file.name} is empty or not valid JSON, skipping. Error: {e}")
        except Exception as e:
            logger.error(f" Unexpected error with {file.name}: {e}")

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