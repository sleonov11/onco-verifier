import json
from pathlib import Path
from typing import List, Dict, Union


def load_chunks(file_path: Union[str, Path]) -> List[Dict]:
    """Универсальный загрузчик JSON и JSONL"""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # JSONL
    if path.suffix == '.jsonl':
        chunks = []
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    chunks.append(json.loads(line))
        return chunks

    # JSON
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    return [data]