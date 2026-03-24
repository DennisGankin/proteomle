from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "db/protein_game_reviewed.duckdb"
EMBEDDINGS_PATH = ROOT / "data/processed/reviewed_embeddings.npy"
NEIGHBORS_PATH = ROOT / "data/processed/reviewed_neighbors_top100.npy"
OUTPUT_DIR = ROOT / "worker-app/public/data"
ALIASES_DIR = OUTPUT_DIR / "aliases"
SHARD_WIDTH = 2
ALIAS_PRIORITY = {
    "uniprot_accession": 0,
    "gene_symbol": 1,
    "entry_name": 2,
    "protein_name": 3,
    "gene_synonym": 4,
    "display_name": 5,
}


def shard_key(alias: str) -> str:
    if len(alias) >= SHARD_WIDTH:
        return alias[:SHARD_WIDTH]
    return alias.ljust(SHARD_WIDTH, "_")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ALIASES_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH), read_only=True)
    proteins_df = con.execute(
        """
        SELECT protein_index, uniprot_accession, gene_symbol, display_name, length
        FROM proteins
        ORDER BY protein_index
        """
    ).fetch_df()
    aliases_df = con.execute(
        """
        SELECT normalized_alias, protein_index, alias_type
        FROM aliases
        ORDER BY normalized_alias, protein_index
        """
    ).fetch_df()
    con.close()

    proteins = {
        "ids": proteins_df["uniprot_accession"].astype(str).tolist(),
        "genes": [None if value != value else str(value) for value in proteins_df["gene_symbol"].tolist()],
        "names": proteins_df["display_name"].astype(str).tolist(),
        "lengths": proteins_df["length"].astype(int).tolist(),
    }
    (OUTPUT_DIR / "proteins.json").write_text(json.dumps(proteins, separators=(",", ":")))

    embeddings = np.load(EMBEDDINGS_PATH)
    quantized_embeddings = np.clip(np.rint(embeddings * 127.0), -127, 127).astype(np.int8)
    (OUTPUT_DIR / "embeddings.i8.bin").write_bytes(quantized_embeddings.tobytes(order="C"))

    neighbors = np.load(NEIGHBORS_PATH).astype(np.int16, copy=False)
    (OUTPUT_DIR / "neighbors.i16.bin").write_bytes(neighbors.tobytes(order="C"))

    alias_best_priority: dict[str, tuple[int, int]] = {}
    alias_to_index: dict[str, int] = {}
    for alias, protein_index, alias_type in aliases_df.itertuples(index=False):
        alias_text = str(alias)
        candidate = (ALIAS_PRIORITY.get(str(alias_type), 99), int(protein_index))
        current = alias_best_priority.get(alias_text)
        if current is None or candidate < current:
            alias_best_priority[alias_text] = candidate
            alias_to_index[alias_text] = int(protein_index)

    shards: dict[str, dict[str, int]] = defaultdict(dict)
    for alias_text, protein_index in alias_to_index.items():
        shards[shard_key(alias_text)][alias_text] = protein_index

    for existing in ALIASES_DIR.glob("*.json"):
        existing.unlink()

    for key, payload in shards.items():
        (ALIASES_DIR / f"{key}.json").write_text(json.dumps(payload, separators=(",", ":")))

    manifest = {
        "version": "worker-reviewed-v2",
        "proteins": int(quantized_embeddings.shape[0]),
        "dimension": int(quantized_embeddings.shape[1]),
        "alias_count": int(len(alias_to_index)),
        "alias_shard_width": SHARD_WIDTH,
        "embeddings_file": "embeddings.i8.bin",
        "neighbors_file": "neighbors.i16.bin",
        "proteins_file": "proteins.json",
    }
    (OUTPUT_DIR / "manifest.json").write_text(json.dumps(manifest, separators=(",", ":")))

    print(f"Wrote worker dataset to {OUTPUT_DIR}")
    print(f"Embeddings bytes: {quantized_embeddings.nbytes}")
    print(f"Neighbors bytes: {neighbors.nbytes}")
    print(f"Alias shards: {len(shards)}")


if __name__ == "__main__":
    main()
