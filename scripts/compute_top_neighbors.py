from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np


def compute_top_neighbors(
    embeddings_path: Path,
    output_path: Path,
    top_k: int,
    batch_size: int,
) -> tuple[int, int]:
    embeddings = np.load(embeddings_path, mmap_mode="r")
    num_proteins, dim = embeddings.shape
    if top_k >= num_proteins:
        raise ValueError(f"top_k must be smaller than the number of proteins ({num_proteins})")

    neighbors = np.empty((num_proteins, top_k), dtype=np.int32)

    for start in range(0, num_proteins, batch_size):
        end = min(start + batch_size, num_proteins)
        batch = np.asarray(embeddings[start:end], dtype=np.float32)
        similarities = batch @ embeddings.T

        row_indices = np.arange(start, end)
        similarities[np.arange(end - start), row_indices] = -np.inf

        candidate_count = top_k
        partition_idx = np.argpartition(similarities, -candidate_count, axis=1)[:, -candidate_count:]
        partition_scores = np.take_along_axis(similarities, partition_idx, axis=1)
        order = np.argsort(partition_scores, axis=1)[:, ::-1]
        ranked_idx = np.take_along_axis(partition_idx, order, axis=1)
        neighbors[start:end] = ranked_idx.astype(np.int32, copy=False)

        print(f"Processed rows {start:,}-{end - 1:,} / {num_proteins - 1:,}", flush=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(output_path, neighbors)
    return num_proteins, dim


def register_artifact(db_path: Path, output_path: Path, top_k: int, num_proteins: int) -> None:
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS artifacts (
            artifact_name TEXT PRIMARY KEY,
            version TEXT NOT NULL,
            file_path TEXT NOT NULL,
            dtype TEXT,
            shape_json TEXT,
            notes TEXT
        )
        """
    )
    con.execute(
        """
        INSERT OR REPLACE INTO artifacts VALUES
            ('neighbors_top100_npy', 'neighbors-v1', ?, 'int32', ?, ?)
        """,
        [
            str(output_path),
            json.dumps([num_proteins, top_k]),
            f'Exact top-{top_k} nearest-neighbor indices over normalized reviewed embeddings',
        ],
    )
    con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute exact top-k nearest neighbors from normalized protein embeddings.")
    parser.add_argument(
        "--embeddings",
        type=Path,
        default=Path("data/processed/reviewed_embeddings.npy"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/processed/reviewed_neighbors_top100.npy"),
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("db/protein_game_reviewed.duckdb"),
    )
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=512)
    args = parser.parse_args()

    num_proteins, _ = compute_top_neighbors(
        embeddings_path=args.embeddings,
        output_path=args.output,
        top_k=args.top_k,
        batch_size=args.batch_size,
    )
    register_artifact(args.db, args.output, args.top_k, num_proteins)
    print(f"Saved neighbors to: {args.output}")
    print(f"Registered artifact in: {args.db}")


if __name__ == "__main__":
    main()
