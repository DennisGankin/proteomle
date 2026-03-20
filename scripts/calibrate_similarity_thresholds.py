from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np


def compute_neighbor_similarities(
    embeddings: np.ndarray,
    neighbors: np.ndarray,
    batch_size: int,
) -> np.ndarray:
    num_proteins, top_k = neighbors.shape
    neighbor_sims = np.empty((num_proteins, top_k), dtype=np.float32)

    for start in range(0, num_proteins, batch_size):
        end = min(start + batch_size, num_proteins)
        batch_embeddings = np.asarray(embeddings[start:end], dtype=np.float32)
        similarity_matrix = batch_embeddings @ embeddings.T
        batch_neighbors = neighbors[start:end]
        batch_neighbor_sims = np.take_along_axis(similarity_matrix, batch_neighbors, axis=1)
        neighbor_sims[start:end] = batch_neighbor_sims.astype(np.float32, copy=False)
        print(f"Processed calibration rows {start:,}-{end - 1:,}", flush=True)

    return neighbor_sims


def sample_random_similarities(
    embeddings: np.ndarray,
    sample_size: int,
    seed: int,
) -> np.ndarray:
    rng = np.random.default_rng(seed)
    left = rng.integers(0, embeddings.shape[0], size=sample_size)
    right = rng.integers(0, embeddings.shape[0], size=sample_size)
    mask = np.not_equal(left, right)
    left = left[mask]
    right = right[mask]
    return np.sum(
        np.asarray(embeddings[left], dtype=np.float32) * np.asarray(embeddings[right], dtype=np.float32),
        axis=1,
    ).astype(np.float32, copy=False)


def summarize(values: np.ndarray) -> dict[str, float]:
    return {
        "min": round(float(values.min()), 6),
        "p25": round(float(np.quantile(values, 0.25)), 6),
        "median": round(float(np.quantile(values, 0.5)), 6),
        "p75": round(float(np.quantile(values, 0.75)), 6),
        "max": round(float(values.max()), 6),
        "mean": round(float(values.mean()), 6),
    }


def build_calibration(
    embeddings_path: Path,
    neighbors_path: Path,
    output_path: Path,
    db_path: Path,
    batch_size: int,
    sample_size: int,
    seed: int,
) -> None:
    embeddings = np.load(embeddings_path, mmap_mode="r")
    neighbors = np.load(neighbors_path, mmap_mode="r")

    neighbor_sims = compute_neighbor_similarities(embeddings, neighbors, batch_size=batch_size)
    random_sims = sample_random_similarities(embeddings, sample_size=sample_size, seed=seed)

    rank_5 = neighbor_sims[:, 4]
    rank_25 = neighbor_sims[:, 24]
    rank_100 = neighbor_sims[:, 99]

    payload = {
        "version": "similarity-calibration-v1",
        "source_embeddings": str(embeddings_path),
        "source_neighbors": str(neighbors_path),
        "num_proteins": int(embeddings.shape[0]),
        "embedding_dimension": int(embeddings.shape[1]),
        "method": "median-neighbor-rank-thresholds",
        "thresholds": {
            "extremely_close": round(float(np.quantile(rank_5, 0.5)), 6),
            "very_close": round(float(np.quantile(rank_25, 0.5)), 6),
            "close": round(float(np.quantile(rank_100, 0.5)), 6),
        },
        "derived_from": {
            "extremely_close": {
                "neighbor_rank": 5,
                "summary": summarize(rank_5),
            },
            "very_close": {
                "neighbor_rank": 25,
                "summary": summarize(rank_25),
            },
            "close": {
                "neighbor_rank": 100,
                "summary": summarize(rank_100),
            },
        },
        "random_similarity_sample": {
            "sample_size": int(random_sims.shape[0]),
            "seed": seed,
            "summary": summarize(random_sims),
            "p90": round(float(np.quantile(random_sims, 0.9)), 6),
            "p95": round(float(np.quantile(random_sims, 0.95)), 6),
            "p99": round(float(np.quantile(random_sims, 0.99)), 6),
            "p995": round(float(np.quantile(random_sims, 0.995)), 6),
            "p999": round(float(np.quantile(random_sims, 0.999)), 6),
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

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
            ('similarity_calibration_json', 'similarity-calibration-v1', ?, 'json', NULL, ?)
        """,
        [
            str(output_path),
            'Rank-based closeness thresholds derived from reviewed embedding neighbor similarities',
        ],
    )
    con.close()

    print(f"Saved calibration to: {output_path}")
    print(f"Thresholds: {payload['thresholds']}")
    print(f"Registered artifact in: {db_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate similarity thresholds from reviewed protein embeddings.")
    parser.add_argument(
        "--embeddings",
        type=Path,
        default=Path("data/processed/reviewed_embeddings.npy"),
    )
    parser.add_argument(
        "--neighbors",
        type=Path,
        default=Path("data/processed/reviewed_neighbors_top100.npy"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/processed/reviewed_similarity_calibration.json"),
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("db/protein_game_reviewed.duckdb"),
    )
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--sample-size", type=int, default=300000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    build_calibration(
        embeddings_path=args.embeddings,
        neighbors_path=args.neighbors,
        output_path=args.output,
        db_path=args.db,
        batch_size=args.batch_size,
        sample_size=args.sample_size,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
