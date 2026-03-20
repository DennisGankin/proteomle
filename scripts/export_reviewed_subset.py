from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def export_reviewed_subset(
    source_db: Path,
    source_embeddings: Path,
    target_db: Path,
    target_embeddings: Path,
    target_proteins_parquet: Path,
) -> None:
    con = duckdb.connect(str(source_db))
    proteins = con.execute(
        "SELECT * FROM proteins WHERE reviewed IS TRUE ORDER BY protein_index"
    ).fetch_df()
    aliases = con.execute(
        "SELECT * FROM aliases WHERE protein_index IN (SELECT protein_index FROM proteins WHERE reviewed IS TRUE)"
    ).fetch_df()
    con.close()

    old_indices = proteins["protein_index"].astype(int).tolist()
    remap = {old_index: new_index for new_index, old_index in enumerate(old_indices)}

    proteins = proteins.copy()
    proteins["protein_index"] = proteins["protein_index"].map(remap)

    aliases = aliases.copy()
    aliases["protein_index"] = aliases["protein_index"].map(remap)
    aliases = aliases.dropna(subset=["protein_index"])
    aliases["protein_index"] = aliases["protein_index"].astype(int)

    full_embeddings = np.load(source_embeddings, mmap_mode="r")
    reviewed_embeddings = np.asarray(full_embeddings[old_indices], dtype=np.float32)

    target_embeddings.parent.mkdir(parents=True, exist_ok=True)
    np.save(target_embeddings, reviewed_embeddings)

    target_proteins_parquet.parent.mkdir(parents=True, exist_ok=True)
    proteins.to_parquet(target_proteins_parquet, index=False)

    target_db.parent.mkdir(parents=True, exist_ok=True)
    out = duckdb.connect(str(target_db))
    out.register("proteins_df", proteins)
    out.register("aliases_df", aliases)
    out.execute("CREATE OR REPLACE TABLE proteins AS SELECT * FROM proteins_df")
    out.execute("CREATE OR REPLACE TABLE aliases AS SELECT * FROM aliases_df")
    out.execute(
        """
        CREATE OR REPLACE TABLE artifacts (
            artifact_name TEXT PRIMARY KEY,
            version TEXT NOT NULL,
            file_path TEXT NOT NULL,
            dtype TEXT,
            shape_json TEXT,
            notes TEXT
        )
        """
    )
    out.execute(
        """
        INSERT OR REPLACE INTO artifacts VALUES
            ('proteins_parquet', 'dpeb-esm-reviewed-v1', ?, NULL, NULL, 'Reviewed-only protein metadata derived from DPEB ESM-2 plus UniProt enrichment'),
            ('normalized_embeddings_npy', 'dpeb-esm-reviewed-v1', ?, 'float32', ?, 'Reviewed-only L2-normalized ESM-2 protein embeddings')
        """,
        [
            str(target_proteins_parquet),
            str(target_embeddings),
            json.dumps(list(reviewed_embeddings.shape)),
        ],
    )
    out.close()

    print(f"Reviewed proteins: {len(proteins):,}")
    print(f"Reviewed aliases: {len(aliases):,}")
    print(f"Reviewed embedding matrix shape: {reviewed_embeddings.shape}")
    print(f"Saved reviewed DuckDB to: {target_db}")
    print(f"Saved reviewed embeddings to: {target_embeddings}")
    print(f"Saved reviewed metadata to: {target_proteins_parquet}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export a reviewed-only subset from the protein DuckDB dataset.")
    parser.add_argument("--source-db", type=Path, default=Path("db/protein_game.duckdb"))
    parser.add_argument("--source-embeddings", type=Path, default=Path("data/processed/embeddings.npy"))
    parser.add_argument("--target-db", type=Path, default=Path("db/protein_game_reviewed.duckdb"))
    parser.add_argument(
        "--target-embeddings",
        type=Path,
        default=Path("data/processed/reviewed_embeddings.npy"),
    )
    parser.add_argument(
        "--target-proteins-parquet",
        type=Path,
        default=Path("data/processed/reviewed_proteins.parquet"),
    )
    args = parser.parse_args()
    export_reviewed_subset(
        args.source_db,
        args.source_embeddings,
        args.target_db,
        args.target_embeddings,
        args.target_proteins_parquet,
    )


if __name__ == "__main__":
    main()
