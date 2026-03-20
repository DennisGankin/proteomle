from __future__ import annotations

import argparse
import ast
import csv
import json
import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


REQUIRED_COLUMNS = {"ProteinID", "Protein_sequence", "ESM_Embeddings"}


def normalize_alias(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "", value.upper())
    return normalized.strip()


def parse_embedding(raw_value: str) -> np.ndarray:
    value = ast.literal_eval(raw_value)
    vector = np.asarray(value, dtype=np.float32)
    if vector.ndim != 1:
        raise ValueError(f"Expected a 1D embedding vector, got shape {vector.shape}")
    return vector


def build_dataset(csv_path: Path, db_path: Path, embeddings_path: Path, parquet_path: Path) -> None:
    proteins: list[dict[str, object]] = []
    embeddings: list[np.ndarray] = []
    alias_rows: list[dict[str, object]] = []
    seen_aliases: set[tuple[str, int]] = set()
    embedding_dim: int | None = None

    with csv_path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS.difference(reader.fieldnames or [])
        if missing:
            raise KeyError(f"Missing required columns: {sorted(missing)}")

        for row_index, row in enumerate(reader):
            accession = row["ProteinID"].strip()
            sequence = row["Protein_sequence"].strip()
            embedding = parse_embedding(row["ESM_Embeddings"])
            if embedding_dim is None:
                embedding_dim = int(embedding.shape[0])
            elif embedding.shape[0] != embedding_dim:
                raise ValueError(
                    f"Embedding dimension mismatch at row {row_index}: "
                    f"expected {embedding_dim}, got {embedding.shape[0]}"
                )

            proteins.append(
                {
                    "protein_index": row_index,
                    "uniprot_accession": accession,
                    "gene_symbol": None,
                    "display_name": accession,
                    "sequence": sequence,
                    "length": len(sequence),
                    "reviewed": None,
                    "proteome_id": None,
                    "organism_id": 9606,
                }
            )
            embeddings.append(embedding)

            normalized = normalize_alias(accession)
            alias_key = (normalized, row_index)
            if normalized and alias_key not in seen_aliases:
                seen_aliases.add(alias_key)
                alias_rows.append(
                    {
                        "normalized_alias": normalized,
                        "raw_alias": accession,
                        "protein_index": row_index,
                        "alias_type": "uniprot_accession",
                    }
                )

            if row_index % 1000 == 0 and row_index > 0:
                print(f"Parsed {row_index:,} proteins...", flush=True)

    embedding_matrix = np.stack(embeddings).astype(np.float32, copy=False)
    norms = np.linalg.norm(embedding_matrix, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1.0, norms)
    embedding_matrix = embedding_matrix / norms

    embeddings_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(embeddings_path, embedding_matrix)

    proteins_df = pd.DataFrame(proteins)
    aliases_df = pd.DataFrame(alias_rows)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    proteins_df.to_parquet(parquet_path, index=False)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.register("proteins_df", proteins_df)
    con.register("aliases_df", aliases_df)
    con.execute(
        """
        CREATE OR REPLACE TABLE proteins (
            protein_index INTEGER,
            uniprot_accession VARCHAR,
            gene_symbol VARCHAR,
            display_name VARCHAR,
            sequence VARCHAR,
            length INTEGER,
            reviewed BOOLEAN,
            proteome_id VARCHAR,
            organism_id INTEGER
        )
        """
    )
    con.execute(
        """
        INSERT INTO proteins
        SELECT
            CAST(protein_index AS INTEGER),
            CAST(uniprot_accession AS VARCHAR),
            CAST(gene_symbol AS VARCHAR),
            CAST(display_name AS VARCHAR),
            CAST(sequence AS VARCHAR),
            CAST(length AS INTEGER),
            CAST(reviewed AS BOOLEAN),
            CAST(proteome_id AS VARCHAR),
            CAST(organism_id AS INTEGER)
        FROM proteins_df
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE aliases (
            normalized_alias VARCHAR,
            raw_alias VARCHAR,
            protein_index INTEGER,
            alias_type VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO aliases
        SELECT
            CAST(normalized_alias AS VARCHAR),
            CAST(raw_alias AS VARCHAR),
            CAST(protein_index AS INTEGER),
            CAST(alias_type AS VARCHAR)
        FROM aliases_df
        """
    )
    con.execute(
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
    con.execute(
        """
        INSERT OR REPLACE INTO artifacts VALUES
            ('proteins_parquet', 'dpeb-esm-v1', ?, NULL, NULL, 'Metadata snapshot derived from DPEB ESM-2 CSV'),
            ('normalized_embeddings_npy', 'dpeb-esm-v1', ?, 'float32', ?, 'L2-normalized ESM-2 protein embeddings'),
            ('source_csv', 'dpeb-esm-v1', ?, NULL, NULL, 'Original aggregated DPEB ESM-2 CSV')
        """,
        [
            str(parquet_path),
            str(embeddings_path),
            json.dumps(list(embedding_matrix.shape)),
            str(csv_path),
        ],
    )
    con.close()

    print(f"Loaded {len(proteins_df):,} proteins")
    print(f"Embedding matrix shape: {embedding_matrix.shape}")
    print(f"DuckDB written to: {db_path}")
    print(f"Normalized embeddings written to: {embeddings_path}")
    print(f"Metadata snapshot written to: {parquet_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build DuckDB metadata and aligned embeddings from DPEB ESM-2 CSV.")
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=Path("data/raw/ProteinID_proteinSEQ_ESM_emb.csv"),
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("db/protein_game.duckdb"),
    )
    parser.add_argument(
        "--embeddings-out",
        type=Path,
        default=Path("data/processed/embeddings.npy"),
    )
    parser.add_argument(
        "--proteins-parquet",
        type=Path,
        default=Path("data/processed/proteins.parquet"),
    )
    args = parser.parse_args()
    build_dataset(args.input_csv, args.db, args.embeddings_out, args.proteins_parquet)


if __name__ == "__main__":
    main()
