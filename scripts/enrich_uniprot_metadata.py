from __future__ import annotations

import argparse
import io
from pathlib import Path

import duckdb
import pandas as pd
import requests


UNIPROT_URL = "https://rest.uniprot.org/uniprotkb/search"
FIELDS = [
    "accession",
    "id",
    "protein_name",
    "gene_primary",
    "gene_names",
    "length",
    "reviewed",
    "organism_name",
]


def normalize_alias(value: str) -> str:
    return "".join(ch for ch in value.upper() if ch.isalnum())


def fetch_chunk(session: requests.Session, accessions: list[str]) -> pd.DataFrame:
    query = " OR ".join(f"accession:{accession}" for accession in accessions)
    response = session.get(
        UNIPROT_URL,
        params={
            "query": query,
            "format": "tsv",
            "fields": ",".join(FIELDS),
            "size": len(accessions),
        },
        timeout=60,
    )
    response.raise_for_status()
    table = pd.read_csv(io.StringIO(response.text), sep="\t")
    rename_map = {
        "Entry": "accession",
        "Entry Name": "entry_name",
        "Protein names": "protein_name",
        "Gene Names (primary)": "gene_primary",
        "Gene Names": "gene_names",
        "Length": "length",
        "Reviewed": "reviewed",
        "Organism": "organism_name",
    }
    return table.rename(columns=rename_map)


def batched(values: list[str], chunk_size: int):
    for index in range(0, len(values), chunk_size):
        yield values[index : index + chunk_size]


def build_alias_rows(metadata: pd.DataFrame, proteins: pd.DataFrame, existing_aliases: set[tuple[str, int]]) -> list[dict[str, object]]:
    protein_lookup = dict(zip(proteins["uniprot_accession"], proteins["protein_index"], strict=False))
    alias_rows: list[dict[str, object]] = []

    for row in metadata.itertuples(index=False):
        protein_index = protein_lookup.get(row.accession)
        if protein_index is None:
            continue

        candidate_aliases = [
            (row.entry_name, "entry_name"),
            (row.gene_primary, "gene_symbol"),
            (row.protein_name, "protein_name"),
        ]
        if isinstance(row.gene_names, str):
            candidate_aliases.extend((alias, "gene_synonym") for alias in row.gene_names.split())

        for raw_alias, alias_type in candidate_aliases:
            if not isinstance(raw_alias, str) or not raw_alias.strip():
                continue
            normalized = normalize_alias(raw_alias)
            alias_key = (normalized, protein_index)
            if not normalized or alias_key in existing_aliases:
                continue
            existing_aliases.add(alias_key)
            alias_rows.append(
                {
                    "normalized_alias": normalized,
                    "raw_alias": raw_alias.strip(),
                    "protein_index": protein_index,
                    "alias_type": alias_type,
                }
            )

    return alias_rows


def load_or_fetch_metadata(accessions: list[str], metadata_out: Path, chunk_size: int, force_refresh: bool) -> pd.DataFrame:
    if metadata_out.exists() and not force_refresh:
        print(f"Reusing existing metadata snapshot: {metadata_out}")
        return pd.read_parquet(metadata_out)

    session = requests.Session()
    frames: list[pd.DataFrame] = []
    batches = list(batched(accessions, chunk_size))
    for batch_index, batch in enumerate(batches, start=1):
        frame = fetch_chunk(session, batch)
        frames.append(frame)
        print(f"Fetched UniProt metadata batch {batch_index}/{len(batches)} ({len(batch)} accessions)", flush=True)

    metadata = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["accession"])
    metadata_out.parent.mkdir(parents=True, exist_ok=True)
    metadata.to_parquet(metadata_out, index=False)
    return metadata


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("ALTER TABLE proteins ALTER COLUMN gene_symbol SET DATA TYPE VARCHAR")
    con.execute("ALTER TABLE proteins ALTER COLUMN reviewed SET DATA TYPE BOOLEAN")
    con.execute("ALTER TABLE proteins ALTER COLUMN proteome_id SET DATA TYPE VARCHAR")


def enrich_database(db_path: Path, metadata_out: Path, chunk_size: int, force_refresh: bool) -> None:
    con = duckdb.connect(str(db_path))
    ensure_schema(con)
    proteins = con.execute(
        "SELECT protein_index, uniprot_accession FROM proteins ORDER BY protein_index"
    ).fetch_df()
    existing_alias_df = con.execute(
        "SELECT normalized_alias, protein_index FROM aliases"
    ).fetch_df()
    existing_aliases = set(zip(existing_alias_df["normalized_alias"], existing_alias_df["protein_index"], strict=False))

    accessions = proteins["uniprot_accession"].dropna().astype(str).tolist()
    metadata = load_or_fetch_metadata(accessions, metadata_out, chunk_size, force_refresh)

    con.register("uniprot_metadata_df", metadata)
    con.execute("CREATE OR REPLACE TABLE uniprot_metadata AS SELECT * FROM uniprot_metadata_df")
    con.execute(
        """
        UPDATE proteins AS p
        SET gene_symbol = COALESCE(m.gene_primary, p.gene_symbol),
            display_name = COALESCE(m.protein_name, p.display_name),
            reviewed = CASE
                WHEN m.reviewed = 'reviewed' THEN TRUE
                WHEN m.reviewed = 'unreviewed' THEN FALSE
                ELSE p.reviewed
            END,
            proteome_id = COALESCE(p.proteome_id, 'UP000005640')
        FROM uniprot_metadata AS m
        WHERE p.uniprot_accession = m.accession
        """
    )

    alias_rows = build_alias_rows(metadata, proteins, existing_aliases)
    if alias_rows:
        alias_df = pd.DataFrame(alias_rows)
        con.register("new_aliases_df", alias_df)
        con.execute("INSERT INTO aliases SELECT * FROM new_aliases_df")

    con.execute(
        """
        INSERT OR REPLACE INTO artifacts VALUES
            ('uniprot_metadata_parquet', 'uniprot-enrichment-v1', ?, NULL, NULL, 'UniProt metadata enrichment snapshot')
        """,
        [str(metadata_out)],
    )
    con.close()

    print(f"Saved UniProt metadata to: {metadata_out}")
    print(f"Matched metadata rows: {len(metadata):,}")
    print(f"Inserted new aliases: {len(alias_rows):,}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich the DuckDB protein dataset with UniProt names and aliases.")
    parser.add_argument("--db", type=Path, default=Path("db/protein_game.duckdb"))
    parser.add_argument(
        "--metadata-out",
        type=Path,
        default=Path("data/processed/uniprot_metadata.parquet"),
    )
    parser.add_argument("--chunk-size", type=int, default=100)
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args()
    enrich_database(args.db, args.metadata_out, args.chunk_size, args.force_refresh)


if __name__ == "__main__":
    main()
