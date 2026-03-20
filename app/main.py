from __future__ import annotations

import hashlib
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date as DateType, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


STATIC_DIR = Path(__file__).resolve().parent / "static"


class NoCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response


def normalize_query(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", value.upper()).strip()


def closeness_message(similarity: float, is_correct: bool = False) -> str:
    if is_correct:
        return "Correct!"
    if similarity > 0.9:
        return "Extremely close"
    if similarity > 0.75:
        return "Very close"
    if similarity > 0.6:
        return "Close"
    return "Far"


@dataclass(frozen=True)
class ProteinRecord:
    protein_index: int
    uniprot_accession: str
    gene_symbol: str | None
    display_name: str
    length: int
    reviewed: bool | None


@dataclass(frozen=True)
class AppConfig:
    db_path: Path
    embeddings_path: Path
    neighbors_path: Path
    timezone_name: str
    daily_seed: str

    @classmethod
    def from_env(cls) -> AppConfig:
        root = Path(__file__).resolve().parent.parent
        return cls(
            db_path=Path(os.getenv("PROTEOMLE_DB_PATH", root / "db/protein_game_reviewed.duckdb")),
            embeddings_path=Path(
                os.getenv("PROTEOMLE_EMBEDDINGS_PATH", root / "data/processed/reviewed_embeddings.npy")
            ),
            neighbors_path=Path(
                os.getenv("PROTEOMLE_NEIGHBORS_PATH", root / "data/processed/reviewed_neighbors_top100.npy")
            ),
            timezone_name=os.getenv("PROTEOMLE_TIMEZONE", "UTC"),
            daily_seed=os.getenv("PROTEOMLE_DAILY_SEED", "proteomle-reviewed-v1"),
        )


class GuessRequest(BaseModel):
    guess: str = Field(..., min_length=1)
    date: DateType | None = None


class GuessResponse(BaseModel):
    guess: str
    protein_id: str
    name: str
    similarity: float
    rank: int | None
    is_top_100: bool
    is_correct: bool
    message: str
    date: DateType


class DailyResponse(BaseModel):
    date: DateType
    protein_length: int
    category: str | None
    dataset_size: int


class AutocompleteItem(BaseModel):
    protein_id: str
    gene_symbol: str | None
    name: str


class AutocompleteResponse(BaseModel):
    query: str
    suggestions: list[AutocompleteItem]


class HealthResponse(BaseModel):
    status: str
    proteins: int
    aliases: int
    embedding_shape: tuple[int, int]
    neighbors_shape: tuple[int, int]


@dataclass(frozen=True)
class ProteinGameData:
    proteins: list[ProteinRecord]
    embeddings: np.ndarray
    neighbors_top100: np.ndarray
    alias_to_index: dict[str, int]
    alias_count: int

    @classmethod
    def load(cls, config: AppConfig) -> ProteinGameData:
        con = duckdb.connect(str(config.db_path), read_only=True)
        proteins_df = con.execute(
            """
            SELECT protein_index, uniprot_accession, gene_symbol, display_name, length, reviewed
            FROM proteins
            ORDER BY protein_index
            """
        ).fetch_df()
        aliases_df = con.execute(
            "SELECT normalized_alias, protein_index, alias_type FROM aliases"
        ).fetch_df()
        con.close()

        embeddings = np.load(config.embeddings_path, mmap_mode="r")
        neighbors_top100 = np.load(config.neighbors_path, mmap_mode="r")

        num_proteins = len(proteins_df)
        if embeddings.shape[0] != num_proteins:
            raise ValueError(
                f"Embedding rows ({embeddings.shape[0]}) do not match proteins table ({num_proteins})"
            )
        if neighbors_top100.shape[0] != num_proteins:
            raise ValueError(
                f"Neighbor rows ({neighbors_top100.shape[0]}) do not match proteins table ({num_proteins})"
            )

        proteins: list[ProteinRecord] = []
        for row in proteins_df.itertuples(index=False):
            gene_symbol = None if pd.isna(row.gene_symbol) else str(row.gene_symbol)
            reviewed = None if pd.isna(row.reviewed) else bool(row.reviewed)
            proteins.append(
                ProteinRecord(
                    protein_index=int(row.protein_index),
                    uniprot_accession=str(row.uniprot_accession),
                    gene_symbol=gene_symbol,
                    display_name=str(row.display_name),
                    length=int(row.length),
                    reviewed=reviewed,
                )
            )

        priority = {
            "uniprot_accession": 0,
            "gene_symbol": 1,
            "entry_name": 2,
            "protein_name": 3,
            "gene_synonym": 4,
            "display_name": 5,
        }
        alias_to_index: dict[str, int] = {}
        alias_best_priority: dict[str, tuple[int, int]] = {}
        for row in aliases_df.itertuples(index=False):
            alias = str(row.normalized_alias)
            protein_index = int(row.protein_index)
            alias_priority = priority.get(str(row.alias_type), 99)
            current = alias_best_priority.get(alias)
            candidate = (alias_priority, protein_index)
            if current is None or candidate < current:
                alias_best_priority[alias] = candidate
                alias_to_index[alias] = protein_index

        return cls(
            proteins=proteins,
            embeddings=embeddings,
            neighbors_top100=neighbors_top100,
            alias_to_index=alias_to_index,
            alias_count=len(aliases_df),
        )


class ProteinGameService:
    def __init__(self, config: AppConfig, data: ProteinGameData) -> None:
        self.config = config
        self.data = data
        self.timezone = ZoneInfo(config.timezone_name)

    @classmethod
    def load(cls) -> ProteinGameService:
        config = AppConfig.from_env()
        data = ProteinGameData.load(config)
        return cls(config=config, data=data)

    def current_game_date(self) -> DateType:
        return datetime.now(self.timezone).date()

    def target_index_for_date(self, game_date: DateType) -> int:
        digest = hashlib.sha256(f"{self.config.daily_seed}:{game_date.isoformat()}".encode("utf-8")).digest()
        return int.from_bytes(digest[:8], byteorder="big") % len(self.data.proteins)

    def target_record_for_date(self, game_date: DateType) -> ProteinRecord:
        return self.data.proteins[self.target_index_for_date(game_date)]

    def resolve_guess(self, guess: str) -> int:
        normalized = normalize_query(guess)
        if not normalized:
            raise HTTPException(status_code=400, detail="Guess must contain letters or numbers.")
        protein_index = self.data.alias_to_index.get(normalized)
        if protein_index is None:
            suggestions = [item.model_dump() for item in self.autocomplete(guess, limit=5)]
            raise HTTPException(
                status_code=404,
                detail={
                    "message": f"Unknown protein guess: {guess}",
                    "suggestions": suggestions,
                },
            )
        return protein_index

    def daily_summary(self, game_date: DateType | None = None) -> DailyResponse:
        resolved_date = game_date or self.current_game_date()
        target = self.target_record_for_date(resolved_date)
        return DailyResponse(
            date=resolved_date,
            protein_length=target.length,
            category=None,
            dataset_size=len(self.data.proteins),
        )

    def score_guess(self, guess: str, game_date: DateType | None = None) -> GuessResponse:
        resolved_date = game_date or self.current_game_date()
        target_index = self.target_index_for_date(resolved_date)
        guess_index = self.resolve_guess(guess)

        guess_record = self.data.proteins[guess_index]
        similarity = float(np.dot(self.data.embeddings[guess_index], self.data.embeddings[target_index]))
        is_correct = guess_index == target_index

        rank: int | None = None
        is_top_100 = False
        if is_correct:
            rank = 0
            is_top_100 = True
        else:
            neighbor_row = self.data.neighbors_top100[target_index]
            match_positions = np.flatnonzero(neighbor_row == guess_index)
            if match_positions.size:
                rank = int(match_positions[0]) + 1
                is_top_100 = True

        return GuessResponse(
            guess=guess.strip(),
            protein_id=guess_record.uniprot_accession,
            name=guess_record.display_name,
            similarity=round(similarity, 6),
            rank=rank,
            is_top_100=is_top_100,
            is_correct=is_correct,
            message=closeness_message(similarity, is_correct=is_correct),
            date=resolved_date,
        )

    def autocomplete(self, query: str, limit: int = 10) -> list[AutocompleteItem]:
        normalized = normalize_query(query)
        if not normalized:
            return []

        scored: list[tuple[tuple[int, int, int], AutocompleteItem]] = []
        for protein in self.data.proteins:
            candidates = []
            if protein.gene_symbol:
                candidates.append((protein.gene_symbol, 0))
            candidates.append((protein.uniprot_accession, 1))
            candidates.append((protein.display_name, 2))

            best_score: tuple[int, int, int] | None = None
            for candidate_text, field_priority in candidates:
                normalized_candidate = normalize_query(candidate_text)
                if not normalized_candidate:
                    continue
                if normalized_candidate == normalized:
                    score = (0, field_priority, protein.protein_index)
                elif normalized_candidate.startswith(normalized):
                    score = (1, field_priority, protein.protein_index)
                elif normalized in normalized_candidate:
                    score = (2, field_priority, protein.protein_index)
                else:
                    continue

                if best_score is None or score < best_score:
                    best_score = score

            if best_score is not None:
                scored.append(
                    (
                        best_score,
                        AutocompleteItem(
                            protein_id=protein.uniprot_accession,
                            gene_symbol=protein.gene_symbol,
                            name=protein.display_name,
                        ),
                    )
                )

        scored.sort(key=lambda item: item[0])
        return [item for _, item in scored[:limit]]

    def health(self) -> HealthResponse:
        return HealthResponse(
            status="ok",
            proteins=len(self.data.proteins),
            aliases=self.data.alias_count,
            embedding_shape=tuple(int(value) for value in self.data.embeddings.shape),
            neighbors_shape=tuple(int(value) for value in self.data.neighbors_top100.shape),
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.game_service = ProteinGameService.load()
    yield


app = FastAPI(title="Protein Guessing Game API", version="0.1.0", lifespan=lifespan)
app.add_middleware(NoCacheMiddleware)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def get_service(request: Request) -> ProteinGameService:
    return request.app.state.game_service


@app.get("/health", response_model=HealthResponse)
def health(request: Request) -> HealthResponse:
    return get_service(request).health()


@app.get("/daily", response_model=DailyResponse)
def daily(request: Request, day: DateType | None = Query(default=None)) -> DailyResponse:
    return get_service(request).daily_summary(day)


@app.post("/guess", response_model=GuessResponse)
def guess(request: Request, payload: GuessRequest) -> GuessResponse:
    return get_service(request).score_guess(payload.guess, payload.date)


@app.get("/autocomplete", response_model=AutocompleteResponse)
def autocomplete(
    request: Request,
    q: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=1, le=20),
) -> AutocompleteResponse:
    suggestions = get_service(request).autocomplete(q, limit=limit)
    return AutocompleteResponse(query=q, suggestions=suggestions)


@app.get("/", include_in_schema=False)
def root() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")
