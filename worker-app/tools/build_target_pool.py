from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PROTEINS_PATH = ROOT / "worker-app/public/data/proteins.json"
TARGET_POOL_TS = ROOT / "worker-app/src/target-pool.generated.ts"

STRONG_EXCLUDE_PREFIXES = (
    "IG",
    "TR",
    "OR",
    "LOC",
    "LINC",
    "MIR",
    "SNORD",
    "SNORA",
    "RNU",
    "RNV",
    "RNA",
)
STRONG_EXCLUDE_PATTERNS = (
    re.compile(r"^C\d+ORF\d+$"),
    re.compile(r"^FAM\d+[A-Z0-9-]*$"),
    re.compile(r"^KIAA\d+$"),
)
TERM_PENALTIES = {
    "putative": 50,
    "probable": 45,
    "uncharacterized": 70,
    "non-functional": 80,
    "fragment": 80,
    "pseudogene": 80,
    "family member": 20,
    "domain-containing": 25,
    "upstream open reading frame": 80,
    "coiled-coil domain-containing": 20,
    "zinc finger protein": 16,
    "olfactory receptor": 120,
    "immunoglobulin": 120,
    "t-cell receptor": 120,
    "readthrough": 60,
    "testis-expressed": 20,
    "small integral membrane protein": 12,
    "transmembrane protein": 12,
    "open reading frame": 50,
    "like": 8,
    "homolog": 4,
}
POSITIVE_TERMS = {
    "kinase": 14,
    "phosphatase": 12,
    "receptor": 12,
    "transcription factor": 12,
    "polymerase": 10,
    "ligase": 8,
    "synthetase": 8,
    "synthase": 8,
    "channel": 10,
    "transporter": 10,
    "integrin": 8,
    "cadherin": 8,
    "collagen": 6,
    "histone": 8,
    "tubulin": 8,
    "actin": 8,
    "myosin": 8,
    "keratin": 6,
    "ribosomal": 6,
    "interferon": 8,
    "chemokine": 8,
    "cytokine": 8,
}
SYMBOL_PENALTY_PATTERNS = {
    re.compile(r"^ZNF\d+$"): 16,
    re.compile(r"^TMEM\d+[A-Z0-9-]*$"): 12,
    re.compile(r"^CCDC\d+[A-Z0-9-]*$"): 12,
    re.compile(r"^ANKRD\d+[A-Z0-9-]*$"): 10,
    re.compile(r"^PRR\d+[A-Z0-9-]*$"): 10,
    re.compile(r"^GOLGA\d+[A-Z0-9-]*$"): 10,
    re.compile(r"^FAM\d+[A-Z0-9-]*$"): 30,
    re.compile(r"^C\d+ORF\d+[A-Z0-9-]*$"): 28,
}
MANUAL_INCLUDE_GENES = {
    "TP53",
    "EGFR",
    "AKT1",
    "MTOR",
    "BRCA1",
    "BRCA2",
    "KRAS",
    "NRAS",
    "HRAS",
    "MAPK1",
    "MAPK3",
    "CDK2",
    "CDK4",
    "CDK6",
    "RB1",
    "MYC",
    "MYCN",
    "STAT3",
    "JAK1",
    "JAK2",
    "INSR",
    "ESR1",
    "AR",
    "CFTR",
    "APP",
    "APOE",
    "PSEN1",
    "PSEN2",
    "HBB",
    "ALB",
    "INS",
}
MANUAL_EXCLUDE_GENES: set[str] = set()


@dataclass(frozen=True)
class ProteinEntry:
    index: int
    gene: str
    name: str
    score: int
    reasons: tuple[str, ...]


def load_proteins(path: Path) -> tuple[list[str], list[str | None], list[str]]:
    payload = json.loads(path.read_text())
    return payload["ids"], payload["genes"], payload["names"]


def score_protein(index: int, gene: str | None, name: str) -> ProteinEntry | None:
    if not gene:
        return None

    gene_upper = gene.upper()
    if gene_upper in MANUAL_EXCLUDE_GENES:
        return None
    if any(gene_upper.startswith(prefix) for prefix in STRONG_EXCLUDE_PREFIXES):
        return None
    if any(pattern.match(gene_upper) for pattern in STRONG_EXCLUDE_PATTERNS):
        return None

    name = name.strip()
    name_lower = name.lower()
    if "immunoglobulin" in name_lower or "olfactory receptor" in name_lower or "t-cell receptor" in name_lower:
        return None

    score = 0
    reasons: list[str] = []

    if gene_upper in MANUAL_INCLUDE_GENES:
        score += 40
        reasons.append("manual-include")

    if 2 <= len(gene_upper) <= 6:
        score += 22
        reasons.append("short-gene")
    elif len(gene_upper) <= 8:
        score += 14
        reasons.append("medium-gene")
    elif len(gene_upper) <= 10:
        score += 6
        reasons.append("longer-gene")
    else:
        score -= 8
        reasons.append("very-long-gene")

    if re.fullmatch(r"[A-Z0-9]+", gene_upper):
        score += 8
        reasons.append("clean-symbol")
    if "-" in gene_upper:
        score -= 6
        reasons.append("hyphenated-symbol")

    digit_count = sum(ch.isdigit() for ch in gene_upper)
    digit_bonus = max(0, 6 - digit_count * 2)
    score += digit_bonus
    if digit_bonus:
        reasons.append("few-digits")

    if len(name) <= 45:
        score += 18
        reasons.append("compact-name")
    elif len(name) <= 80:
        score += 10
        reasons.append("readable-name")
    elif len(name) <= 120:
        score += 4
    else:
        score -= 8
        reasons.append("long-name")

    paren_count = name.count("(")
    if paren_count:
        penalty = min(paren_count * 2, 8)
        score -= penalty
        reasons.append("many-aliases")

    if "," not in name and ";" not in name:
        score += 2

    for term, penalty in TERM_PENALTIES.items():
        if term in name_lower:
            score -= penalty
            reasons.append(f"term:{term}")

    for term, bonus in POSITIVE_TERMS.items():
        if term in name_lower:
            score += bonus
            reasons.append(f"positive:{term}")

    for pattern, penalty in SYMBOL_PENALTY_PATTERNS.items():
        if pattern.match(gene_upper):
            score -= penalty
            reasons.append(f"pattern:{pattern.pattern}")

    return ProteinEntry(index=index, gene=gene_upper, name=name, score=score, reasons=tuple(reasons))


def build_target_pool(proteins_path: Path, top_n: int) -> list[ProteinEntry]:
    _, genes, names = load_proteins(proteins_path)
    candidates = [
        entry
        for index, (gene, name) in enumerate(zip(genes, names, strict=False))
        if (entry := score_protein(index, gene, name)) is not None
    ]
    candidates.sort(key=lambda entry: (-entry.score, entry.gene, entry.name, entry.index))
    return candidates[:top_n]


def write_target_pool_module(entries: list[ProteinEntry], output_path: Path) -> None:
    indices = ", ".join(str(entry.index) for entry in entries)
    output = (
        "// Generated by worker-app/tools/build_target_pool.py\n"
        "// Do not edit by hand.\n\n"
        f"export const TARGET_POOL = [{indices}] as const;\n"
        f"export const TARGET_POOL_SIZE = {len(entries)};\n"
    )
    output_path.write_text(output)


def write_preview_csv(entries: list[ProteinEntry], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["rank", "protein_index", "gene_symbol", "score", "display_name", "reasons"])
        for rank, entry in enumerate(entries, start=1):
            writer.writerow([rank, entry.index, entry.gene, entry.score, entry.name, ";".join(entry.reasons)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a curated daily target pool for the worker app.")
    parser.add_argument("--proteins", type=Path, default=PROTEINS_PATH)
    parser.add_argument("--output", type=Path, default=TARGET_POOL_TS)
    parser.add_argument("--top-n", type=int, default=3000)
    parser.add_argument("--preview-out", type=Path)
    args = parser.parse_args()

    entries = build_target_pool(args.proteins, args.top_n)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_target_pool_module(entries, args.output)
    if args.preview_out:
        write_preview_csv(entries, args.preview_out)

    print(f"Wrote target pool with {len(entries):,} proteins to {args.output}")
    if args.preview_out:
        print(f"Wrote preview CSV to {args.preview_out}")
    print("Top examples:")
    for entry in entries[:15]:
        print(f"  {entry.gene:<10} score={entry.score:<3}  {entry.name}")


if __name__ == "__main__":
    main()
