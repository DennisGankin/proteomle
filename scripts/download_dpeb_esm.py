from __future__ import annotations

import argparse
from pathlib import Path

import requests


DPEB_ESM_URL = (
    "https://deepdrug-dpeb.s3.us-west-2.amazonaws.com/"
    "ESM-2/ProteinID_proteinSEQ_ESM_emb.csv"
)


def download_file(url: str, destination: Path, chunk_size: int = 1024 * 1024) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        written = 0
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                handle.write(chunk)
                written += len(chunk)
                if total:
                    percent = written * 100 / total
                    print(
                        f"\rDownloading {destination.name}: "
                        f"{written / 1024**2:.1f} / {total / 1024**2:.1f} MiB "
                        f"({percent:.1f}%)",
                        end="",
                        flush=True,
                    )
        if total:
            print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Download the DPEB ESM-2 aggregated CSV.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw/ProteinID_proteinSEQ_ESM_emb.csv"),
        help="Destination path for the downloaded CSV.",
    )
    args = parser.parse_args()
    print(f"Fetching {DPEB_ESM_URL}")
    download_file(DPEB_ESM_URL, args.output)
    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
