# Protl Worker App

This directory contains a Cloudflare Worker-based version of the protein guessing game.

## What it does

- serves the existing frontend as static assets
- implements `/daily`, `/guess`, `/autocomplete`, and `/health` in a TypeScript Worker
- uses a Worker-friendly exported dataset under `public/data/`

## Why this exists

The current FastAPI app depends on `duckdb`, `numpy`, and local binary artifacts, which are not a good fit for standard Cloudflare Workers. This Worker app uses:

- quantized `int8` embeddings
- `int16` top-100 neighbor indices
- sharded alias maps
- static assets fetched through the Worker `ASSETS` binding

## Current design

- embeddings are exported as a single `embeddings.i8.bin` asset
- neighbors are exported as `neighbors.i16.bin`
- protein metadata is stored in `proteins.json`
- alias resolution uses small JSON shards in `public/data/aliases/`
- daily target selection can be restricted to a generated whitelist in `src/target-pool.generated.ts`
- percentile scoring is computed inside the Worker from the quantized embedding matrix and cached per target

## Caveat

This is the right architectural direction for a Worker-native deployment, but it still needs benchmarking on real Cloudflare limits. In particular, percentile computation for a new daily target is still done in the Worker at runtime.

## Build the Worker dataset

From the repository root:

```bash
uv run python worker-app/tools/export_dataset.py
```

To rebuild the curated daily target whitelist:

```bash
python worker-app/tools/build_target_pool.py --top-n 3000
```

## Run locally

```bash
cd worker-app
npm install
npx wrangler dev
```

## Deploy

```bash
cd worker-app
npx wrangler deploy
```
