interface Env {
  ASSETS: Fetcher;
  DAILY_SEED?: string;
  TIMEZONE?: string;
}

type Manifest = {
  version: string;
  proteins: number;
  dimension: number;
  alias_count: number;
  alias_shard_width: number;
  embeddings_file: string;
  neighbors_file: string;
  proteins_file: string;
};

type ProteinTable = {
  ids: string[];
  genes: Array<string | null>;
  names: string[];
  lengths: number[];
};

type SearchEntry = {
  gene: string;
  proteinId: string;
  name: string;
};

type AutocompleteItem = {
  protein_id: string;
  gene_symbol: string | null;
  name: string;
};

type GuessResponse = {
  guess: string;
  protein_id: string;
  name: string;
  similarity: number;
  similarity_percentile: number;
  rank: number | null;
  is_top_100: boolean;
  is_correct: boolean;
  message: string;
  date: string;
};

const datasetCache: {
  manifest?: Promise<Manifest>;
  proteins?: Promise<ProteinTable>;
  embeddings?: Promise<Int8Array>;
  neighbors?: Promise<Int16Array>;
  searchEntries?: Promise<SearchEntry[]>;
  sortedSimilarityByTarget: Map<number, Promise<Float32Array>>;
  aliasShardByKey: Map<string, Promise<Record<string, number>>>;
} = {
  sortedSimilarityByTarget: new Map(),
  aliasShardByKey: new Map(),
};

const MAX_TARGET_CACHE = 4;
const SCALE = 127 * 127;

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function normalizeQuery(value: string): string {
  return value.toUpperCase().replace(/[^A-Z0-9]+/g, "").trim();
}

async function fetchAsset(env: Env, request: Request, path: string): Promise<Response> {
  return env.ASSETS.fetch(new URL(path, request.url));
}

async function fetchJsonAsset<T>(env: Env, request: Request, path: string): Promise<T> {
  const response = await fetchAsset(env, request, path);
  if (!response.ok) {
    throw new Error(`Could not load asset ${path}: ${response.status}`);
  }
  return (await response.json()) as T;
}

async function fetchBinaryAsset(env: Env, request: Request, path: string): Promise<ArrayBuffer> {
  const response = await fetchAsset(env, request, path);
  if (!response.ok) {
    throw new Error(`Could not load asset ${path}: ${response.status}`);
  }
  return await response.arrayBuffer();
}

function getManifest(env: Env, request: Request): Promise<Manifest> {
  datasetCache.manifest ||= fetchJsonAsset<Manifest>(env, request, "/data/manifest.json");
  return datasetCache.manifest;
}

function getProteins(env: Env, request: Request): Promise<ProteinTable> {
  datasetCache.proteins ||= fetchJsonAsset<ProteinTable>(env, request, "/data/proteins.json");
  return datasetCache.proteins;
}

async function getEmbeddings(env: Env, request: Request): Promise<Int8Array> {
  if (!datasetCache.embeddings) {
    datasetCache.embeddings = (async () => {
      const manifest = await getManifest(env, request);
      const buffer = await fetchBinaryAsset(env, request, `/data/${manifest.embeddings_file}`);
      return new Int8Array(buffer);
    })();
  }
  return datasetCache.embeddings;
}

async function getNeighbors(env: Env, request: Request): Promise<Int16Array> {
  if (!datasetCache.neighbors) {
    datasetCache.neighbors = (async () => {
      const manifest = await getManifest(env, request);
      const buffer = await fetchBinaryAsset(env, request, `/data/${manifest.neighbors_file}`);
      return new Int16Array(buffer);
    })();
  }
  return datasetCache.neighbors;
}

async function getSearchEntries(env: Env, request: Request): Promise<SearchEntry[]> {
  if (!datasetCache.searchEntries) {
    datasetCache.searchEntries = (async () => {
      const proteins = await getProteins(env, request);
      return proteins.ids.map((proteinId, index) => ({
        gene: normalizeQuery(proteins.genes[index] ?? ""),
        proteinId: normalizeQuery(proteinId),
        name: normalizeQuery(proteins.names[index]),
      }));
    })();
  }
  return datasetCache.searchEntries;
}

async function getAliasShard(env: Env, request: Request, normalized: string): Promise<Record<string, number>> {
  const manifest = await getManifest(env, request);
  const key = normalized.length >= manifest.alias_shard_width
    ? normalized.slice(0, manifest.alias_shard_width)
    : normalized.padEnd(manifest.alias_shard_width, "_");

  let shardPromise = datasetCache.aliasShardByKey.get(key);
  if (!shardPromise) {
    shardPromise = (async () => {
      const response = await fetchAsset(env, request, `/data/aliases/${key}.json`);
      if (response.status === 404) {
        return {};
      }
      if (!response.ok) {
        throw new Error(`Could not load alias shard ${key}: ${response.status}`);
      }
      return (await response.json()) as Record<string, number>;
    })();
    datasetCache.aliasShardByKey.set(key, shardPromise);
  }
  return shardPromise;
}

function roundTo(value: number, digits: number): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function closenessMessage(percentile: number, isCorrect: boolean): string {
  if (isCorrect) {
    return "Correct!";
  }
  if (percentile >= 99.0) {
    return "Very close";
  }
  if (percentile >= 95.0) {
    return "Close";
  }
  if (percentile >= 80.0) {
    return "Warm";
  }
  if (percentile >= 15.0) {
    return "Far";
  }
  return "Very far";
}

function parseGameDate(url: URL, timezone: string): string {
  const day = url.searchParams.get("day");
  if (day) {
    return day;
  }
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

async function targetIndexForDate(seed: string, gameDate: string, size: number): Promise<number> {
  const bytes = new TextEncoder().encode(`${seed}:${gameDate}`);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  const view = new DataView(digest);
  const value = view.getBigUint64(0, false);
  return Number(value % BigInt(size));
}

function dotProduct(embeddings: Int8Array, dimension: number, leftIndex: number, rightIndex: number): number {
  const leftOffset = leftIndex * dimension;
  const rightOffset = rightIndex * dimension;
  let sum = 0;
  for (let i = 0; i < dimension; i += 1) {
    sum += embeddings[leftOffset + i] * embeddings[rightOffset + i];
  }
  return sum / SCALE;
}

async function getSortedSimilaritiesForTarget(
  env: Env,
  request: Request,
  targetIndex: number,
): Promise<Float32Array> {
  let cached = datasetCache.sortedSimilarityByTarget.get(targetIndex);
  if (!cached) {
    cached = (async () => {
      const manifest = await getManifest(env, request);
      const embeddings = await getEmbeddings(env, request);
      const values = new Float32Array(manifest.proteins);
      for (let index = 0; index < manifest.proteins; index += 1) {
        values[index] = dotProduct(embeddings, manifest.dimension, index, targetIndex);
      }
      values.sort();
      return values;
    })();
    datasetCache.sortedSimilarityByTarget.set(targetIndex, cached);
    if (datasetCache.sortedSimilarityByTarget.size > MAX_TARGET_CACHE) {
      const oldestKey = datasetCache.sortedSimilarityByTarget.keys().next().value as number | undefined;
      if (oldestKey !== undefined && oldestKey !== targetIndex) {
        datasetCache.sortedSimilarityByTarget.delete(oldestKey);
      }
    }
  }
  return cached;
}

function percentileFromSorted(sorted: Float32Array, similarity: number): number {
  let low = 0;
  let high = sorted.length;
  while (low < high) {
    const mid = (low + high) >> 1;
    if (sorted[mid] <= similarity) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return (low / sorted.length) * 100;
}

function compareScore(left: [number, number, number], right: [number, number, number]): number {
  if (left[0] !== right[0]) {
    return left[0] - right[0];
  }
  if (left[1] !== right[1]) {
    return left[1] - right[1];
  }
  return left[2] - right[2];
}

async function autocomplete(env: Env, request: Request, query: string, limit: number): Promise<AutocompleteItem[]> {
  const normalized = normalizeQuery(query);
  if (!normalized) {
    return [];
  }

  const proteins = await getProteins(env, request);
  const searchEntries = await getSearchEntries(env, request);
  const scored: Array<{ score: [number, number, number]; index: number }> = [];

  for (let index = 0; index < searchEntries.length; index += 1) {
    const entry = searchEntries[index];
    const candidates: Array<[string, number]> = [
      [entry.gene, 0],
      [entry.proteinId, 1],
      [entry.name, 2],
    ];

    let bestScore: [number, number, number] | null = null;
    for (const [candidate, fieldPriority] of candidates) {
      if (!candidate) {
        continue;
      }
      let score: [number, number, number] | null = null;
      if (candidate === normalized) {
        score = [0, fieldPriority, index];
      } else if (candidate.startsWith(normalized)) {
        score = [1, fieldPriority, index];
      } else if (candidate.includes(normalized)) {
        score = [2, fieldPriority, index];
      }
      if (score && (!bestScore || compareScore(score, bestScore) < 0)) {
        bestScore = score;
      }
    }

    if (bestScore) {
      scored.push({ score: bestScore, index });
    }
  }

  scored.sort((left, right) => compareScore(left.score, right.score));
  return scored.slice(0, limit).map(({ index }) => ({
    protein_id: proteins.ids[index],
    gene_symbol: proteins.genes[index],
    name: proteins.names[index],
  }));
}

async function resolveGuessIndex(env: Env, request: Request, guess: string): Promise<number> {
  const normalized = normalizeQuery(guess);
  if (!normalized) {
    throw jsonResponse({ detail: "Guess must contain letters or numbers." }, 400);
  }
  const shard = await getAliasShard(env, request, normalized);
  const value = shard[normalized];
  if (value !== undefined) {
    return value;
  }

  const suggestions = await autocomplete(env, request, guess, 5);
  throw jsonResponse(
    {
      detail: {
        message: `Unknown protein guess: ${guess}`,
        suggestions,
      },
    },
    404,
  );
}

async function dailySummary(env: Env, request: Request, gameDate: string): Promise<Response> {
  const manifest = await getManifest(env, request);
  const proteins = await getProteins(env, request);
  const seed = env.DAILY_SEED || "proteomle-reviewed-v1";
  const targetIndex = await targetIndexForDate(seed, gameDate, manifest.proteins);
  return jsonResponse({
    date: gameDate,
    protein_length: proteins.lengths[targetIndex],
    category: null,
    dataset_size: manifest.proteins,
  });
}

async function health(env: Env, request: Request): Promise<Response> {
  const manifest = await getManifest(env, request);
  return jsonResponse({
    status: "ok",
    proteins: manifest.proteins,
    aliases: manifest.alias_count,
    embedding_shape: [manifest.proteins, manifest.dimension],
    neighbors_shape: [manifest.proteins, 100],
  });
}

async function guess(env: Env, request: Request): Promise<Response> {
  const manifest = await getManifest(env, request);
  const proteins = await getProteins(env, request);
  const embeddings = await getEmbeddings(env, request);
  const neighbors = await getNeighbors(env, request);
  const payload = (await request.json()) as { guess?: string; date?: string };
  const rawGuess = (payload.guess ?? "").trim();
  const timezone = env.TIMEZONE || "UTC";
  const gameDate = payload.date ?? parseGameDate(new URL(request.url), timezone);
  const seed = env.DAILY_SEED || "proteomle-reviewed-v1";
  const targetIndex = await targetIndexForDate(seed, gameDate, manifest.proteins);
  const guessIndex = await resolveGuessIndex(env, request, rawGuess);

  const similarity = dotProduct(embeddings, manifest.dimension, guessIndex, targetIndex);
  const sorted = await getSortedSimilaritiesForTarget(env, request, targetIndex);
  let similarityPercentile = percentileFromSorted(sorted, similarity);
  const isCorrect = guessIndex === targetIndex;
  if (!isCorrect) {
    similarityPercentile = Math.min(similarityPercentile, 99.9);
  }

  let rank: number | null = null;
  let isTop100 = false;
  if (isCorrect) {
    rank = 1;
    isTop100 = true;
  } else {
    const offset = targetIndex * 100;
    for (let index = 0; index < 100; index += 1) {
      if (neighbors[offset + index] === guessIndex) {
        rank = index + 2;
        isTop100 = true;
        break;
      }
    }
  }

  const response: GuessResponse = {
    guess: rawGuess,
    protein_id: proteins.ids[guessIndex],
    name: proteins.names[guessIndex],
    similarity: roundTo(similarity, 6),
    similarity_percentile: roundTo(similarityPercentile, 2),
    rank,
    is_top_100: isTop100,
    is_correct: isCorrect,
    message: closenessMessage(similarityPercentile, isCorrect),
    date: gameDate,
  };
  return jsonResponse(response);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    try {
      if (request.method === "GET" && url.pathname === "/health") {
        return await health(env, request);
      }
      if (request.method === "GET" && url.pathname === "/daily") {
        const gameDate = parseGameDate(url, env.TIMEZONE || "UTC");
        return await dailySummary(env, request, gameDate);
      }
      if (request.method === "POST" && url.pathname === "/guess") {
        return await guess(env, request);
      }
      if (request.method === "GET" && url.pathname === "/autocomplete") {
        const query = url.searchParams.get("q") ?? "";
        const limit = Math.min(Math.max(Number(url.searchParams.get("limit") ?? "10"), 1), 20);
        const suggestions = await autocomplete(env, request, query, limit);
        return jsonResponse({ query, suggestions });
      }
      return env.ASSETS.fetch(request);
    } catch (error) {
      if (error instanceof Response) {
        return error;
      }
      return jsonResponse({ detail: { message: error instanceof Error ? error.message : "Worker request failed." } }, 500);
    }
  },
};
