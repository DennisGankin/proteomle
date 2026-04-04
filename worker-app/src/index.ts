import { TARGET_POOL, TARGET_POOL_SIZE } from "./target-pool.generated";

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

type AlphaFoldPrediction = {
  pdbUrl?: string;
  sequenceStart?: number;
  sequenceEnd?: number;
};

type UniProtSubcellularLocation = {
  location?: {
    value?: string;
  };
};

type UniProtComment = {
  commentType?: string;
  subcellularLocations?: UniProtSubcellularLocation[];
};

type UniProtKeyword = {
  category?: string;
  name?: string;
};

type UniProtProperty = {
  key?: string;
  value?: string;
};

type UniProtCrossReference = {
  database?: string;
  properties?: UniProtProperty[];
};

type UniProtResult = {
  comments?: UniProtComment[];
  keywords?: UniProtKeyword[];
  uniProtKBCrossReferences?: UniProtCrossReference[];
};

type UniProtSearchResponse = {
  results?: UniProtResult[];
};

type DailyHints = {
  localization: string | null;
  goTags: string[];
};

const datasetCache: {
  manifest?: Promise<Manifest>;
  proteins?: Promise<ProteinTable>;
  embeddings?: Promise<Int8Array>;
  neighbors?: Promise<Int16Array>;
  searchEntries?: Promise<SearchEntry[]>;
  sortedSimilarityByTarget: Map<number, Promise<Float32Array>>;
  aliasShardByKey: Map<string, Promise<Record<string, number>>>;
  dailyHintsByAccession: Map<string, Promise<DailyHints>>;
} = {
  sortedSimilarityByTarget: new Map(),
  aliasShardByKey: new Map(),
  dailyHintsByAccession: new Map(),
};

const MAX_TARGET_CACHE = 4;
const SCALE = 127 * 127;
const ALPHAFOLD_HEADERS = {
  Accept: "application/json",
  "User-Agent": "Proteomle/1.0 (+https://proteomle.com)",
};
const ALPHAFOLD_FILE_HEADERS = {
  Accept: "text/plain",
  "User-Agent": "Proteomle/1.0 (+https://proteomle.com)",
};
const UNIPROT_HEADERS = {
  Accept: "application/json",
  "User-Agent": "Proteomle/1.0 (+https://proteomle.com)",
};

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function textResponse(payload: string, status = 200, contentType = "text/plain; charset=utf-8"): Response {
  return new Response(payload, {
    status,
    headers: {
      "content-type": contentType,
      "cache-control": "no-store",
    },
  });
}

function titleCase(value: string): string {
  return value
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function normalizeQuery(value: string): string {
  return value.toUpperCase().replace(/[^A-Z0-9]+/g, "").trim();
}

function canonicalGuessLabel(proteins: ProteinTable, index: number): string {
  return proteins.genes[index] || proteins.ids[index];
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
  if (TARGET_POOL.length > 0) {
    return TARGET_POOL[Number(value % BigInt(TARGET_POOL.length))];
  }
  return Number(value % BigInt(size));
}

function simplifySubcellularLocation(raw: string): string | null {
  const cleaned = raw.replace(/\s+/g, " ").split("{")[0].trim();
  if (!cleaned) {
    return null;
  }
  const primary = cleaned.split(",")[0].trim();
  if (!primary) {
    return null;
  }
  return titleCase(primary);
}

function appendUnique(values: string[], value: string | null, limit = 3): void {
  if (!value || values.includes(value) || values.length >= limit) {
    return;
  }
  values.push(value);
}

function goPropertyValue(reference: UniProtCrossReference, key: string): string | null {
  const property = (reference.properties ?? []).find((item) => item.key === key);
  return property?.value ?? null;
}

function simplifyGoTag(raw: string): string | null {
  const value = raw.toLowerCase();
  const tagRules: Array<[string, RegExp[]]> = [
    ["Kinase", [/kinase/, /phosphorylation/]],
    ["Phosphatase", [/phosphatase/, /dephosphorylation/]],
    ["Receptor signaling", [/receptor/, /signal transduction/, /signaling pathway/]],
    ["Transcription", [/transcription/, /rna polymerase/, /transcription factor/, /dna-binding/]],
    ["RNA processing", [/\brna\b/, /splicing/, /spliceosome/, /mrna/, /rrna/, /trna/]],
    ["DNA repair", [/dna repair/, /dna damage/, /double-strand break/, /genome integrity/, /replication checkpoint/]],
    ["Cell cycle", [/cell cycle/, /mitotic/, /checkpoint/, /chromosome segregation/]],
    ["Apoptosis", [/apoptosis/, /apoptotic/, /cell death/, /necrosis/]],
    ["Immune signaling", [/immune/, /interferon/, /cytokine/, /chemokine/, /antigen/]],
    ["Cytoskeleton", [/cytoskeleton/, /microtubule/, /actin/, /centrosome/, /cilium/]],
    ["Chromatin", [/chromatin/, /histone/, /nucleosome/, /epigenetic/]],
    ["Protein homeostasis", [/ubiquitin/, /proteasom/, /protein folding/, /chaperone/, /autophagy/]],
    ["Transport", [/transport/, /channel/, /pump/, /vesicle/, /ion homeostasis/]],
    ["Metabolism", [/metabolic process/, /biosynthetic process/, /catabolic process/, /oxidation-reduction/]],
  ];

  for (const [tag, patterns] of tagRules) {
    if (patterns.some((pattern) => pattern.test(value))) {
      return tag;
    }
  }
  return null;
}

function extractGoTags(entry: UniProtResult): string[] {
  const tags: string[] = [];

  for (const reference of entry.uniProtKBCrossReferences ?? []) {
    if (reference.database !== "GO") {
      continue;
    }
    const goTerm = goPropertyValue(reference, "GoTerm");
    if (!goTerm || goTerm.startsWith("C:")) {
      continue;
    }
    appendUnique(tags, simplifyGoTag(goTerm.slice(2)), 3);
    if (tags.length >= 3) {
      return tags;
    }
  }

  for (const keyword of entry.keywords ?? []) {
    if (keyword.category === "Cellular component" || keyword.category === "Technical term" || keyword.category === "Disease") {
      continue;
    }
    appendUnique(tags, simplifyGoTag(keyword.name ?? ""), 3);
    if (tags.length >= 3) {
      return tags;
    }
  }

  return tags;
}

async function getDailyHintsForAccession(accession: string): Promise<DailyHints> {
  let cached = datasetCache.dailyHintsByAccession.get(accession);
  if (!cached) {
    cached = (async () => {
      try {
        const response = await fetch(
          `https://rest.uniprot.org/uniprotkb/search?query=accession:${encodeURIComponent(accession)}&format=json&fields=cc_subcellular_location,go_p,go_f,keyword`,
          { headers: UNIPROT_HEADERS },
        );
        if (!response.ok) {
          console.warn("UniProt hint lookup failed", response.status);
          return { localization: null, goTags: [] };
        }

        const payload = (await response.json()) as UniProtSearchResponse;
        const entry = Array.isArray(payload.results) ? payload.results[0] : null;
        if (!entry) {
          return { localization: null, goTags: [] };
        }

        const uniqueLocations: string[] = [];
        for (const comment of entry.comments ?? []) {
          if (comment.commentType !== "SUBCELLULAR LOCATION") {
            continue;
          }
          for (const subcellularLocation of comment.subcellularLocations ?? []) {
            const simplified = simplifySubcellularLocation(subcellularLocation.location?.value ?? "");
            if (simplified && !uniqueLocations.includes(simplified)) {
              uniqueLocations.push(simplified);
            }
          }
        }
        let localization: string | null = null;
        if (uniqueLocations.length > 0) {
          localization = uniqueLocations.slice(0, 2).join(" / ");
        } else {
          const keywordLocation = (entry.keywords ?? [])
            .filter((keyword) => keyword.category === "Cellular component")
            .map((keyword) => simplifySubcellularLocation(keyword.name ?? ""))
            .find((value) => Boolean(value));
          localization = keywordLocation ?? null;
        }

        return {
          localization,
          goTags: extractGoTags(entry),
        };
      } catch (error) {
        console.warn("UniProt hint lookup failed");
        return { localization: null, goTags: [] };
      }
    })();
    datasetCache.dailyHintsByAccession.set(accession, cached);
  }
  return cached;
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

async function resolveExactProteinIndex(env: Env, request: Request, normalized: string): Promise<number | null> {
  const searchEntries = await getSearchEntries(env, request);
  let geneMatch: number | null = null;

  for (let index = 0; index < searchEntries.length; index += 1) {
    const entry = searchEntries[index];
    if (entry.proteinId === normalized) {
      return index;
    }
    if (geneMatch === null && entry.gene === normalized) {
      geneMatch = index;
    }
  }

  return geneMatch;
}

async function resolveGuessIndex(env: Env, request: Request, guess: string): Promise<number> {
  const normalized = normalizeQuery(guess);
  if (!normalized) {
    throw jsonResponse({ detail: "Guess must contain letters or numbers." }, 400);
  }
  const exactIndex = await resolveExactProteinIndex(env, request, normalized);
  if (exactIndex !== null) {
    return exactIndex;
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
  const hints = await getDailyHintsForAccession(proteins.ids[targetIndex]);
  return jsonResponse({
    date: gameDate,
    protein_length: proteins.lengths[targetIndex],
    localization: hints.localization,
    go_tags: hints.goTags,
    category: null,
    dataset_size: manifest.proteins,
  });
}

function sanitizePdb(pdbText: string): string {
  const allowedPrefixes = ["ATOM", "HETATM", "ANISOU", "CONECT", "TER", "MODEL", "ENDMDL", "END"];
  const lines = pdbText.split(/\r?\n/).filter((line) => {
    return allowedPrefixes.some((prefix) => line.startsWith(prefix));
  });
  if (lines.length === 0) {
    return "";
  }
  if (lines[lines.length - 1] !== "END") {
    lines.push("END");
  }
  return `${lines.join("\n")}\n`;
}

async function dailyStructure(env: Env, request: Request, gameDate: string): Promise<Response> {
  const manifest = await getManifest(env, request);
  const proteins = await getProteins(env, request);
  const seed = env.DAILY_SEED || "proteomle-reviewed-v1";
  const targetIndex = await targetIndexForDate(seed, gameDate, manifest.proteins);
  const accession = proteins.ids[targetIndex];

  const predictionResponse = await fetch(`https://alphafold.ebi.ac.uk/api/prediction/${encodeURIComponent(accession)}`, {
    headers: ALPHAFOLD_HEADERS,
  });
  if (!predictionResponse.ok) {
    console.warn("AlphaFold prediction lookup failed", predictionResponse.status);
    return textResponse("Structure unavailable.", 404);
  }

  const predictions = (await predictionResponse.json()) as AlphaFoldPrediction[];
  if (!Array.isArray(predictions) || predictions.length === 0) {
    console.warn("AlphaFold prediction payload empty");
    return textResponse("Structure unavailable.", 404);
  }

  const selectedPrediction = predictions
    .slice()
    .sort((left, right) => (left.sequenceStart ?? 0) - (right.sequenceStart ?? 0))[0];

  if (!selectedPrediction.pdbUrl) {
    console.warn("AlphaFold prediction missing pdbUrl");
    return textResponse("Structure unavailable.", 404);
  }

  const pdbResponse = await fetch(selectedPrediction.pdbUrl, {
    headers: ALPHAFOLD_FILE_HEADERS,
  });
  if (!pdbResponse.ok) {
    console.warn("AlphaFold pdb download failed", pdbResponse.status);
    return textResponse("Structure unavailable.", 404);
  }

  const sanitizedPdb = sanitizePdb(await pdbResponse.text());
  if (!sanitizedPdb) {
    console.warn("Sanitized AlphaFold pdb is empty");
    return textResponse("Structure unavailable.", 404);
  }

  return textResponse(sanitizedPdb);
}

async function health(env: Env, request: Request): Promise<Response> {
  const manifest = await getManifest(env, request);
  return jsonResponse({
    status: "ok",
    proteins: manifest.proteins,
    daily_target_pool: TARGET_POOL_SIZE,
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
    guess: canonicalGuessLabel(proteins, guessIndex),
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
      if (request.method === "GET" && url.pathname === "/daily-structure") {
        const gameDate = parseGameDate(url, env.TIMEZONE || "UTC");
        return await dailyStructure(env, request, gameDate);
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
