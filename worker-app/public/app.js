const state = {
  history: [],
  bestGuess: null,
  latestGuess: null,
  gameDate: null,
  nextGuessNumber: 1,
  suggestionController: null,
  suggestionTimerId: null,
  latestSuggestionQuery: "",
  guessController: null,
  activeGuessId: 0,
  isSubmittingGuess: false,
};

const STORAGE_KEY = "protl-guess-state";

const dailyDate = document.getElementById("daily-date");
const guessForm = document.getElementById("guess-form");
const guessInput = document.getElementById("guess-input");
const guessButton = document.getElementById("guess-button");
const suggestionsBox = document.getElementById("suggestions");
const formMessage = document.getElementById("form-message");
const latestResult = document.getElementById("latest-result");
const bestGuessPanel = document.getElementById("best-guess");
const healthState = document.getElementById("health-state");
const healthMeta = document.getElementById("health-meta");
const embeddingMeta = document.getElementById("embedding-meta");
const historyBody = document.getElementById("history-body");
const historySummary = document.getElementById("history-summary");
const structureLengthBadge = document.getElementById("structure-length-badge");
const structureViewer = document.getElementById("structure-viewer");
const winModal = document.getElementById("win-modal");
const winTitle = document.getElementById("win-title");
const winSubtitle = document.getElementById("win-subtitle");
const winTries = document.getElementById("win-tries");
const winChart = document.getElementById("win-chart");
let structureViewerInstance = null;
let threeDMolPromise = null;

async function fetchJson(url, options) {
  const requestOptions = Object.assign({ cache: "no-store" }, options || {});
  const timeoutMs = requestOptions.timeoutMs || 30000;
  delete requestOptions.timeoutMs;

  const controller = new AbortController();
  let timedOut = false;
  const timeoutId = window.setTimeout(function () {
    timedOut = true;
    controller.abort();
  }, timeoutMs);

  const externalSignal = requestOptions.signal || null;
  if (externalSignal) {
    if (externalSignal.aborted) {
      controller.abort();
    } else {
      externalSignal.addEventListener("abort", function () {
        controller.abort();
      }, { once: true });
    }
  }
  requestOptions.signal = controller.signal;

  try {
    const response = await fetch(url, requestOptions);
    const contentType = response.headers.get("content-type") || "";
    let payload = null;

    if (contentType.includes("application/json")) {
      payload = await response.json();
    } else {
      payload = await response.text();
    }

    if (response.ok === false) {
      throw payload;
    }
    return payload;
  } catch (error) {
    if (error.name === "AbortError" && timedOut) {
      throw { message: "The request timed out. Please try again." };
    }
    throw error;
  } finally {
    window.clearTimeout(timeoutId);
  }
}

function setFormMessage(message, kind) {
  formMessage.textContent = message || "";
  formMessage.className = "form-message" + (kind ? " " + kind : "");
}
function clearPersistedState() {
  state.history = [];
  state.bestGuess = null;
  state.latestGuess = null;
  state.nextGuessNumber = 1;
  closeWinModal();

  try {
    window.localStorage.removeItem(STORAGE_KEY);
  } catch (error) {
    console.warn("Could not clear guess history", error);
  }
}

function persistState() {
  try {
    const payload = {
      version: 1,
      date: state.gameDate,
      history: state.history,
      latestGuess: state.latestGuess,
      nextGuessNumber: state.nextGuessNumber,
    };
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  } catch (error) {
    console.warn("Could not persist guess history", error);
  }
}

function loadPersistedState() {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      state.history = [];
      state.bestGuess = null;
      state.latestGuess = null;
      return false;
    }

    const payload = JSON.parse(raw);
    const history = Array.isArray(payload.history) ? payload.history : [];
    const latestGuess = payload.latestGuess && typeof payload.latestGuess === "object" ? payload.latestGuess : null;
    state.gameDate = typeof payload.date === "string" ? payload.date : null;
    state.history = history;
    const derivedNextGuessNumber = initializeGuessNumbers(state.history);
    state.nextGuessNumber = Number.isInteger(payload.nextGuessNumber) && payload.nextGuessNumber > 0
      ? Math.max(payload.nextGuessNumber, derivedNextGuessNumber)
      : derivedNextGuessNumber;
    sortHistoryItems(state.history);
    state.bestGuess = history[0] || null;
    state.latestGuess = latestGuess;
    return history.length > 0;
  } catch (error) {
    console.warn("Could not restore guess history", error);
    state.history = [];
    state.bestGuess = null;
    state.latestGuess = null;
    return false;
  }
}

function reconcilePersistedStateWithDate(gameDate) {
  const savedDate = state.gameDate;
  state.gameDate = gameDate;

  if (!savedDate || savedDate === gameDate) {
    return;
  }

  clearPersistedState();
  renderHistory();
  latestResult.className = "glass-panel result-spotlight empty";
  latestResult.innerHTML = '<h2>Current guess</h2><p>Percentile, closeness, cosine similarity, and rank will appear here after each guess.</p>';
  setFormMessage("New daily puzzle loaded.");
}


function formatSimilarity(value) {
  return value.toFixed(2);
}

function getGuessNumber(item) {
  const value = item ? Number(item.guess_number) : NaN;
  return Number.isInteger(value) && value > 0 ? value : null;
}

function initializeGuessNumbers(items) {
  let maxGuessNumber = 0;
  let hasExistingGuessNumber = false;

  items.forEach(function (item) {
    const guessNumber = getGuessNumber(item);
    if (guessNumber !== null) {
      hasExistingGuessNumber = true;
      maxGuessNumber = Math.max(maxGuessNumber, guessNumber);
    }
  });

  if (!hasExistingGuessNumber) {
    items.forEach(function (item, index) {
      item.guess_number = index + 1;
    });
    return items.length + 1;
  }

  items.forEach(function (item) {
    if (getGuessNumber(item) === null) {
      maxGuessNumber += 1;
      item.guess_number = maxGuessNumber;
    }
  });

  return maxGuessNumber + 1;
}

function sortHistoryItems(items) {
  items.sort(function (a, b) {
    const aHasRank = a.rank !== null && a.rank !== undefined;
    const bHasRank = b.rank !== null && b.rank !== undefined;

    if (aHasRank && bHasRank) {
      if (a.rank !== b.rank) {
        return a.rank - b.rank;
      }
      if (b.similarity !== a.similarity) {
        return b.similarity - a.similarity;
      }
      return a.guess.localeCompare(b.guess);
    }

    if (aHasRank) {
      return -1;
    }
    if (bHasRank) {
      return 1;
    }

    if (b.similarity !== a.similarity) {
      return b.similarity - a.similarity;
    }
    return a.guess.localeCompare(b.guess);
  });
}

function displayPercentileValue(resultOrValue, isCorrect) {
  if (typeof resultOrValue === "number") {
    return isCorrect ? resultOrValue : Math.min(resultOrValue, 99.9);
  }
  if (!resultOrValue) {
    return 0;
  }
  return resultOrValue.is_correct ? resultOrValue.similarity_percentile : Math.min(resultOrValue.similarity_percentile, 99.9);
}

function formatPercentile(value) {
  return value.toFixed(1) + "%";
}

function clampPercent(value) {
  return Math.max(0, Math.min(100, value));
}

function toneClass(percentile) {
  if (percentile >= 95) {
    return "tone-high";
  }
  if (percentile >= 80) {
    return "tone-mid";
  }
  if (percentile >= 15) {
    return "tone-low";
  }
  return "tone-vlow";
}

function messageToneClass(message) {
  if (message === "Very close" || message === "Close" || message === "Correct!") {
    return "tone-high";
  }
  if (message === "Warm") {
    return "tone-mid";
  }
  if (message === "Very far") {
    return "tone-vlow";
  }
  return "tone-low";
}

function rankLabel(result) {
  if (result.is_correct) {
    return "Solved";
  }
  if (result.is_top_100) {
    return "#" + result.rank;
  }
  return "Outside top 100";
}

function rankClass(result) {
  if (result.is_correct || result.is_top_100) {
    return "good";
  }
  return "muted";
}

function statusBadge(result) {
  if (result.is_correct) {
    return '<span class="result-badge hot">Target matched</span>';
  }
  if (result.is_top_100) {
    return '<span class="result-badge good">Top 100</span>';
  }
  return '<span class="result-badge muted">Still searching</span>';
}

function renderDaily(data) {
  dailyDate.textContent = data.date;
  if (structureLengthBadge) {
    structureLengthBadge.textContent = `Sequence length: ${data.protein_length} aa`;
  }
}

function renderHealth(data) {
  healthState.textContent = "Ready";
  healthMeta.textContent = `${data.proteins.toLocaleString()} reviewed proteins loaded locally.`;
  if (embeddingMeta) {
    embeddingMeta.innerHTML = `Similarity is computed from cosine similarity over ${data.embedding_shape[1]}D <a href="https://github.com/facebookresearch/esm" target="_blank" rel="noreferrer">ESM-2</a> embeddings sourced from the <a href="https://deepdrug-dpeb.s3.us-west-2.amazonaws.com/ESM-2/ProteinID_proteinSEQ_ESM_emb.csv" target="_blank" rel="noreferrer">DPEB aggregated release</a>.`;
  }
}

function forceTransparentStructureViewer() {
  if (!structureViewer) {
    return;
  }
  Array.from(structureViewer.querySelectorAll("canvas, div")).forEach(function (element) {
    element.style.backgroundColor = "transparent";
  });
}

function ensureThreeDMol() {
  if (window.$3Dmol) {
    return Promise.resolve(window.$3Dmol);
  }
  if (!threeDMolPromise) {
    threeDMolPromise = new Promise(function (resolve, reject) {
      let attempts = 0;
      const maxAttempts = 80;

      function check() {
        if (window.$3Dmol) {
          resolve(window.$3Dmol);
          return;
        }
        attempts += 1;
        if (attempts >= maxAttempts) {
          reject(new Error("3Dmol failed to load."));
          return;
        }
        window.setTimeout(check, 100);
      }

      check();
    });
  }
  return threeDMolPromise;
}

async function loadDailyStructure() {
  if (!structureViewer || !state.gameDate) {
    return;
  }

  structureViewer.innerHTML = "";

  try {
    const pdbText = await fetchJson(`/daily-structure?day=${encodeURIComponent(state.gameDate)}`, {
      timeoutMs: 25000,
    });
    if (typeof pdbText !== "string" || pdbText.trim() === "") {
      throw new Error("Structure unavailable.");
    }

    const $3Dmol = await ensureThreeDMol();
    structureViewerInstance = $3Dmol.createViewer(structureViewer, {
      backgroundColor: "white",
      backgroundAlpha: 0,
      alpha: true,
      premultipliedAlpha: false,
      antialias: true,
    });
    structureViewerInstance.setBackgroundColor(0x000000, 0);
    structureViewerInstance.addModel(pdbText, "pdb");
    structureViewerInstance.setStyle({}, { cartoon: { color: "spectrum" } });
    structureViewerInstance.zoomTo();
    structureViewerInstance.zoom(1.22);
    structureViewerInstance.render();
    forceTransparentStructureViewer();
  } catch (error) {
    console.error("Could not load daily structure", error);
    structureViewer.innerHTML = '<div class="structure-empty">No structure could be loaded for today.</div>';
  }
}

function buildWinChartMarkup(items) {
  if (items.length === 0) {
    return "";
  }

  const width = 280;
  const height = 170;
  const padding = { top: 16, right: 14, bottom: 48, left: 58 };
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const similarities = items.map(function (item) {
    return Math.min(item.similarity, 1);
  });
  const guessNumbers = items.map(function (item) {
    return getGuessNumber(item) || 1;
  });
  const minGuessNumber = Math.min.apply(null, guessNumbers);
  const maxGuessNumber = Math.max.apply(null, guessNumbers);

  let minSimilarity = Math.min.apply(null, similarities);
  const maxSimilarity = 1;
  if (minSimilarity === maxSimilarity) {
    minSimilarity -= 0.01;
  }

  function xPosition(guessNumber) {
    if (maxGuessNumber === minGuessNumber) {
      return width / 2;
    }
    return padding.left + ((guessNumber - minGuessNumber) / (maxGuessNumber - minGuessNumber)) * innerWidth;
  }

  function yPosition(value) {
    return padding.top + ((maxSimilarity - value) / (maxSimilarity - minSimilarity)) * innerHeight;
  }

  const points = items.map(function (item) {
    const guessNumber = getGuessNumber(item) || 1;
    return `${xPosition(guessNumber)},${yPosition(item.similarity)}`;
  }).join(" ");

  const gridLines = [0, 0.5, 1].map(function (fraction) {
    const y = padding.top + innerHeight * fraction;
    return `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="win-chart-grid"></line>`;
  }).join("");

  const axisLines = `
    <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" class="win-chart-axis"></line>
    <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" class="win-chart-axis"></line>
  `;

  const yLabels = [minSimilarity, 1].map(function (value) {
    const y = yPosition(value);
    return `<text x="${padding.left - 10}" y="${y + 4}" text-anchor="end" class="win-chart-label">${value.toFixed(2)}</text>`;
  }).join("");

  const labels = items.map(function (item, index) {
    const guessNumber = getGuessNumber(item) || 1;
    if (items.length > 8 && guessNumber !== minGuessNumber && guessNumber !== maxGuessNumber && index !== Math.floor(items.length / 2)) {
      return "";
    }
    const x = xPosition(guessNumber);
    return `<text x="${x}" y="${height - padding.bottom + 16}" text-anchor="middle" class="win-chart-label">${guessNumber}</text>`;
  }).join("");

  const dots = items.map(function (item, index) {
    const guessNumber = getGuessNumber(item) || 1;
    const x = xPosition(guessNumber);
    const y = yPosition(item.similarity);
    const isLast = index === items.length - 1;
    return `<circle cx="${x}" cy="${y}" r="${isLast ? 4.5 : 3.5}" class="win-chart-dot${isLast ? " is-last" : ""}"></circle>`;
  }).join("");

  return `
    <svg viewBox="0 0 ${width} ${height}" class="win-chart-svg" aria-hidden="true">
      ${gridLines}
      ${axisLines}
      <polyline points="${points}" class="win-chart-line"></polyline>
      ${dots}
      ${yLabels}
      ${labels}
      <text x="${padding.left + innerWidth / 2}" y="${height - 10}" text-anchor="middle" class="win-chart-axis-title">tries</text>
      <text x="18" y="${padding.top + innerHeight / 2}" text-anchor="middle" class="win-chart-axis-title" transform="rotate(-90 18 ${padding.top + innerHeight / 2})">cosine similarity</text>
    </svg>
  `;
}

function openWinModal(result) {
  if (!winModal) {
    return;
  }

  const orderedGuesses = state.history.slice().sort(function (a, b) {
    return (getGuessNumber(a) || 0) - (getGuessNumber(b) || 0);
  });
  const tries = state.history.length;

  winTitle.textContent = `Congrats, target protein found: ${result.guess}`;
  winSubtitle.textContent = result.name;
  winTries.textContent = String(tries);
  winChart.innerHTML = buildWinChartMarkup(orderedGuesses);
  winModal.hidden = false;
  document.body.classList.add("modal-open");
}

function closeWinModal() {
  if (!winModal) {
    return;
  }
  winModal.hidden = true;
  document.body.classList.remove("modal-open");
}

function renderLatest(result) {
  const displayPercentile = displayPercentileValue(result);
  const width = clampPercent(displayPercentile);
  const tone = messageToneClass(result.message);

  latestResult.classList.remove("empty");
  latestResult.innerHTML = `
    <div class="result-shell result-shell-focused">
      <div class="result-topline">
        <h2 class="card-title">Current guess</h2>
        <div class="result-badge-row">
          ${statusBadge(result)}
          <span class="result-badge ${rankClass(result)}">${rankLabel(result)}</span>
        </div>
      </div>
      <div class="result-main-text">
        <h2 class="result-protein-id mono">${result.guess}</h2>
        <p class="result-name">${result.name}</p>
      </div>
      <div class="result-summary-row">
        <div class="result-closeness result-closeness-${tone}">
          <span class="result-closeness-dot"></span>
          <span class="result-message">${result.message}</span>
        </div>
        <div class="result-percent-wrap">
          <span class="result-percent-inline">${formatPercentile(displayPercentile)}</span>
          <div class="result-bar compact"><span class="bar-fill ${tone}" style="width: ${width}%"></span></div>
        </div>
      </div>
      <div class="result-footnote">
        <span class="mono">UniProt ${result.protein_id}</span>
        <span>Cosine ${formatSimilarity(result.similarity)}</span>
        <span>${result.date}</span>
      </div>
    </div>
  `;
}

function renderBestGuess() {
  if (!bestGuessPanel) {
    return;
  }
  if (state.bestGuess === null) {
    bestGuessPanel.className = "best-guess empty";
    bestGuessPanel.innerHTML = "<strong>No guesses yet</strong><span>Start exploring the embedding space.</span>";
    return;
  }

  const best = state.bestGuess;
  bestGuessPanel.className = "best-guess";
  bestGuessPanel.innerHTML = `
    <div class="best-guess-card">
      <div class="best-guess-header">
        <div>
          <strong>${best.guess}</strong>
          <div class="best-guess-subline">${best.name}</div>
        </div>
        <span class="search-status ${rankClass(best)}">${rankLabel(best)}</span>
      </div>
      <div class="best-guess-metrics">
        <div class="metric-card"><span>Percentile</span><strong>${formatPercentile(displayPercentileValue(best))}</strong></div>
        <div class="metric-card"><span>Closeness</span><strong>${best.message}</strong></div>
        <div class="metric-card"><span>Cosine</span><strong>${formatSimilarity(best.similarity)}</strong></div>
        <div class="metric-card"><span>UniProt</span><strong>${best.protein_id}</strong></div>
      </div>
    </div>
  `;
}

function renderHistory() {
  if (state.history.length === 0) {
    historyBody.innerHTML = '<div class="history-empty">Your guesses will appear here.</div>';
    historySummary.textContent = "No guesses submitted yet.";
    return;
  }

  historySummary.textContent = `${state.history.length} guess${state.history.length === 1 ? "" : "es"} submitted.`;
  historyBody.innerHTML = state.history.map(function (item) {
    const displayPercentile = displayPercentileValue(item);
    const width = clampPercent(displayPercentile);
    const tone = toneClass(displayPercentile);
    return `
      <article class="history-item">
        <div class="history-turn">
          <strong class="history-turn-number">${getGuessNumber(item) || "—"}</strong>
        </div>
        <div class="history-status">
          <span class="history-rank ${rankClass(item)}">${rankLabel(item)}</span>
        </div>
        <div class="history-protein">
          <strong>${item.guess}</strong>
          <div class="history-name">${item.name}</div>
        </div>
        <div class="history-score">
          <strong class="history-percent">${formatPercentile(displayPercentile)}</strong>
          <div class="history-bar"><span class="bar-fill ${tone}" style="width: ${width}%"></span></div>
        </div>
        <div class="history-cosine">
          <div class="history-detail">${formatSimilarity(item.similarity)}</div>
        </div>
      </article>
    `;
  }).join("");
}

function updateHistory(result) {
  const existingIndex = state.history.findIndex(function (item) {
    return item.protein_id === result.protein_id;
  });
  let guessNumber = null;
  if (existingIndex >= 0) {
    guessNumber = getGuessNumber(state.history[existingIndex]);
    state.history.splice(existingIndex, 1);
  }
  if (guessNumber === null) {
    guessNumber = state.nextGuessNumber;
    state.nextGuessNumber += 1;
  }

  state.gameDate = result.date;
  result.guess_number = guessNumber;
  state.latestGuess = result;
  state.history.unshift(result);
  sortHistoryItems(state.history);
  state.bestGuess = state.history[0] || null;
  persistState();
  renderLatest(result);
  renderHistory();
  if (result.is_correct) {
    openWinModal(result);
  }
}

function renderSuggestions(items) {
  if (items.length === 0) {
    suggestionsBox.hidden = true;
    suggestionsBox.innerHTML = "";
    return;
  }

  suggestionsBox.hidden = false;
  suggestionsBox.innerHTML = items.map(function (item) {
    const value = item.gene_symbol || item.protein_id;
    return `
      <button type="button" class="suggestion-button" data-value="${value}">
        <strong>${value}</strong>
        <span>${item.name}</span>
      </button>
    `;
  }).join("");
}

async function loadDaily() {
  const data = await fetchJson("/daily", { timeoutMs: 10000 });
  renderDaily(data);
  return data;
}

async function loadHealth() {
  const data = await fetchJson("/health", { timeoutMs: 3000 });
  renderHealth(data);
}

async function submitGuess(rawGuess) {
  const guess = rawGuess.trim();
  if (guess === "") {
    setFormMessage("Enter a protein guess first.", "error");
    return;
  }

  if (state.suggestionTimerId !== null) {
    window.clearTimeout(state.suggestionTimerId);
    state.suggestionTimerId = null;
  }
  if (state.suggestionController) {
    state.suggestionController.abort();
    state.suggestionController = null;
  }
  state.latestSuggestionQuery = "";
  renderSuggestions([]);

  if (state.guessController) {
    state.guessController.abort();
  }

  state.activeGuessId += 1;
  const guessId = state.activeGuessId;
  state.guessController = new AbortController();
  state.isSubmittingGuess = true;

  const guessTimeoutMs = state.history.length === 0 ? 15000 : 7000;

  guessButton.disabled = true;
  setFormMessage("Scoring your guess...");
  try {
    const result = await fetchJson("/guess", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ guess: guess }),
      signal: state.guessController.signal,
      timeoutMs: guessTimeoutMs,
    });

    if (guessId !== state.activeGuessId) {
      return;
    }

    updateHistory(result);
    setFormMessage(result.is_correct ? "You found the target protein." : "Guess scored successfully.");
    guessInput.value = "";
    renderSuggestions([]);
  } catch (error) {
    if (guessId !== state.activeGuessId) {
      return;
    }

    if (error.name === "AbortError") {
      setFormMessage("Previous request cancelled. Try again.", "error");
      return;
    }

    console.error("Guess request failed", error);
    const detail = error.detail || error;
    const message = typeof detail === "string" ? detail : (detail.message || "Guess failed.");
    setFormMessage(message, "error");
    renderSuggestions(detail.suggestions || []);
  } finally {
    if (guessId === state.activeGuessId) {
      state.isSubmittingGuess = false;
      state.guessController = null;
      guessButton.disabled = false;
      guessInput.focus();
    }
  }
}

async function requestSuggestions(query) {
  const trimmed = query.trim();
  if (state.isSubmittingGuess) {
    return;
  }
  if (trimmed === "" || trimmed.length < 2) {
    if (state.suggestionController) {
      state.suggestionController.abort();
      state.suggestionController = null;
    }
    state.latestSuggestionQuery = "";
    renderSuggestions([]);
    return;
  }

  if (state.suggestionController) {
    state.suggestionController.abort();
  }

  state.latestSuggestionQuery = trimmed;
  state.suggestionController = new AbortController();
  try {
    const data = await fetchJson(`/autocomplete?q=${encodeURIComponent(trimmed)}&limit=6`, {
      signal: state.suggestionController.signal,
      timeoutMs: 5000,
    });
    if (state.latestSuggestionQuery !== trimmed || state.isSubmittingGuess) {
      return;
    }
    renderSuggestions(data.suggestions);
  } catch (error) {
    if (error.name === "AbortError") {
      return;
    }
    renderSuggestions([]);
  } finally {
    if (state.latestSuggestionQuery === trimmed) {
      state.suggestionController = null;
    }
  }
}

guessForm.addEventListener("submit", async function (event) {
  event.preventDefault();
  await submitGuess(guessInput.value);
});

guessInput.addEventListener("input", function (event) {
  const value = event.target.value;
  if (state.suggestionTimerId !== null) {
    window.clearTimeout(state.suggestionTimerId);
  }
  state.suggestionTimerId = window.setTimeout(function () {
    state.suggestionTimerId = null;
    requestSuggestions(value);
  }, 250);
});

document.addEventListener("click", function (event) {
  const suggestionButton = event.target.closest(".suggestion-button");
  if (suggestionButton) {
    guessInput.value = suggestionButton.dataset.value;
    renderSuggestions([]);
    guessInput.focus();
    return;
  }

  if (event.target.closest(".guess-panel") === null) {
    renderSuggestions([]);
  }

  if (event.target.closest("[data-win-dismiss]")) {
    closeWinModal();
  }
});

document.addEventListener("keydown", function (event) {
  if (event.key === "Escape" && winModal && !winModal.hidden) {
    closeWinModal();
  }
});

window.addEventListener("DOMContentLoaded", async function () {
  const restored = loadPersistedState();
  if (restored && state.latestGuess) {
    renderLatest(state.latestGuess);
    renderHistory();
    setFormMessage("Restored saved guesses.");
  } else {
    renderHistory();
  }

  loadHealth().catch(function () {
    healthState.textContent = "Unavailable";
    healthMeta.textContent = "Could not reach the backend.";
    if (embeddingMeta) {
      embeddingMeta.textContent = "Embedding metadata is unavailable until the backend responds.";
    }
  });

  try {
    const dailyData = await loadDaily();
    reconcilePersistedStateWithDate(dailyData.date);
    loadDailyStructure().catch(function () {
      structureViewer.innerHTML = '<div class="structure-empty">No structure could be loaded for today.</div>';
    });
    if (!restored || !state.latestGuess) {
      setFormMessage("Ready when you are.");
    }
  } catch (error) {
    if (!restored) {
      setFormMessage("The backend did not respond. Check the server logs.", "error");
    }
  }
});
