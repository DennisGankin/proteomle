const state = {
  history: [],
  bestGuess: null,
  latestGuess: null,
  gameDate: null,
  suggestionController: null,
  suggestionTimerId: null,
  latestSuggestionQuery: "",
  guessController: null,
  activeGuessId: 0,
  isSubmittingGuess: false,
};

const STORAGE_KEY = "protl-guess-state";

const dailyDate = document.getElementById("daily-date");
const dailyLength = document.getElementById("daily-length");
const guessForm = document.getElementById("guess-form");
const guessInput = document.getElementById("guess-input");
const guessButton = document.getElementById("guess-button");
const suggestionsBox = document.getElementById("suggestions");
const formMessage = document.getElementById("form-message");
const latestResult = document.getElementById("latest-result");
const bestGuessPanel = document.getElementById("best-guess");
const healthState = document.getElementById("health-state");
const healthMeta = document.getElementById("health-meta");
const historyBody = document.getElementById("history-body");
const historySummary = document.getElementById("history-summary");

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
  renderBestGuess();
  renderHistory();
  latestResult.className = "glass-panel result-spotlight empty";
  latestResult.innerHTML = '<h2>Current guess</h2><p>Percentile, closeness, cosine similarity, and top-100 rank will show up here.</p>';
  setFormMessage("New daily puzzle loaded.");
}


function formatSimilarity(value) {
  return value.toFixed(4);
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
  dailyLength.textContent = String(data.protein_length) + " aa";
  state.gameDate = data.date;
}

function renderHealth(data) {
  healthState.textContent = "Ready";
  healthMeta.textContent = `${data.proteins.toLocaleString()} proteins loaded · embeddings ${data.embedding_shape[1]}D`;
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
        <div class="history-main">
          <span class="history-rank ${rankClass(item)}">${rankLabel(item)}</span>
          <div>
            <strong>${item.guess}</strong>
            <div class="history-name">${item.name}</div>
          </div>
        </div>
        <div class="history-metrics">
          <div class="history-score-row">
            <span class="history-meta">Similarity percentile</span>
            <strong class="history-percent">${formatPercentile(displayPercentile)}</strong>
          </div>
          <div class="history-bar"><span class="bar-fill ${tone}" style="width: ${width}%"></span></div>
          <div class="history-detail">Cosine similarity ${formatSimilarity(item.similarity)}</div>
        </div>
      </article>
    `;
  }).join("");
}

function updateHistory(result) {
  const existingIndex = state.history.findIndex(function (item) {
    return item.protein_id === result.protein_id;
  });
  if (existingIndex >= 0) {
    state.history.splice(existingIndex, 1);
  }

  state.gameDate = result.date;
  state.latestGuess = result;
  state.history.unshift(result);
  state.history.sort(function (a, b) {
    if (b.similarity_percentile === a.similarity_percentile) {
      return a.guess.localeCompare(b.guess);
    }
    return b.similarity_percentile - a.similarity_percentile;
  });
  state.bestGuess = state.history[0] || null;
  persistState();
  renderLatest(result);
  renderBestGuess();
  renderHistory();
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

  guessButton.disabled = true;
  setFormMessage("Scoring your guess...");
  try {
    const result = await fetchJson("/guess", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ guess: guess }),
      signal: state.guessController.signal,
      timeoutMs: 5000,
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
});

window.addEventListener("DOMContentLoaded", async function () {
  const restored = loadPersistedState();
  if (restored && state.latestGuess) {
    renderLatest(state.latestGuess);
    renderBestGuess();
    renderHistory();
    setFormMessage("Restored saved guesses.");
  } else {
    renderBestGuess();
    renderHistory();
  }

  loadHealth().catch(function () {
    healthState.textContent = "Unavailable";
    healthMeta.textContent = "Could not reach the backend.";
  });

  try {
    await loadDaily();
    reconcilePersistedStateWithDate(state.gameDate);
    if (!restored || !state.latestGuess) {
      setFormMessage("Backend connected. Start guessing.");
    }
  } catch (error) {
    if (!restored) {
      setFormMessage("The backend did not respond. Check the server logs.", "error");
    }
  }
});
