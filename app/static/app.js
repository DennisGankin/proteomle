const state = {
  history: [],
  bestGuess: null,
  abortController: null,
  guessController: null,
  activeGuessId: 0,
};

const dailyDate = document.getElementById("daily-date");
const dailyLength = document.getElementById("daily-length");
const dailySize = document.getElementById("daily-size");
const dailyCategory = document.getElementById("daily-category");
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

function formatSimilarity(value) {
  return value.toFixed(4);
}

function renderDaily(data) {
  dailyDate.textContent = data.date;
  dailyLength.textContent = String(data.protein_length) + " aa";
  dailySize.textContent = data.dataset_size.toLocaleString();
  dailyCategory.textContent = data.category || "TBD";
}

function renderHealth(data) {
  healthState.textContent = "Ready";
  healthMeta.textContent = data.proteins.toLocaleString() + " proteins loaded · embeddings " + data.embedding_shape[1] + "D";
}

function renderLatest(result) {
  let rankText = "<span class=\"meta-pill danger\">Outside top 100</span>";
  if (result.is_correct) {
    rankText = "<span class=\"meta-pill good\">Solved the daily protein</span>";
  } else if (result.is_top_100) {
    rankText = "<span class=\"meta-pill good\">Top 100 · rank #" + result.rank + "</span>";
  }

  const correctness = result.is_correct ? "<span class=\"badge correct\">Correct</span>" : "";

  latestResult.classList.remove("empty");
  latestResult.innerHTML = ""
    + "<p class=\"result-kicker\">Latest result</p>"
    + "<h3>" + result.guess + " " + correctness + "</h3>"
    + "<p>" + result.name + "</p>"
    + "<div class=\"result-meta\">"
    +   "<span class=\"meta-pill hot\">Similarity " + formatSimilarity(result.similarity) + "</span>"
    +   "<span class=\"meta-pill\">" + result.message + "</span>"
    +   rankText
    + "</div>";
}

function renderBestGuess() {
  if (state.bestGuess === null) {
    bestGuessPanel.className = "best-guess empty";
    bestGuessPanel.innerHTML = "<strong>No guesses yet</strong><span>Start exploring the embedding space.</span>";
    return;
  }

  bestGuessPanel.className = "best-guess";
  bestGuessPanel.innerHTML = ""
    + "<strong>" + state.bestGuess.guess + "</strong>"
    + "<span>" + state.bestGuess.name + "</span>"
    + "<span>Similarity " + formatSimilarity(state.bestGuess.similarity) + " · " + state.bestGuess.message + "</span>";
}

function renderHistory() {
  if (state.history.length === 0) {
    historyBody.innerHTML = "<tr class=\"empty-row\"><td colspan=\"5\">Your guesses will appear here.</td></tr>";
    historySummary.textContent = "No guesses submitted yet.";
    return;
  }

  historySummary.textContent = String(state.history.length) + " guess" + (state.history.length === 1 ? "" : "es") + " submitted.";
  historyBody.innerHTML = state.history.map(function (item) {
    let rank = "—";
    if (item.is_correct) {
      rank = "<span class=\"badge correct\">Solved</span>";
    } else if (item.is_top_100) {
      rank = "<span class=\"badge\">#" + item.rank + "</span>";
    }

    return ""
      + "<tr>"
      +   "<td><strong>" + item.guess + "</strong></td>"
      +   "<td>" + item.name + "</td>"
      +   "<td>" + formatSimilarity(item.similarity) + "</td>"
      +   "<td>" + item.message + "</td>"
      +   "<td>" + rank + "</td>"
      + "</tr>";
  }).join("");
}

function updateHistory(result) {
  const existingIndex = state.history.findIndex(function (item) {
    return item.protein_id === result.protein_id;
  });
  if (existingIndex >= 0) {
    state.history.splice(existingIndex, 1);
  }

  state.history.unshift(result);
  state.history.sort(function (a, b) {
    if (b.similarity === a.similarity) {
      return a.guess.localeCompare(b.guess);
    }
    return b.similarity - a.similarity;
  });
  state.bestGuess = state.history[0] || null;
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
    return ""
      + "<button type=\"button\" class=\"suggestion-button\" data-value=\"" + value + "\">"
      +   "<strong>" + value + "</strong>"
      +   "<span>" + item.name + "</span>"
      + "</button>";
  }).join("");
}

async function loadDaily() {
  const data = await fetchJson("/daily", { timeoutMs: 10000 });
  renderDaily(data);
}

async function loadHealth() {
  const data = await fetchJson("/health", { timeoutMs: 10000 });
  renderHealth(data);
}

async function submitGuess(rawGuess) {
  const guess = rawGuess.trim();
  if (guess === "") {
    setFormMessage("Enter a protein guess first.", "error");
    return;
  }

  if (state.guessController) {
    state.guessController.abort();
  }

  state.activeGuessId += 1;
  const guessId = state.activeGuessId;
  state.guessController = new AbortController();

  guessButton.disabled = true;
  setFormMessage("Scoring your guess...");
  try {
    const result = await fetchJson("/guess", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ guess: guess }),
      signal: state.guessController.signal,
      timeoutMs: 30000,
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
      state.guessController = null;
      guessButton.disabled = false;
      guessInput.focus();
    }
  }
}

async function requestSuggestions(query) {
  if (state.abortController) {
    state.abortController.abort();
  }
  if (query.trim() === "") {
    renderSuggestions([]);
    return;
  }

  state.abortController = new AbortController();
  try {
    const data = await fetchJson("/autocomplete?q=" + encodeURIComponent(query) + "&limit=6", {
      signal: state.abortController.signal,
      timeoutMs: 5000,
    });
    renderSuggestions(data.suggestions);
  } catch (error) {
    if (error.name === "AbortError") {
      return;
    }
    renderSuggestions([]);
  }
}

guessForm.addEventListener("submit", async function (event) {
  event.preventDefault();
  await submitGuess(guessInput.value);
});

guessInput.addEventListener("input", async function (event) {
  await requestSuggestions(event.target.value);
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
  try {
    await Promise.all([loadDaily(), loadHealth()]);
    setFormMessage("Backend connected. Start guessing.");
  } catch (error) {
    healthState.textContent = "Unavailable";
    healthMeta.textContent = "Could not reach the backend.";
    setFormMessage("The backend did not respond. Check the server logs.", "error");
  }
  renderBestGuess();
  renderHistory();
});
