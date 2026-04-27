(function () {
  const form = document.getElementById("search-form");
  const ingredientsEl = document.getElementById("ingredients");
  const statusEl = document.getElementById("status");
  const recentStatusEl = document.getElementById("recent-status");
  const parsedEl = document.getElementById("parsed");
  const resultsEl = document.getElementById("results");
  const submitBtn = document.getElementById("submit-btn");
  const loadMoreBtn = document.getElementById("load-more-btn");
  const INITIAL_PAGE_SIZE = 30;
  const LOAD_MORE_STEP = 10;
  const MAX_LIMIT = 100;
  let loadedCount = 0;
  let currentIngredients = "";

  // Keep hidden until we have a successful search response.
  function setLoadMoreVisible(visible) {
    loadMoreBtn.classList.toggle("hidden", !visible);
    loadMoreBtn.style.display = visible ? "" : "none";
  }

  setLoadMoreVisible(false);

  function esc(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function sourceFromLink(link) {
    try {
      const url = new URL(link);
      return url.hostname.replace(/^www\./, "");
    } catch (_) {
      return "";
    }
  }

  function normalizedSourceLabel(source, link) {
    const raw = String(source || "").trim();
    if (raw && raw.toLowerCase() !== "source") {
      return raw;
    }
    const fromLink = sourceFromLink(link);
    return fromLink || "Unknown";
  }

  function renderResults(data, { append = false } = {}) {
    if (!append) {
      resultsEl.innerHTML = "";
    }
    if (data.query_parsed && data.query_parsed.length) {
      parsedEl.textContent = "Matched against: " + data.query_parsed.join(", ");
      parsedEl.classList.remove("hidden");
    } else {
      parsedEl.classList.add("hidden");
    }

    const items = data.results || [];
    if (!items.length) {
      const li = document.createElement("li");
      li.className = "empty";
      li.textContent = "No recipes match those ingredients. Try broader ingredients.";
      resultsEl.appendChild(li);
      return;
    }

    for (const rec of items) {
      const li = document.createElement("li");
      li.className = "card";
      const pct = Math.round(rec.match_score * 100);
      const link = rec.link || rec.search_link || "#";
      li.innerHTML =
        '<div class="card-head">' +
        '<h3 class="card-title"><a href="' +
        esc(link) +
        '" target="_blank" rel="noopener">' +
        esc(rec.title) +
        "</a></h3>" +
        '<span class="badge">' +
        pct +
        "% match</span></div>" +
        '<p class="meta">' +
        (rec.rating != null ? "★ " + rec.rating : "") +
        "</p>" +
        '<ul class="chips" id="chips-' +
        esc(rec.recipe_id) +
        '"></ul>';
      resultsEl.appendChild(li);
      const chips = li.querySelector('[id^="chips-"]');
      chips.removeAttribute("id");
      for (const name of rec.matched_ingredients || []) {
        const c = document.createElement("li");
        c.className = "chip ok";
        c.textContent = name;
        chips.appendChild(c);
      }
      for (const name of rec.missing_ingredients || []) {
        const c = document.createElement("li");
        c.className = "chip miss";
        c.textContent = "need: " + name;
        chips.appendChild(c);
      }
    }
  }

  async function fetchAndRender(limit, offset, { append = false } = {}) {
    statusEl.textContent = append ? "Loading more recipes..." : "Loading recipes...";
    statusEl.classList.remove("error");
    submitBtn.disabled = true;
    submitBtn.textContent = "Loading...";
    loadMoreBtn.disabled = true;
    setLoadMoreVisible(false);
    if (!append) {
      recentStatusEl.textContent = "";
      recentStatusEl.classList.remove("error");
    }

    const body = {
      ingredients: currentIngredients,
      limit,
      offset,
      min_score: 0,
    };

    try {
      const r = await fetch("/api/recommend", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await r.json();
      if (!r.ok) {
        let msg = r.statusText || "Request failed";
        if (data.detail) {
          msg = Array.isArray(data.detail)
            ? data.detail.map((d) => d.msg || JSON.stringify(d)).join("; ")
            : String(data.detail);
        }
        throw new Error(msg);
      }
      renderResults(data, { append });
      const resultCount = (data.results || []).length;
      loadedCount = offset + resultCount;
      statusEl.textContent = loadedCount + " recipe(s) ranked by ingredient overlap.";
      const hasMore = resultCount > 0 && Boolean(data.has_more) && loadedCount < MAX_LIMIT;
      setLoadMoreVisible(hasMore);
    } catch (err) {
      statusEl.textContent = String(err.message || err);
      statusEl.classList.add("error");
      resultsEl.innerHTML = "";
      setLoadMoreVisible(false);
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = "Find recipes";
      loadMoreBtn.disabled = false;
    }
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    currentIngredients = ingredientsEl.value;
    loadedCount = 0;
    await fetchAndRender(INITIAL_PAGE_SIZE, 0, { append: false });
  });

  loadMoreBtn.addEventListener("click", async () => {
    const remaining = Math.max(MAX_LIMIT - loadedCount, 0);
    const pageSize = Math.min(LOAD_MORE_STEP, remaining);
    if (pageSize <= 0) {
      setLoadMoreVisible(false);
      return;
    }
    await fetchAndRender(pageSize, loadedCount, { append: true });
  });

  async function loadRecentRecipes() {
    recentStatusEl.classList.remove("error");
    recentStatusEl.textContent = "Loading recent recipe feed...";
    setLoadMoreVisible(false);
    try {
      const r = await fetch("/api/recent-recipes");
      const data = await r.json();
      const recipes = data.results || [];
      renderRecentRecipes(recipes);
      if (recipes.length) {
        recentStatusEl.textContent = `Showing top ${recipes.length} recently uploaded recipe(s).`;
      } else {
        recentStatusEl.textContent = "No recent recipes available in Snowflake.";
      }
    } catch (err) {
      console.error("Failed to load recent recipes:", err);
      recentStatusEl.textContent = "Unable to load recent recipe feed.";
      recentStatusEl.classList.add("error");
    }
  }

  function renderRecentRecipes(recipes) {
    resultsEl.innerHTML = "";
    parsedEl.classList.add("hidden");
    setLoadMoreVisible(false);

    for (const rec of recipes) {
        const li = document.createElement("li");
        li.className = "card";
        const link = rec.link
            ? (rec.link.startsWith("http") ? rec.link : "https://" + rec.link)
            : "#";
        const sourceLabel = normalizedSourceLabel(rec.source, link);
        li.innerHTML =
            '<div class="card-head">' +
            '<h3 class="card-title"><a href="' + esc(link) +
            '" target="_blank" rel="noopener">' + esc(rec.title) + "</a></h3>" +
            '<span class="badge source">' + esc(sourceLabel) + "</span></div>" +
            '<ul class="chips"></ul>';
        resultsEl.appendChild(li);

        const chips = li.querySelector(".chips");
        for (const name of rec.ingredients || []) {
            const c = document.createElement("li");
            c.className = "chip";  // no ok/miss class — neutral grey
            c.textContent = name;
            chips.appendChild(c);
        }
    }
  }

  loadRecentRecipes();

  // WebSocket: listen for new recipes from Kafka
  function connectWebSocket() {
    const ws = new WebSocket(`ws://${location.host}/ws/new-recipes`);
    const liveBanner = document.createElement("div");
    liveBanner.className = "new-recipe-banner";
    liveBanner.style.display = "none";
    document.querySelector(".results-wrap").prepend(liveBanner);

    const recentTimestamps = []; // epoch-ms for each recipe received

    function updateLiveBanner() {
      const cutoff = Date.now() - 10_000;
      while (recentTimestamps.length && recentTimestamps[0] < cutoff) {
        recentTimestamps.shift();
      }
      const n = recentTimestamps.length;
      if (n === 0) {
        liveBanner.style.display = "none";
        liveBanner.onclick = null;
      } else {
        liveBanner.style.display = ""; 
        liveBanner.textContent = `New recipe${n !== 1 ? "s" : ""} just added — click to load`;
        liveBanner.onclick = () => {
          recentTimestamps.length = 0;
          liveBanner.style.display = "none";
          liveBanner.onclick = null;
          loadRecentRecipes();
        }
      }
    }
    // Tick every second so the count decays even without new messages
    setInterval(updateLiveBanner, 1_000);

    ws.onmessage = (e) => {
      const data = JSON.parse(e.data);
      if (data.type === "new_recipes") {
        const now = Date.now();
        data.recipes.forEach(() => recentTimestamps.push(now));
        updateLiveBanner();
      }
    };

    ws.onerror = () => {
      console.warn("WebSocket connection failed — live updates unavailable");
    };

    ws.onclose = () => {
      // reconnect after 5 seconds
      setTimeout(connectWebSocket, 5000);
    };
  }

  connectWebSocket()

})();
