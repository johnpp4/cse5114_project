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
  let currentLimit = INITIAL_PAGE_SIZE;
  let currentIngredients = "";

  // Keep hidden until we have a successful search response.
  loadMoreBtn.classList.add("hidden");

  function esc(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function renderResults(data) {
    resultsEl.innerHTML = "";
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
      const link = rec.search_link || rec.link || "#";
      const sourceLink =
        rec.link && rec.search_link && rec.link !== rec.search_link
          ? ' · <a href="' + esc(rec.link) + '" target="_blank" rel="noopener">Recipe</a>'
          : "";
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
        esc([rec.cuisine, rec.meal_type].filter(Boolean).join(" · ")) +
        (rec.rating != null ? " · ★ " + rec.rating : "") +
        sourceLink +
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

  async function fetchAndRender(limit) {
    statusEl.textContent = "Loading recipes...";
    statusEl.classList.remove("error");
    submitBtn.disabled = true;
    submitBtn.textContent = "Loading...";
    loadMoreBtn.disabled = true;
    loadMoreBtn.classList.add("hidden");
    recentStatusEl.textContent = "";

    const body = {
      ingredients: currentIngredients,
      limit,
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
      statusEl.textContent = data.results.length + " recipe(s) ranked by ingredient overlap.";
      renderResults(data);
      const hasMore = data.results.length >= limit && limit < MAX_LIMIT;
      if (hasMore) {
        loadMoreBtn.classList.remove("hidden");
      } else {
        loadMoreBtn.classList.add("hidden");
      }
    } catch (err) {
      statusEl.textContent = String(err.message || err);
      statusEl.classList.add("error");
      resultsEl.innerHTML = "";
      loadMoreBtn.classList.add("hidden");
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = "Find recipes";
      loadMoreBtn.disabled = false;
    }
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    currentIngredients = ingredientsEl.value;
    currentLimit = INITIAL_PAGE_SIZE;
    await fetchAndRender(currentLimit);
  });

  loadMoreBtn.addEventListener("click", async () => {
    currentLimit = Math.min(currentLimit + LOAD_MORE_STEP, MAX_LIMIT);
    await fetchAndRender(currentLimit);
  });

  // load recent recipes on page load
  async function loadRecentRecipes() {
      recentStatusEl.textContent = "Loading recent recipe feed...";
      try {
          const r = await fetch("/api/recent-recipes");
          const data = await r.json();
          if (data.results && data.results.length) {
              renderRecentRecipes(data.results);
              recentStatusEl.textContent = `Showing ${data.results.length} recent recipe(s).`;
          } else {
              recentStatusEl.textContent = "No recent recipes available.";
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

    for (const rec of recipes) {
        const li = document.createElement("li");
        li.className = "card";
        const link = rec.link
            ? (rec.link.startsWith("http") ? rec.link : "https://" + rec.link)
            : "#";
        li.innerHTML =
            '<div class="card-head">' +
            '<h3 class="card-title"><a href="' + esc(link) +
            '" target="_blank" rel="noopener">' + esc(rec.title) + "</a></h3>" +
            '<span class="badge source">' + esc(rec.source || "") + "</span></div>" +
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

    ws.onmessage = (e) => {
        const data = JSON.parse(e.data);
        if (data.type === "new_recipes") {
            const banner = document.createElement("div");
            banner.className = "new-recipe-banner";
            banner.textContent = `✨ ${data.recipes.length} new recipe(s) just added — refresh feed`;
            banner.onclick = () => {
                loadRecentRecipes();  // reload the recent feed
                banner.remove();
            };
            document.querySelector(".results-wrap").prepend(banner);
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
