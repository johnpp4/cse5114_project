(function () {
  const form = document.getElementById("search-form");
  const ingredientsEl = document.getElementById("ingredients");
  const statusEl = document.getElementById("status");
  const parsedEl = document.getElementById("parsed");
  const resultsEl = document.getElementById("results");
  const submitBtn = document.getElementById("submit-btn");

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
      const link = rec.link || "#";
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

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    statusEl.textContent = "";
    statusEl.classList.remove("error");
    submitBtn.disabled = true;

    const body = {
      ingredients: ingredientsEl.value,
      limit: 30,
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
    } catch (err) {
      statusEl.textContent = String(err.message || err);
      statusEl.classList.add("error");
      resultsEl.innerHTML = "";
    } finally {
      submitBtn.disabled = false;
    }
  });

})();
