(function () {
  const form = document.getElementById("search-form");
  const ingredientsEl = document.getElementById("ingredients");
  const cuisineEl = document.getElementById("cuisine");
  const mealEl = document.getElementById("meal_type");
  const statusEl = document.getElementById("status");
  const parsedEl = document.getElementById("parsed");
  const resultsEl = document.getElementById("results");
  const submitBtn = document.getElementById("submit-btn");

  async function loadFilters() {
    try {
      const r = await fetch("/api/filters");
      if (!r.ok) return;
      const data = await r.json();
      for (const c of data.cuisines || []) {
        const opt = document.createElement("option");
        opt.value = c;
        opt.textContent = c.charAt(0).toUpperCase() + c.slice(1);
        cuisineEl.appendChild(opt);
      }
      for (const m of data.meal_types || []) {
        const opt = document.createElement("option");
        opt.value = m;
        opt.textContent = m.charAt(0).toUpperCase() + m.slice(1);
        mealEl.appendChild(opt);
      }
    } catch (_) {
      /* filters optional */
    }
  }

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
      li.textContent = "No recipes match those filters. Try broader ingredients or clear cuisine/meal.";
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
      cuisine: cuisineEl.value || null,
      meal_type: mealEl.value || null,
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

  loadFilters();
})();
