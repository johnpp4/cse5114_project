const cities = [
    {
        title: "Manila",
        description: "Manila, the capital of the Philippines, is a bustling metropolis blending Spanish colonial history with modern city life. The city is known for its vibrant culture, historic Intramuros district, and the famous Manila Bay sunset.",
        image: "/images/manila_img.jpg",
        imageAlt: "Manila at night",
        latitude: 14.5995,
        longitude: 120.9842,
        recommendations: [
            "Binondo: world's oldest chinatown!",
            "Manila Ocean Park",
            "Venice Grand Canal Mall",
            "Mall of Asia",
            "Bonifacio Global City (BGC)"
        ]
    },
    {
        title: "Cebu",
        description: "Cebu City is the oldest city in the Philippines and the first Spanish settlement. It's a gateway to stunning beaches, diving spots, and the famous Sinulog Festival. It is also a major tradehub, the birthplace of Christianity in the Philippines, and features rich historical sites. ",
        image: "/images/cebu.jpg",
        imageAlt: "Cebu city",
        latitude: 10.3157,
        longitude: 123.8854,
        recommendations: [
            "Magellan's Cross",
            "Basilica del Santo Niño",
            "Taoist Temple",
            "Kawasan Falls",
            "Sinulog Festival"
        ]
    },
    {
        title: "Davao",
        description: "Davao is the largest city in the Philippines by land area. It's known for Mount Apo, the country's highest peak, durian, and the Philippine Eagle Center. It is a great city for nature lovers and those looking to experience the mix of indigenous culture and cuisines in the Philippines.",
        image: "/images/davao.jpg",
        imageAlt: "Davao city",
        latitude: 7.1907,
        longitude: 125.4553,
        recommendations: [
            "Philippine Eagle Center",
            "Mount Apo",
            "Davao Crocodile Park",
            "Eden Nature Park",
            "Bankerohan Market"
        ]
    },
    {
        title: "Boracay Island",
        description: "Boracay is a small island in central Philippines known for its white sandy beaches and crystal clear turquoise waters. It's a tropical paradise perfect for a relaxing vacation with diverse water activities and nightlife.",
        image: "/images/boracay.jpg",
        imageAlt: "Boracay island",
        latitude: 11.9674,
        longitude: 121.9248,
        recommendations: [
            "White Beach",
            "Bulabog Beach",
            "Puka Shell Beach",
            "Ariel's Point",
            "Sunset sailing"
        ]
    }
];

let currentIndex = 0;
const CITY_INDEX_STORAGE_KEY = "preferredCityIndex";
let isCityTransitioning = false;

async function fetchWeatherForCity(city) {
    const weatherEl = document.getElementById("city-weather");
    if (!weatherEl || city.latitude == null || city.longitude == null) {
        return;
    }

    weatherEl.textContent = "Current weather: Loading…";

    try {
        const response = await fetch(`https://cse2004.com/api/weather?latitude=${encodeURIComponent(city.latitude)}&longitude=${encodeURIComponent(city.longitude)}`);
        if (!response.ok) {
            throw new Error("Weather request failed");
        }
        const data = await response.json();
        const temperature = data?.temperature?.degrees;
        const condition = data?.weatherCondition?.description?.text;

        if (typeof temperature === "number" && condition) {
            weatherEl.textContent = `Current weather: ${temperature}°F, ${condition}`;
        } else {
            weatherEl.textContent = "Current weather: unavailable right now.";
        }
    } catch (error) {
        console.error("Error fetching weather:", error);
        weatherEl.textContent = "Current weather: unable to load right now.";
    }
}

async function showCity(index) {
    currentIndex = ((index % cities.length) + cities.length) % cities.length;
    const city = cities[currentIndex];
    document.getElementById("city-title").textContent = city.title;
    document.getElementById("city-description").textContent = city.description;
    const img = document.getElementById("city-image");
    img.src = city.image;
    img.alt = city.imageAlt;

    const listEl = document.getElementById("city-recommendations");
    listEl.innerHTML = "";
    (city.recommendations || []).forEach((item) => {
        const li = document.createElement("li");
        li.textContent = item;
        listEl.appendChild(li);
    });

    try {
        localStorage.setItem(CITY_INDEX_STORAGE_KEY, String(currentIndex));
    } catch (error) {
        console.warn("Unable to save preferred city to localStorage.", error);
    }

    await fetchWeatherForCity(city);
}

async function transitionCity(nextIndex) {
    if (isCityTransitioning) {
        return;
    }

    const cardEl = document.querySelector(".city-card");
    if (!cardEl) {
        await showCity(nextIndex);
        return;
    }

    isCityTransitioning = true;
    cardEl.classList.add("city-card-exit");

    setTimeout(async () => {
        await showCity(nextIndex);
        cardEl.classList.remove("city-card-exit");
        cardEl.classList.add("city-card-enter");
        requestAnimationFrame(() => {
            cardEl.classList.remove("city-card-enter");
        });
        setTimeout(() => {
            isCityTransitioning = false;
        }, 280);
    }, 180);
}

function setupCityNavigation() {
    const leftButton = document.querySelector(".arrow-left");
    const rightButton = document.querySelector(".arrow-right");

    if (leftButton) {
        leftButton.addEventListener("click", () => {
            transitionCity(currentIndex - 1);
        });
    }

    if (rightButton) {
        rightButton.addEventListener("click", () => {
            transitionCity(currentIndex + 1);
        });
    }
}

function setupFoodCards() {
    const cards = document.querySelectorAll(".food-card");
    if (!cards.length) return;

    const toggleCard = (card) => {
        card.classList.toggle("flipped");
    };

    cards.forEach((card) => {
        card.addEventListener("click", () => {
            toggleCard(card);
        });

        card.addEventListener("keydown", (event) => {
            if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                toggleCard(card);
            }
        });
    });
}

function setupSectionObserver() {
    const revealSections = document.querySelectorAll(".reveal-section");
    const populationSection = document.getElementById("population");

    if (!revealSections.length) {
        return;
    }

    if (!("IntersectionObserver" in window)) {
        revealSections.forEach((section) => {
            section.classList.add("visible");
        });
        animatePopulationCounters();
        return;
    }

    const observer = new IntersectionObserver(
        (entries, obs) => {
            entries.forEach((entry) => {
                if (entry.isIntersecting) {
                    entry.target.classList.add("visible");
                    if (populationSection && entry.target === populationSection) {
                        animatePopulationCounters();
                    }
                    obs.unobserve(entry.target);
                }
            });
        },
        {
            threshold: 0.2
        }
    );

    revealSections.forEach((section) => observer.observe(section));
}

function preparePopulationCounters() {
    const populationCells = document.querySelectorAll("#population tbody td:nth-child(2)");
    populationCells.forEach((cell) => {
        const cleaned = cell.textContent.replace(/,/g, "").trim();
        const targetValue = Number.parseInt(cleaned, 10);
        if (Number.isNaN(targetValue)) {
            return;
        }
        cell.dataset.targetValue = String(targetValue);
        cell.textContent = "0";
    });
}

let hasAnimatedPopulation = false;

function animatePopulationCounters() {
    if (hasAnimatedPopulation) {
        return;
    }

    const populationCells = document.querySelectorAll("#population tbody td:nth-child(2)");
    if (!populationCells.length) {
        return;
    }

    hasAnimatedPopulation = true;
    const durationMs = 2800;
    const start = performance.now();

    const step = (timestamp) => {
        const progress = Math.min((timestamp - start) / durationMs, 1);
        const easedProgress = 1 - Math.pow(1 - progress, 3);

        populationCells.forEach((cell) => {
            const targetValue = Number.parseInt(cell.dataset.targetValue || "", 10);
            if (Number.isNaN(targetValue)) {
                return;
            }
            const currentValue = Math.floor(targetValue * easedProgress);
            cell.textContent = currentValue.toLocaleString("en-US");
        });

        if (progress < 1) {
            requestAnimationFrame(step);
        }
    };

    requestAnimationFrame(step);
}

function setupQuestionForm() {
    const form = document.getElementById("qform");
    const questionInput = document.getElementById("question");

    if (!form || !questionInput) {
        return;
    }

    form.addEventListener("submit", (event) => {
        event.preventDefault();
        const value = questionInput.value.trim();
        if (value === "") {
            alert("Please enter a question before submitting.");
            return;
        }
        alert("Question submitted. Thank you!");
        form.reset();
    });
}

document.addEventListener("DOMContentLoaded", () => {
    requestAnimationFrame(() => {
        document.body.classList.add("page-loaded");
    });

    let initialIndex = 0;
    try {
        const stored = localStorage.getItem(CITY_INDEX_STORAGE_KEY);
        const parsed = stored != null ? Number.parseInt(stored, 10) : NaN;
        if (!Number.isNaN(parsed) && parsed >= 0 && parsed < cities.length) {
            initialIndex = parsed;
        }
    } catch (error) {
        console.warn("Unable to read preferred city from localStorage.", error);
    }

    showCity(initialIndex);
    setupCityNavigation();
    setupFoodCards();
    preparePopulationCounters();
    setupSectionObserver();
    setupQuestionForm();
});