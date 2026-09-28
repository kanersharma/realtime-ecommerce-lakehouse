// Shared by the store (app.js) and the catalog admin (admin.js).
const $ = (s) => document.querySelector(s);
const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

// Category tints for tiles; the stripe colors match the dashboard's category colors.
const CATS = {
  Beauty:      { tint: "#FFD6E8", color: "#E83E8C", emoji: "💄" },
  Books:       { tint: "#F3E3C7", color: "#8A5A00", emoji: "📚" },
  Electronics: { tint: "#D6E2FF", color: "#2F6BFF", emoji: "🔌" },
  Fashion:     { tint: "#FFE0C7", color: "#E8700A", emoji: "👗" },
  Home:        { tint: "#CDF2E0", color: "#128A5E", emoji: "🏠" },
  Sports:      { tint: "#D6EEFF", color: "#5FB7FF", emoji: "⚽" },
};

const OFFLINE = "Can't reach the Lakeshop server. Is it running? Start it with `docker compose up -d` and try again.";

// GET by default, POST when there's a body; pass method for DELETE etc.
async function api(path, body, method) {
  let res;
  try {
    res = await fetch(path, {
      method: method || (body ? "POST" : "GET"),
      headers: body ? { "Content-Type": "application/json" } : {},
      body: body ? JSON.stringify(body) : undefined,
    });
  } catch {
    throw new Error(OFFLINE); // network failure: fetch rejects with a bare "Failed to fetch"
  }
  if (res.status === 204) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const d = data.detail;
    throw new Error(Array.isArray(d) ? d.map((x) => x.msg).join("; ") : d || `Request failed (${res.status})`);
  }
  return data;
}

function toast(msg) {
  const t = $("#toast");
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => t.classList.remove("show"), 2200);
}

function stars(r) {
  const n = Math.max(0, Math.min(5, Math.round(r) || 0)); // clamp: a typed "6" must not throw
  return "★".repeat(n) + "☆".repeat(5 - n);
}

// New (admin-added) products have no reviews yet.
function ratingText(p) {
  return p.reviews ? `${stars(p.rating)}  ${Number(p.rating).toFixed(1)} · ${p.reviews.toLocaleString()}` : "No reviews yet";
}
