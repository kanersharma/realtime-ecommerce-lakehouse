// Catalog admin: list, search, filter, add and delete products. Seed products are read-only.
// Server-side rules (shop/main.py) are the real validation; the checks here only give faster feedback.
let items = [];
let options = null; // { categories, emoji: {category: [..]}, badges }
let category = "All";
let highlight = null;
const NAME = /^[\p{L}\p{N}_][\p{L}\p{N}_ .,'&()+\/-]*$/u; // mirrors NewProduct.name in main.py

// ------------------------------------------------ reviews (shared by the add and reviews dialogs)
// Realistic random reviews: ratings skew high (3.5-5.0), counts spread from ~20 to ~3,000.
function randomReviews() {
  const rating = Math.min(5, Math.round((3.5 + 1.5 * Math.sqrt(Math.random())) * 10) / 10);
  return { rating, reviews: Math.round(Math.exp(3 + Math.random() * 5)) };
}

function readReviews(f) {
  return { rating: Number(f.rating.value) || 0, reviews: Number(f.reviews.value) || 0 };
}

function fillReviews(f, r) {
  f.rating.value = r.reviews ? r.rating.toFixed(1) : "";
  f.reviews.value = r.reviews || "";
}

// ------------------------------------------------ stock (mirrors Restock / InventorySettings in main.py)
const LOW_STOCK = 5;

function stockCell(p) {
  if (p.on_hand <= 0) return '<span class="chip chip-out">SOLD OUT</span>';
  if (p.on_hand <= LOW_STOCK) return `<span class="chip chip-low">${p.on_hand} · low</span>`;
  return `<strong>${p.on_hand.toLocaleString()}</strong>`;
}

function wholeIn(v, lo, hi) {
  return Number.isInteger(v) && v >= lo && v <= hi;
}

function inventoryProblem({ lead_time_days, target_cover_days }) {
  if (lead_time_days !== null && !wholeIn(lead_time_days, 1, 60)) return "Lead time must be a whole number of days from 1 to 60.";
  if (!wholeIn(target_cover_days, 1, 90)) return "Target cover must be a whole number of days from 1 to 90.";
  return null;
}

// Mirrors check_reviews() in main.py.
function reviewsProblem({ rating, reviews }) {
  if (!Number.isInteger(reviews) || reviews < 0 || reviews > 100000) return "Number of reviews must be a whole number from 0 to 100,000.";
  if (Math.round(rating * 10) !== rating * 10) return "Rating can have at most 1 decimal.";
  if (reviews === 0 && rating !== 0) return "Add how many reviews there are, or clear the rating.";
  if (reviews > 0 && (rating < 1 || rating > 5)) return "Rating must be between 1.0 and 5.0 when there are reviews.";
  return null;
}

// ------------------------------------------------ list
function row(p) {
  const c = CATS[p.category];
  return `<tr data-id="${esc(p.id)}"${p.id === highlight ? ' class="flash"' : ""}>
    <td><span class="thumb" style="background:${c.tint}">${esc(p.emoji)}</span></td>
    <td class="mono">${esc(p.id)}</td>
    <td class="name"><strong>${esc(p.name)}</strong><span class="muted desc">${esc(p.description)}</span></td>
    <td><span class="chip" style="border-color:${c.color}">${esc(p.category)}</span></td>
    <td class="num"><strong>${money.format(p.price)}</strong></td>
    <td class="num nowrap">${stockCell(p)}</td>
    <td>${p.badge ? `<span class="badge-inline">${esc(p.badge.toUpperCase())}</span>` : '<span class="muted">–</span>'}</td>
    <td class="nowrap">${esc(ratingText(p))}</td>
    <td>${p.seed ? '<span class="muted">Seed</span>' : '<span class="chip chip-new">Added</span>'}</td>
    <td class="actions">
      <a class="btn small" href="/?q=${encodeURIComponent(p.name)}" aria-label="View ${esc(p.name)} in the store">View</a>
      <button class="btn small" data-stock="${esc(p.id)}" aria-label="Stock of ${esc(p.name)}">📦</button>
      ${p.seed ? "" : `<button class="btn small" data-rev="${esc(p.id)}" aria-label="Reviews of ${esc(p.name)}">⭐</button>`}
      ${p.seed ? "" : `<button class="btn small" data-del="${esc(p.id)}" aria-label="Delete ${esc(p.name)}">🗑️</button>`}
    </td>
  </tr>`;
}

function render() {
  const q = $("#search").value.trim().toLowerCase();
  const shown = items.filter((p) =>
    (category === "All" || p.category === category) &&
    (!q || `${p.id} ${p.name} ${p.category} ${p.description}`.toLowerCase().includes(q)));
  $("#rows").innerHTML = shown.map(row).join("");
  $("#empty").hidden = shown.length > 0;
  $("#list-title").textContent = q ? `Results for “${$("#search").value.trim()}”` : category === "All" ? "All products" : category;
  $("#list-count").textContent = `${shown.length} of ${items.length}`;
}

function renderStats() {
  const added = items.filter((p) => !p.seed);
  const prices = items.map((p) => p.price);
  $("#st-total").textContent = items.length;
  $("#st-split").textContent = `${items.length - added.length} seed · ${added.length} added by you`;
  $("#st-avg").textContent = money.format(prices.reduce((a, b) => a + b, 0) / (prices.length || 1));
  $("#st-range").textContent = `from ${money.format(Math.min(...prices))} to ${money.format(Math.max(...prices))}`;
  const units = items.reduce((a, p) => a + p.on_hand, 0);
  const out = items.filter((p) => p.on_hand <= 0).length;
  const low = items.filter((p) => p.on_hand > 0 && p.on_hand <= LOW_STOCK).length;
  $("#st-units").textContent = units.toLocaleString();
  $("#st-stock-split").textContent = `${out} sold out · ${low} low`;
  const newest = added[added.length - 1] || items[items.length - 1];
  $("#st-new-emoji").textContent = newest ? newest.emoji : "–";
  $("#st-new-name").textContent = newest ? `${newest.name} · ${newest.id}` : "";
}

function renderCategories() {
  const counts = {};
  items.forEach((p) => (counts[p.category] = (counts[p.category] || 0) + 1));
  const tiles = [["All", "🛍️", items.length], ...Object.keys(CATS).map((k) => [k, CATS[k].emoji, counts[k] || 0])];
  $("#cats").innerHTML = tiles.map(([name, e, n]) =>
    `<button class="cat" data-cat="${name}" aria-pressed="${name === category}"
       style="${name !== "All" ? `border-bottom: 6px solid ${CATS[name].color}` : ""}">
       <span class="e">${e}</span>${name}<span class="muted">${n}</span></button>`).join("");
}

async function load() {
  items = await api("/api/products");
  renderStats();
  renderCategories();
  render();
}

// ------------------------------------------------ add product
const form = () => $("#add-form");

function renderEmoji() {
  const cat = form().category.value;
  const current = form().querySelector('input[name="emoji"]:checked')?.value;
  const choices = options.emoji[cat];
  const pick = choices.includes(current) ? current : choices[0];
  $("#emoji-grid").innerHTML = choices.map((e) =>
    `<label class="emoji-choice" title="${esc(e)}">
       <input type="radio" name="emoji" value="${esc(e)}"${e === pick ? " checked" : ""} aria-label="${esc(e)}"><span>${esc(e)}</span>
     </label>`).join("");
}

function draft() {
  const f = form();
  return {
    name: f.name.value.trim(),
    category: f.category.value,
    price: Number(f.price.value),
    emoji: f.querySelector('input[name="emoji"]:checked')?.value,
    description: f.description.value.trim(),
    badge: f.badge.value || null,
    ...readReviews(f),
    stock: f.stock.value === "" ? 0 : Number(f.stock.value),
    lead_time_days: f.lead_time_days.value === "" ? null : Number(f.lead_time_days.value),
    target_cover_days: Number(f.target_cover_days.value),
  };
}

function renderPreview() {
  const d = draft();
  const c = CATS[d.category];
  $("#desc-count").textContent = `${form().description.value.length} / 400`;
  $("#preview").innerHTML = `
    <article class="tile product preview-tile">
      <div class="art" style="background:${c.tint}">${esc(d.emoji || "❔")}${d.badge ? `<span class="badge">${esc(d.badge.toUpperCase())}</span>` : ""}</div>
      <div class="body">
        <span class="stripe" style="background:${c.color}"></span>
        <h3>${esc(d.name || "Product name")}</h3>
        <p class="rating${d.reviews ? "" : " muted"}">${esc(ratingText(d))}</p>
        ${d.stock <= 0 ? '<p class="stock-low">Sold out</p>' : d.stock <= LOW_STOCK ? `<p class="stock-low">Only ${d.stock} left</p>` : ""}
        <p class="desc">${esc(d.description || "Description")}</p>
        <div class="foot"><p class="price">${d.price > 0 ? money.format(d.price) : "$—"}</p><span class="btn small">+ Add</span></div>
      </div>
    </article>`;
}

function problem(d) {
  if (d.name.length < 2) return "Give the product a name (at least 2 characters).";
  if (!NAME.test(d.name)) return "Names can use letters, numbers, spaces and . , ' & ( ) + / -";
  if (items.some((p) => p.name.toLowerCase() === d.name.toLowerCase())) return `A product called “${d.name}” already exists.`;
  if (!(d.price > 0) || d.price > 10000) return "Price must be between $0.01 and $10,000.";
  if (Math.round(d.price * 100) !== d.price * 100) return "Price can have at most 2 decimals.";
  if (d.description.length < 10) return "Description needs at least 10 characters.";
  if (!d.emoji) return "Pick a photo.";
  if (!wholeIn(d.stock, 0, 10000)) return "Starting stock must be a whole number from 0 to 10,000.";
  return inventoryProblem(d) || reviewsProblem(d);
}

function inventoryDefaults() {
  const f = form();
  f.lead_time_days.placeholder = options.lead_time_days[f.category.value]; // empty = the category's default
}

function openAdd() {
  form().reset();
  form().stock.value = options.starting_stock;
  form().target_cover_days.value = options.target_cover_days;
  inventoryDefaults();
  $("#add-error").hidden = true;
  renderEmoji();
  renderPreview();
  $("#add-dlg").showModal();
  form().name.focus();
}

async function save(e) {
  e.preventDefault();
  const d = draft();
  const err = $("#add-error");
  const why = problem(d);
  if (why) {
    err.textContent = why;
    err.hidden = false;
    return;
  }
  const btn = $("#save-btn");
  btn.disabled = true;
  try {
    const p = await api("/api/products", d);
    $("#add-dlg").close();
    highlight = p.id;
    category = "All";
    $("#search").value = "";
    await load();
    toast(`Added ${p.emoji} ${p.name} (${p.id})`);
    document.querySelector(`tr[data-id="${p.id}"]`)?.scrollIntoView({ block: "center" });
  } catch (ex) {
    err.textContent = ex.message;
    err.hidden = false;
  } finally {
    btn.disabled = false;
  }
}

// ------------------------------------------------ reviews of an existing (admin-added) product
let reviewing = null;
const revForm = () => $("#rev-form");

function renderReviewPreview() {
  $("#rev-preview").textContent = ratingText(readReviews(revForm()));
}

function openReviews(id) {
  reviewing = items.find((x) => x.id === id);
  $("#rev-product").textContent = `${reviewing.emoji} ${reviewing.name} · ${reviewing.id}`;
  $("#rev-error").hidden = true;
  fillReviews(revForm(), reviewing);
  renderReviewPreview();
  $("#rev-dlg").showModal();
}

async function saveReviews(e) {
  e.preventDefault();
  const r = readReviews(revForm());
  const why = reviewsProblem(r);
  if (why) {
    $("#rev-error").textContent = why;
    $("#rev-error").hidden = false;
    return;
  }
  try {
    const p = await api(`/api/products/${encodeURIComponent(reviewing.id)}/reviews`, r, "PUT");
    $("#rev-dlg").close();
    highlight = p.id;
    await load();
    toast(`Reviews saved: ${p.name} · ${ratingText(p)}`);
  } catch (ex) {
    $("#rev-error").textContent = ex.message;
    $("#rev-error").hidden = false;
  }
}

// ------------------------------------------------ stock of any product
let stocking = null;
const stockForm = () => $("#stock-form");

function openStock(id) {
  stocking = items.find((x) => x.id === id);
  const f = stockForm();
  f.reset();
  $("#stock-product").textContent = `${stocking.emoji} ${stocking.name} · ${stocking.id}`;
  $("#stock-now").textContent = stocking.on_hand.toLocaleString();
  f.lead_time_days.value = stocking.lead_time_days;
  f.target_cover_days.value = stocking.target_cover_days;
  $("#stock-error").hidden = true;
  $("#stock-dlg").showModal();
  f.quantity.focus();
}

async function saveStock(e) {
  e.preventDefault();
  const f = stockForm();
  const qty = f.quantity.value === "" ? 0 : Number(f.quantity.value);
  const settings = { lead_time_days: Number(f.lead_time_days.value), target_cover_days: Number(f.target_cover_days.value) };
  const why = !wholeIn(qty, 0, 10000) ? "Units to add must be a whole number from 0 to 10,000." : inventoryProblem(settings);
  if (why) {
    $("#stock-error").textContent = why;
    $("#stock-error").hidden = false;
    return;
  }
  const changed = settings.lead_time_days !== stocking.lead_time_days || settings.target_cover_days !== stocking.target_cover_days;
  try {
    const base = `/api/products/${encodeURIComponent(stocking.id)}`;
    if (qty > 0) await api(`${base}/restock`, { quantity: qty });
    if (changed) await api(`${base}/inventory`, settings, "PUT");
    $("#stock-dlg").close();
    highlight = stocking.id;
    await load();
    const p = items.find((x) => x.id === stocking.id);
    toast(qty > 0 ? `Restocked ${p.name}: +${qty} → ${p.on_hand} on hand` : changed ? `Saved reorder settings for ${p.name}` : "Nothing to change");
  } catch (ex) {
    $("#stock-error").textContent = ex.message;
    $("#stock-error").hidden = false;
  }
}

async function remove(id) {
  const p = items.find((x) => x.id === id);
  if (!confirm(`Delete ${p.name} (${p.id}) from the catalog? Past orders stay in the lakehouse.`)) return;
  try {
    await api(`/api/products/${encodeURIComponent(id)}`, null, "DELETE");
    toast(`Deleted ${p.name}`);
    await load();
  } catch (ex) {
    toast(ex.message);
  }
}

// ------------------------------------------------ wiring
$("#search").addEventListener("input", render);
$("#cats").addEventListener("click", (e) => {
  const b = e.target.closest("[data-cat]");
  if (!b) return;
  category = b.dataset.cat;
  renderCategories();
  render();
});
$("#rows").addEventListener("click", (e) => {
  const del = e.target.closest("[data-del]");
  if (del) remove(del.dataset.del);
  const rev = e.target.closest("[data-rev]");
  if (rev) openReviews(rev.dataset.rev);
  const stk = e.target.closest("[data-stock]");
  if (stk) openStock(stk.dataset.stock);
});
$("#add-btn").onclick = openAdd;
$("#add-category").addEventListener("change", () => { renderEmoji(); inventoryDefaults(); });
form().addEventListener("input", () => { $("#add-error").hidden = true; renderPreview(); });
form().addEventListener("change", renderPreview);
form().addEventListener("submit", save);
$("#add-random").onclick = () => { fillReviews(form(), randomReviews()); $("#add-error").hidden = true; renderPreview(); };
$("#add-no-reviews").onclick = () => { fillReviews(form(), { rating: 0, reviews: 0 }); renderPreview(); };
revForm().addEventListener("input", () => { $("#rev-error").hidden = true; renderReviewPreview(); });
stockForm().addEventListener("input", () => ($("#stock-error").hidden = true));
stockForm().addEventListener("submit", saveStock);
stockForm().addEventListener("click", (e) => {
  const b = e.target.closest("[data-plus]");
  if (!b) return;
  const f = stockForm();
  f.quantity.value = (Number(f.quantity.value) || 0) + Number(b.dataset.plus);
  $("#stock-error").hidden = true;
});
revForm().addEventListener("submit", saveReviews);
$("#rev-random").onclick = () => { fillReviews(revForm(), randomReviews()); $("#rev-error").hidden = true; renderReviewPreview(); };
$("#rev-clear").onclick = () => { fillReviews(revForm(), { rating: 0, reviews: 0 }); renderReviewPreview(); };
document.querySelectorAll("dialog").forEach((d) => d.addEventListener("click", (e) => { if (e.target === d) d.close(); }));

(async function init() {
  try {
    options = await api("/api/catalog/options");
    $("#add-category").innerHTML = options.categories.map((c) => `<option>${esc(c)}</option>`).join("");
    $("#add-badge").innerHTML += options.badges.map((b) => `<option>${esc(b)}</option>`).join("");
    await load();
  } catch (e) {
    $("#empty").textContent = e.message;
    $("#empty").hidden = false;
  }
})();
