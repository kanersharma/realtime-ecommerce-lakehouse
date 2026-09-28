// Catalog admin: list, search, filter, add and delete products. Seed products are read-only.
// Server-side rules (shop/main.py) are the real validation; the checks here only give faster feedback.
let items = [];
let options = null; // { categories, emoji: {category: [..]}, badges }
let category = "All";
let highlight = null;
const NAME = /^[\p{L}\p{N}_][\p{L}\p{N}_ .,'&()+\/-]*$/u; // mirrors NewProduct.name in main.py

// ------------------------------------------------ list
function row(p) {
  const c = CATS[p.category];
  return `<tr data-id="${esc(p.id)}"${p.id === highlight ? ' class="flash"' : ""}>
    <td><span class="thumb" style="background:${c.tint}">${esc(p.emoji)}</span></td>
    <td class="mono">${esc(p.id)}</td>
    <td class="name"><strong>${esc(p.name)}</strong><span class="muted desc">${esc(p.description)}</span></td>
    <td><span class="chip" style="border-color:${c.color}">${esc(p.category)}</span></td>
    <td class="num"><strong>${money.format(p.price)}</strong></td>
    <td>${p.badge ? `<span class="badge-inline">${esc(p.badge.toUpperCase())}</span>` : '<span class="muted">–</span>'}</td>
    <td class="nowrap">${esc(ratingText(p))}</td>
    <td>${p.seed ? '<span class="muted">Seed</span>' : '<span class="chip chip-new">Added</span>'}</td>
    <td class="actions">
      <a class="btn small" href="/?q=${encodeURIComponent(p.name)}" aria-label="View ${esc(p.name)} in the store">View</a>
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
        <p class="rating muted">No reviews yet</p>
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
  return null;
}

function openAdd() {
  form().reset();
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
});
$("#add-btn").onclick = openAdd;
$("#add-category").addEventListener("change", renderEmoji);
form().addEventListener("input", () => { $("#add-error").hidden = true; renderPreview(); });
form().addEventListener("change", renderPreview);
form().addEventListener("submit", save);
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
