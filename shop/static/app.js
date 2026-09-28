// Lakeshop front end. Every meaningful action becomes a pipeline event via the shop API:
//   open a product -> page_view, add to cart -> add_to_cart, pay -> one order per cart line.
// Shared helpers ($, money, esc, CATS, api, toast, ratingText) live in common.js.
const FEATURED = "P021";
const LOW_STOCK = 5; // "Only N left" at or below this
const maxQty = (p) => Math.max(0, Math.min(10, p.on_hand)); // per product in the cart

// Anonymous ids: the user id persists in this browser, the session id lasts for the tab session.
// crypto.getRandomValues works in every context; crypto.randomUUID only in secure ones (https, localhost),
// so over http://shop:8000 or a LAN IP the store would not even start.
const newId = (prefix) =>
  prefix + Array.from(crypto.getRandomValues(new Uint8Array(8)), (b) => b.toString(16).padStart(2, "0")).join("");
const store = (area, key, make) => area.getItem(key) || (area.setItem(key, make()), area.getItem(key));
const USER = store(localStorage, "ls_user", () => newId("W-"));
const SESSION = store(sessionStorage, "ls_session", () => newId("S-"));

let products = [];
let byId = {};
let category = "All";
let cart = JSON.parse(localStorage.getItem("ls_cart") || "{}"); // { productId: qty }
let sent = Number(sessionStorage.getItem("ls_sent") || 0);

// ------------------------------------------------ events
async function track(event_type, product_id) {
  try {
    await api("/api/events", { event_type, product_id, user_id: USER, session_id: SESSION });
    bumpEvents(1, `${event_type} · ${byId[product_id].name}`);
  } catch (e) {
    console.warn("event not sent", e);
  }
}

function bumpEvents(n, label) {
  sent += n;
  sessionStorage.setItem("ls_sent", sent);
  $("#evt-count").textContent = sent;
  if (label) $("#evt-last").innerHTML = `Last: <code>${esc(label)}</code>`;
}

// ------------------------------------------------ catalog + search
function productTile(p) {
  const c = CATS[p.category];
  const wide = p.badge === "Bestseller";
  const out = p.on_hand <= 0;
  const low = !out && p.on_hand <= LOW_STOCK;
  return `
    <article class="tile product${wide ? " wide" : ""}${out ? " soldout" : ""}" data-id="${p.id}" tabindex="0" role="button"
             aria-label="${esc(p.name)}, ${money.format(p.price)}${out ? ", sold out" : ""}">
      <div class="art" style="background:${c.tint}">${esc(p.emoji)}${p.badge ? `<span class="badge">${esc(p.badge.toUpperCase())}</span>` : ""}${out ? '<span class="soldout-tag">SOLD OUT</span>' : ""}</div>
      <div class="body">
        <span class="stripe" style="background:${c.color}" title="${p.category}"></span>
        <h3>${esc(p.name)}</h3>
        <p class="rating">${esc(ratingText(p))}</p>
        ${low ? `<p class="stock-low">Only ${p.on_hand} left</p>` : ""}
        ${wide ? `<p class="desc">${esc(p.description)}</p>` : ""}
        <div class="foot">
          <p class="price">${money.format(p.price)}</p>
          <button class="btn small" data-add="${p.id}" aria-label="Add ${esc(p.name)} to cart"${out ? " disabled" : ""}>${out ? "Sold out" : "+ Add"}</button>
        </div>
      </div>
    </article>`;
}

function render() {
  const q = $("#search").value.trim().toLowerCase();
  const shown = products.filter((p) =>
    (category === "All" || p.category === category) &&
    (!q || `${p.name} ${p.category} ${p.description}`.toLowerCase().includes(q)));
  $("#products").innerHTML = shown.map(productTile).join("");
  $("#empty").hidden = shown.length > 0;
  $("#results-title").textContent = q ? `Results for “${$("#search").value.trim()}”` : category === "All" ? "All products" : category;
  $("#results-count").textContent = `${shown.length} item${shown.length === 1 ? "" : "s"}`;
}

function renderCategories() {
  const counts = {};
  products.forEach((p) => (counts[p.category] = (counts[p.category] || 0) + 1));
  const tiles = [["All", "🛍️", products.length], ...Object.keys(CATS).map((k) => [k, CATS[k].emoji, counts[k] || 0])];
  $("#cats").innerHTML = tiles.map(([name, e, n]) =>
    `<button class="cat" data-cat="${name}" aria-pressed="${name === category}"
       style="${name !== "All" ? `border-bottom: 6px solid ${CATS[name].color}` : ""}">
       <span class="e">${e}</span>${name}<span class="muted">${n}</span></button>`).join("");
}

// ------------------------------------------------ product dialog
let current = null;
function openProduct(id) {
  const p = byId[id];
  current = p;
  $("#pd-art").textContent = p.emoji;
  $("#pd-art").style.background = CATS[p.category].tint;
  $("#pd-cat").textContent = p.category.toUpperCase();
  $("#pd-name").textContent = p.name;
  $("#pd-rating").textContent = p.reviews ? `${ratingText(p)} reviews` : ratingText(p);
  $("#pd-desc").textContent = p.description;
  $("#pd-price").textContent = money.format(p.price);
  const out = p.on_hand <= 0;
  $("#pd-stock").textContent = out ? "Sold out" : p.on_hand <= LOW_STOCK ? `Only ${p.on_hand} left` : "In stock";
  $("#pd-stock").className = `stock${out ? " out" : p.on_hand <= LOW_STOCK ? " low" : ""}`;
  $("#pd-add").disabled = out;
  $("#pd-qty").value = out ? 0 : 1;
  $("#product-dlg").showModal();
  track("page_view", id);
}

// ------------------------------------------------ cart
function saveCart() {
  localStorage.setItem("ls_cart", JSON.stringify(cart));
  const n = Object.values(cart).reduce((a, b) => a + b, 0);
  $("#cart-count").textContent = n;
  renderCart();
}

function addToCart(id, qty = 1) {
  const p = byId[id];
  const room = maxQty(p) - (cart[id] || 0);
  if (room <= 0) {
    toast(p.on_hand <= 0 ? `${p.name} is sold out` : `Only ${p.on_hand} left, all in your cart`);
    return;
  }
  const added = Math.min(qty, room);
  cart[id] = (cart[id] || 0) + added;
  saveCart();
  toast(added < qty ? `Only ${p.on_hand} left: added ${added}` : `Added ${p.emoji} ${p.name}`);
  track("add_to_cart", id);
}

// Stock changes with every order (ours and other shoppers'), so re-read it after checkout attempts.
async function refreshProducts() {
  try {
    products = await api("/api/products");
  } catch {
    return;
  }
  byId = Object.fromEntries(products.map((p) => [p.id, p]));
  renderCategories();
  render();
  renderCart();
}

const subtotal = () => Object.entries(cart).reduce((s, [id, q]) => s + byId[id].price * q, 0);

function renderCart() {
  const lines = Object.entries(cart).filter(([id]) => byId[id]);
  $("#cart-lines").innerHTML = lines.length ? lines.map(([id, q]) => {
    const p = byId[id];
    return `<div class="line">
      <span class="e" style="background:${CATS[p.category].tint}">${p.emoji}</span>
      <div class="t"><strong>${esc(p.name)}</strong><span class="muted">${money.format(p.price)}</span>
        ${q > p.on_hand ? `<span class="stock-low">${p.on_hand <= 0 ? "Sold out" : `Only ${p.on_hand} left`}</span>` : ""}</div>
      <div class="stepper" role="group" aria-label="Quantity of ${esc(p.name)}">
        <button type="button" data-dec="${id}" aria-label="Decrease">−</button><output>${q}</output><button type="button" data-inc="${id}" aria-label="Increase">+</button>
      </div>
      <button class="rm" data-rm="${id}" aria-label="Remove ${esc(p.name)}">🗑️</button>
    </div>`;
  }).join("") : `<p class="muted">Your cart is empty. Go find something shiny.</p>`;
  $("#cart-subtotal").textContent = money.format(subtotal());
  $("#to-checkout").disabled = lines.length === 0;
}

// ------------------------------------------------ checkout
function syncMethod() {
  const m = new FormData($("#co-form")).get("method");
  $(".card-fields").hidden = m !== "card";
  $(".upi-fields").hidden = m !== "upi";
  $(".cod-fields").hidden = m !== "cod";
}

function openCheckout() {
  $("#cart-dlg").close();
  $("#co-form").hidden = false;
  $("#co-success").hidden = true;
  $("#co-error").hidden = true;
  $("#pay-btn").textContent = `Pay ${money.format(subtotal())}`;
  syncMethod();
  $("#checkout-dlg").showModal();
}

async function pay(e) {
  e.preventDefault();
  const f = new FormData($("#co-form"));
  const err = $("#co-error");
  err.hidden = true;
  if (!f.get("name").trim()) { err.textContent = "Please enter your name."; err.hidden = false; return; }
  const btn = $("#pay-btn");
  btn.disabled = true;
  btn.textContent = "Processing…";
  try {
    const items = Object.entries(cart).map(([product_id, quantity]) => ({ product_id, quantity }));
    const res = await api("/api/checkout", {
      user_id: USER, session_id: SESSION, name: f.get("name").trim(), country: f.get("country"), items,
      payment: { method: f.get("method"), card_number: f.get("demo_card"), expiry: f.get("demo_exp"),
                 cvc: f.get("demo_cvc"), upi_id: f.get("upi") },
    });
    bumpEvents(res.lines, `${res.lines} order event${res.lines > 1 ? "s" : ""} · ${money.format(res.total)}`);
    $("#co-summary").innerHTML = `<strong>${esc(res.order_ref)}</strong> · ${money.format(res.total)} · ${esc(res.paid_with)}`;
    cart = {};
    saveCart();
    $("#co-form").reset();
    $("#co-form").hidden = true;
    $("#co-success").hidden = false;
  } catch (ex) {
    err.textContent = ex.message;
    err.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = `Pay ${money.format(subtotal())}`;
    refreshProducts(); // shows the new stock (or why a line didn't fit)
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
$("#products").addEventListener("click", (e) => {
  const add = e.target.closest("[data-add]");
  if (add) return addToCart(add.dataset.add);
  const tile = e.target.closest("[data-id]");
  if (tile) openProduct(tile.dataset.id);
});
$("#products").addEventListener("keydown", (e) => {
  const tile = e.target.closest("[data-id]");
  if (tile && e.target === tile && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); openProduct(tile.dataset.id); }
});
$("#pd-minus").onclick = () => ($("#pd-qty").value = Math.max(1, +$("#pd-qty").value - 1));
$("#pd-plus").onclick = () => ($("#pd-qty").value = Math.min(maxQty(current), +$("#pd-qty").value + 1));
$("#pd-add").onclick = () => { addToCart(current.id, +$("#pd-qty").value); $("#product-dlg").close(); };
$("#cart-btn").onclick = () => { renderCart(); $("#cart-dlg").showModal(); };
$("#cart-lines").addEventListener("click", (e) => {
  const t = e.target.closest("button");
  if (!t) return;
  const { inc, dec, rm } = t.dataset;
  if (inc) cart[inc] = Math.min(cart[inc] + 1, maxQty(byId[inc]));
  if (dec) cart[dec] > 1 ? cart[dec]-- : delete cart[dec];
  if (rm) delete cart[rm];
  saveCart();
});
$("#to-checkout").onclick = openCheckout;
$("#co-form").addEventListener("change", syncMethod);
// a stale payment error disappears as soon as the shopper edits the form
$("#co-form").addEventListener("input", () => ($("#co-error").hidden = true));
$("#co-form").addEventListener("submit", pay);
$("#fill-test").onclick = () => {
  const f = $("#co-form");
  f.demo_card.value = "4242 4242 4242 4242";
  f.demo_exp.value = "12/30";
  f.demo_cvc.value = "123";
  $("#co-error").hidden = true;
};
// close dialogs when the backdrop is clicked
document.querySelectorAll("dialog").forEach((d) => d.addEventListener("click", (e) => { if (e.target === d) d.close(); }));

// ------------------------------------------------ boot
(async function init() {
  try {
    products = await api("/api/products");
  } catch (e) {
    $("#empty").textContent = e.message;
    $("#empty").hidden = false;
    return;
  }
  byId = Object.fromEntries(products.map((p) => [p.id, p]));
  cart = Object.fromEntries(Object.entries(cart).filter(([id]) => byId[id]));
  const f = byId[FEATURED];
  $("#hero-product").innerHTML = `${f.emoji}<span class="tag">${esc(f.name)} · ${money.format(f.price)}</span>`;
  $("#hero-product").onclick = $("#hero-cta").onclick = () => openProduct(FEATURED);
  $("#evt-count").textContent = sent;
  const q = new URLSearchParams(location.search).get("q"); // deep link from the catalog admin
  if (q) $("#search").value = q;
  renderCategories();
  render();
  saveCart();
})();
