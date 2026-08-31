(function () {
  "use strict";

  const data = window.PH_CATALOG_DATA;
  const tabs = [...document.querySelectorAll("[data-tab]")];
  const pages = [...document.querySelectorAll("[data-page]")];
  const productList = document.getElementById("product-list");
  const productDetail = document.getElementById("product-detail");
  const search = document.getElementById("product-search");
  const empty = document.getElementById("empty-products");
  let selectedSlug = data.products[0].slug;

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function activatePage(name) {
    tabs.forEach((tab) => tab.classList.toggle("is-active", tab.dataset.tab === name));
    pages.forEach((page) => {
      const active = page.dataset.page === name;
      page.hidden = !active;
      page.classList.toggle("is-active", active);
    });
    window.history.replaceState(null, "", `#${name}`);
    window.scrollTo({ top: 0, behavior: "auto" });
  }

  function renderBars() {
    const root = document.getElementById("label-bars");
    const max = Math.max(...data.labelCounts.map((entry) => entry[1]));
    root.innerHTML = data.labelCounts
      .map(
        ([label, count]) => `
          <div class="label-row">
            <span class="label-name">${escapeHtml(label)}</span>
            <div class="bar-track" aria-hidden="true"><div class="bar-value" style="width:${(
              (count / max) *
              100
            ).toFixed(1)}%"></div></div>
            <span class="label-count">${count.toLocaleString()}</span>
          </div>`
      )
      .join("");
  }

  function renderProductDetail(product) {
    const sourceChips = product.sourceCategories.length
      ? product.sourceCategories
          .map((category) => `<span class="chip source">${escapeHtml(category)}</span>`)
          .join("")
      : '<span class="chip source">No Product Hunt category</span>';
    const labelChips = product.labels
      .map((label) => `<span class="chip">${escapeHtml(label)}</span>`)
      .join("");
    productDetail.innerHTML = `
      <div class="detail-topline">
        <span class="detail-kicker">Provisional primary · ${escapeHtml(product.primaryCategory)}</span>
        <span class="detail-score">${Math.round(product.primaryScore * 100)}% classifier score</span>
      </div>
      <h2>${escapeHtml(product.name)}</h2>
      <p class="detail-tagline">${escapeHtml(product.tagline)}</p>
      <p class="detail-description">${escapeHtml(product.description)}</p>
      <dl class="detail-facts">
        <div><dt>Product ID</dt><dd>${product.productId.toLocaleString()}</dd></div>
        <div><dt>Launch relationships</dt><dd>${product.launchCount.toLocaleString()}</dd></div>
        <div><dt>Post ID span</dt><dd>${product.firstPostId ? `${product.firstPostId.toLocaleString()}–${product.latestPostId.toLocaleString()}` : "None"}</dd></div>
      </dl>
      <span class="detail-score">Derived labels</span>
      <div class="chip-group">${labelChips}</div>
      <span class="detail-score">Original Product Hunt categories</span>
      <div class="chip-group">${sourceChips}</div>
      <div class="detail-links">
        <a href="${escapeHtml(product.websiteUrl)}" target="_blank" rel="noopener noreferrer">Visit website</a>
        <a class="secondary" href="${escapeHtml(product.producthuntUrl)}" target="_blank" rel="noopener noreferrer">Product Hunt</a>
      </div>`;
  }

  function renderProducts(query = "") {
    const normalized = query.trim().toLowerCase();
    const filtered = data.products.filter((product) =>
      [
        product.name,
        product.tagline,
        product.primaryCategory,
        ...product.labels,
        ...product.sourceCategories
      ]
        .join(" ")
        .toLowerCase()
        .includes(normalized)
    );
    if (!filtered.some((product) => product.slug === selectedSlug) && filtered.length) {
      selectedSlug = filtered[0].slug;
    }
    productList.innerHTML = filtered
      .map(
        (product) => `
          <button class="product-row ${product.slug === selectedSlug ? "is-active" : ""}" type="button" data-product="${escapeHtml(product.slug)}">
            <span><span class="product-row-name">${escapeHtml(product.name)}</span><span class="product-row-tagline">${escapeHtml(product.tagline)}</span></span>
            <span class="product-row-category">${escapeHtml(product.primaryCategory)}</span>
          </button>`
      )
      .join("");
    empty.hidden = filtered.length !== 0;
    if (filtered.length) {
      renderProductDetail(filtered.find((product) => product.slug === selectedSlug));
    } else {
      productDetail.innerHTML = "";
    }
  }

  tabs.forEach((tab) => tab.addEventListener("click", () => activatePage(tab.dataset.tab)));
  document.querySelector("[data-open-catalogue]").addEventListener("click", () => activatePage("catalogue"));
  document.querySelector("[data-tab-link]").addEventListener("click", (event) => {
    event.preventDefault();
    activatePage("overview");
  });
  productList.addEventListener("click", (event) => {
    const button = event.target.closest("[data-product]");
    if (!button) return;
    selectedSlug = button.dataset.product;
    renderProducts(search.value);
  });
  search.addEventListener("input", () => renderProducts(search.value));

  renderBars();
  renderProducts();
  const initial = window.location.hash.slice(1);
  if (["overview", "catalogue", "pipeline"].includes(initial)) activatePage(initial);
})();
