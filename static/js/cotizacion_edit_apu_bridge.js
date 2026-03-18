document.addEventListener("DOMContentLoaded", () => {
  const searchInput = document.getElementById("apu_search_edit");
  const suggestions = document.getElementById("apu_suggestions_edit");
  const resumen = document.getElementById("apu_resumen_edit");
  const qtyInput = document.getElementById("apu_cantidad_edit");
  const addBtn = document.getElementById("btn-add-apu-edit");
  const items = document.getElementById("items");
  const budgetBox = document.getElementById("mardata-budget-summary-edit");
  if (!searchInput || !suggestions || !resumen || !qtyInput || !addBtn || !items) return;
  let selectedAPU = null;
  function fmtMoney(n){ return (Number(n)||0).toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2}); }
  async function fetchJSON(url) { const r = await fetch(url); if (!r.ok) throw new Error("No se pudo cargar " + url); return await r.json(); }
  function clearSuggestions(){ suggestions.innerHTML = ""; }
  function setResumen(item){
    if (!item) { resumen.innerHTML = "Busca un APU y selecciónalo para agregarlo como renglón."; return; }
    resumen.innerHTML = `
      <div><b>Concepto:</b> ${item.concepto || ""}</div>
      <div><b>Clave:</b> ${item.clave || ""} ${item.categoria ? "· <b>Categoria:</b> " + item.categoria : ""}</div>
      <div><b>Unidad:</b> ${item.unidad || ""}</div>
      <div><b>P.U. venta:</b> $${fmtMoney(item.precio_unitario)}</div>
      <div><b>Costo directo:</b> $${fmtMoney(item.costo_directo || 0)}</div>
      <div class="mt-2 small text-muted">${item.descripcion || "Sin descripcion tecnica."}</div>
    `;
  }
  async function buscarAPU(q){
    if (!q || q.trim().length < 1) { clearSuggestions(); return; }
    const data = await fetchJSON(`/apu/api/suggest?q=${encodeURIComponent(q.trim())}`);
    clearSuggestions();
    if (!Array.isArray(data) || data.length === 0) {
      const div = document.createElement("div");
      div.className = "list-group-item text-muted";
      div.textContent = "No se encontraron APU.";
      suggestions.appendChild(div);
      return;
    }
    data.forEach(item => {
      const div = document.createElement("div");
      div.className = "list-group-item list-group-item-action";
      div.textContent = `${item.id} · ${item.clave ? item.clave + " — " : ""}${item.concepto} — ${item.unidad} — $${fmtMoney(item.precio_unitario)}`;
      div.onclick = async () => {
        searchInput.value = item.concepto;
        clearSuggestions();
        selectedAPU = await fetchJSON(`/apu/api/${item.id}/resumen`);
        setResumen(selectedAPU);
      };
      suggestions.appendChild(div);
    });
  }
  function renderBudgetSummary(){
    if (!budgetBox) return;
    const rows = items.querySelectorAll(".item-edit-row");
    const groups = new Map();
    let directoTotal = 0;
    let ventaTotal = 0;

    rows.forEach((row) => {
      const origen = row.querySelector('input[name="item_origen[]"]')?.value || "";
      if (origen !== "APU") return;
      const apuId = row.querySelector('input[name="item_apu_id[]"]')?.value || "";
      const apuClave = row.querySelector('input[name="item_apu_clave[]"]')?.value || "";
      const apuDirecto = Number(row.querySelector('input[name="item_apu_directo[]"]')?.value || 0);
      const nombre = row.querySelector('.item-edit-nombre')?.value || "Partida MAR DATA";
      const cantidad = Number(row.querySelector('.item-edit-cantidad')?.value || 0);
      const precio = Number(row.querySelector('.item-edit-precio')?.value || 0);
      const key = apuId || apuClave || nombre;
      const item = groups.get(key) || { clave: apuClave || apuId || "-", nombre, cantidad: 0, directo: 0, venta: 0 };
      item.cantidad += cantidad;
      item.directo += apuDirecto * cantidad;
      item.venta += precio * cantidad;
      groups.set(key, item);
      directoTotal += apuDirecto * cantidad;
      ventaTotal += precio * cantidad;
    });

    if (!groups.size) {
      budgetBox.className = "border rounded bg-light p-3 text-muted";
      budgetBox.textContent = "Esta cotización no tiene partidas MAR DATA registradas.";
      return;
    }

    const html = Array.from(groups.values()).map((item) => `
      <tr>
        <td>${item.clave}</td>
        <td>${item.nombre}</td>
        <td class="text-end">${fmtMoney(item.cantidad)}</td>
        <td class="text-end">$${fmtMoney(item.directo)}</td>
        <td class="text-end">$${fmtMoney(item.venta)}</td>
      </tr>
    `).join("");

    budgetBox.className = "border rounded bg-white p-3";
    budgetBox.innerHTML = `
      <div class="row g-3 mb-3">
        <div class="col-md-4"><div class="rounded bg-light p-3"><small class="text-muted d-block">Partidas APU</small><strong>${groups.size}</strong></div></div>
        <div class="col-md-4"><div class="rounded bg-light p-3"><small class="text-muted d-block">Directo acumulado</small><strong>$${fmtMoney(directoTotal)}</strong></div></div>
        <div class="col-md-4"><div class="rounded bg-light p-3"><small class="text-muted d-block">Venta acumulada</small><strong>$${fmtMoney(ventaTotal)}</strong></div></div>
      </div>
      <div class="table-responsive">
        <table class="table table-sm align-middle mb-0">
          <thead class="table-light"><tr><th>APU</th><th>Partida</th><th class="text-end">Cantidad</th><th class="text-end">Directo</th><th class="text-end">Venta</th></tr></thead>
          <tbody>${html}</tbody>
        </table>
      </div>
    `;
  }
  function addAPURow(){
    if (!selectedAPU) { alert("Primero selecciona un APU."); return; }
    const cantidad = Number(qtyInput.value || 0);
    if (!cantidad || cantidad <= 0) { alert("La cantidad debe ser mayor a cero."); return; }
    const html = `
      <div class="row g-2 align-items-end mb-2 border-bottom pb-2 item-edit-row">
        <div class="col-md-3">
          <input type="text" name="item_nombre_concepto[]" class="form-control item-edit-nombre" value="${selectedAPU.concepto || ''}">
          <input type="hidden" name="item_origen[]" value="APU">
          <input type="hidden" name="item_apu_id[]" value="${selectedAPU.id || ''}">
          <input type="hidden" name="item_apu_clave[]" value="${selectedAPU.clave || ''}">
          <input type="hidden" name="item_apu_directo[]" value="${Number(selectedAPU.costo_directo || 0)}">
          <input type="hidden" name="item_apu_resumen[]" value='${JSON.stringify({
            id: selectedAPU.id || null,
            clave: selectedAPU.clave || "",
            categoria: selectedAPU.categoria || "",
            concepto: selectedAPU.concepto || "",
            unidad: selectedAPU.unidad || "",
            directo: Number(selectedAPU.costo_directo || 0),
            venta: Number(selectedAPU.precio_unitario || 0),
            descripcion: selectedAPU.descripcion || "",
          }).replace(/'/g, "&apos;")}'>
          <div class="small text-muted mt-1">Origen: APU ${selectedAPU.clave || selectedAPU.id || ""} · Directo $${fmtMoney(selectedAPU.costo_directo || 0)}</div>
        </div>
        <div class="col-md-1"><input type="text" name="item_unidad[]" class="form-control item-edit-unidad" value="${selectedAPU.unidad || ''}"></div>
        <div class="col-md-1"><input type="number" step="any" name="item_cantidad[]" class="form-control item-edit-cantidad" value="${cantidad}"></div>
        <div class="col-md-2"><input type="number" step="any" name="item_precio[]" class="form-control item-edit-precio" value="${Number(selectedAPU.precio_unitario || 0)}"></div>
        <div class="col-md-2"><input type="text" name="item_sistema[]" class="form-control item-edit-sistema" value="${selectedAPU.categoria ? "MAR DATA · " + selectedAPU.categoria : "MAR DATA"}"></div>
        <div class="col-md-2"><input type="text" name="item_descripcion[]" class="form-control item-edit-descripcion" value="${selectedAPU.descripcion || `Generado desde APU ${selectedAPU.clave || selectedAPU.id || ''}`}"></div>
        <div class="col-md-1 text-end"><button type="button" class="btn btn-outline-danger btn-sm" onclick="this.closest('.item-edit-row').remove()">🗑</button></div>
      </div>`;
    items.insertAdjacentHTML('beforeend', html);
    renderBudgetSummary();
  }
  searchInput.addEventListener("input", () => buscarAPU(searchInput.value));
  addBtn.addEventListener("click", addAPURow);
  document.addEventListener("click", (e) => { if (!suggestions.contains(e.target) && e.target !== searchInput) clearSuggestions(); });
  items.addEventListener("input", renderBudgetSummary);
  const observer = new MutationObserver(renderBudgetSummary);
  observer.observe(items, { childList: true, subtree: true });
  renderBudgetSummary();
});
