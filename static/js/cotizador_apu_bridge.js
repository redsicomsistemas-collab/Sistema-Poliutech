document.addEventListener("DOMContentLoaded", () => {
  function fmtMoney(n){ return (Number(n)||0).toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2}); }
  const searchInput = document.getElementById("apu_search");
  const suggestions = document.getElementById("apu_suggestions");
  const resumen = document.getElementById("apu_resumen");
  const qtyInput = document.getElementById("apu_cantidad");
  const addBtn = document.getElementById("btn-add-apu-to-quote");
  const addRowBtn = document.getElementById("btn-add-row");
  const itemsBody = document.getElementById("items-body");
  if (!searchInput || !suggestions || !resumen || !addBtn || !addRowBtn || !itemsBody) return;
  const localCatalog = Array.isArray(window.APU_CATALOG) ? window.APU_CATALOG : [];
  let selectedAPU = null;
  async function fetchJSON(url){ const r = await fetch(url); if(!r.ok) throw new Error("No se pudo cargar " + url); return await r.json(); }
  function clearSuggestions(){ suggestions.innerHTML = ""; }
  function setResumen(item){
    if (!item) { resumen.innerHTML = "Busca un APU y selecciónalo para cargarlo al cotizador."; return; }
    resumen.innerHTML = `
      <div><b>Concepto:</b> ${item.concepto || ""}</div>
      <div><b>Clave:</b> ${item.clave || ""} ${item.categoria ? "· <b>Categoria:</b> " + item.categoria : ""}</div>
      <div><b>Unidad:</b> ${item.unidad || ""}</div>
      <div><b>P.U. venta:</b> $${fmtMoney(item.precio_unitario)}</div>
      <div><b>Costo directo:</b> $${fmtMoney(item.costo_directo || 0)}</div>
      <div><b>Indirectos + sobrecostos:</b> $${fmtMoney((item.precio_unitario || 0) - (item.costo_directo || 0))}</div>
      <div class="mt-2 small text-muted">${item.descripcion || "Sin descripcion tecnica."}</div>
    `;
  }
  function renderNoResults(text){
    clearSuggestions();
    const div = document.createElement("div");
    div.className = "list-group-item text-muted";
    div.textContent = text;
    suggestions.appendChild(div);
  }
  function normalizeText(value){
    return String(value || "").trim().toLowerCase();
  }
  function findLocalAPUs(q){
    const term = normalizeText(q);
    if (!term) return [];
    return localCatalog.filter((item) => {
      const id = String(item.id || "");
      const clave = normalizeText(item.clave);
      const concepto = normalizeText(item.concepto);
      return id.includes(term) || clave.includes(term) || concepto.includes(term);
    }).slice(0, 15);
  }
  async function buscarAPU(q){
    if (!q || q.trim().length < 1) { clearSuggestions(); return; }
    let data = findLocalAPUs(q);
    if (!data.length) {
      try {
        data = await fetchJSON(`/apu/api/suggest?q=${encodeURIComponent(q.trim())}`);
      } catch (err) {
        console.error("Error buscando APU", err);
      }
    }
    clearSuggestions();
    if (!Array.isArray(data) || data.length === 0) {
      renderNoResults("No se encontraron APU.");
      return;
    }
    data.forEach(item => {
      const div = document.createElement("div");
      div.className = "list-group-item list-group-item-action";
      div.textContent = `${item.id} · ${item.clave ? item.clave + " — " : ""}${item.concepto} — ${item.unidad} — $${fmtMoney(item.precio_unitario)}`;
      div.onclick = async () => {
        searchInput.value = item.concepto;
        clearSuggestions();
        selectedAPU = localCatalog.find((apu) => Number(apu.id) === Number(item.id)) || null;
        if (!selectedAPU) {
          selectedAPU = await fetchJSON(`/apu/api/${item.id}/resumen`);
        }
        setResumen(selectedAPU);
      };
      suggestions.appendChild(div);
    });
  }
  function getLastRow(){ const rows = itemsBody.querySelectorAll("tr"); return rows.length ? rows[rows.length - 1] : null; }
  function triggerInput(el){ if (!el) return; el.dispatchEvent(new Event("input", { bubbles: true })); el.dispatchEvent(new Event("change", { bubbles: true })); el.dispatchEvent(new Event("keyup", { bubbles: true })); }
  async function addAPUToQuote(){
    if (!selectedAPU) { Swal.fire("Selecciona un APU", "Primero busca y selecciona un APU.", "warning"); return; }
    const cantidad = Number(qtyInput.value || 0);
    if (!cantidad || cantidad <= 0) { Swal.fire("Cantidad inválida", "La cantidad debe ser mayor a cero.", "warning"); return; }
    addRowBtn.click();
    await new Promise(resolve => setTimeout(resolve, 120));
    const row = getLastRow();
    if (!row) { Swal.fire("Error", "No se pudo agregar una fila al cotizador.", "error"); return; }
    const nombre = row.querySelector(".item-nombre");
    const unidad = row.querySelector(".item-unidad");
    const cantidadEl = row.querySelector(".item-cantidad");
    const precio = row.querySelector(".item-precio");
    const sistema = row.querySelector(".item-sistema");
    const desc = row.querySelector('input[name="item_descripcion[]"]');
    const origen = row.querySelector('input[name="item_origen[]"]');
    const apuId = row.querySelector('input[name="item_apu_id[]"]');
    const apuClave = row.querySelector('input[name="item_apu_clave[]"]');
    const apuDirecto = row.querySelector('input[name="item_apu_directo[]"]');
    const apuResumen = row.querySelector('input[name="item_apu_resumen[]"]');
    const originBadge = row.querySelector(".item-origin-badge");
    const subtotalEl = row.querySelector(".item-subtotal");
    if (!nombre || !unidad || !cantidadEl || !precio) { Swal.fire("Error", "La fila del cotizador no coincide con la estructura esperada.", "error"); return; }
    nombre.value = selectedAPU.concepto || "";
    unidad.value = selectedAPU.unidad || "";
    cantidadEl.value = String(cantidad);
    precio.value = String(Number(selectedAPU.precio_unitario || 0));
    if (sistema) sistema.value = selectedAPU.categoria ? `MAR DATA · ${selectedAPU.categoria}` : "MAR DATA";
    if (desc) desc.value = (selectedAPU.descripcion || `Generado desde APU ${selectedAPU.clave || selectedAPU.id || ""}`).trim();
    if (origen) origen.value = "APU";
    if (apuId) apuId.value = String(selectedAPU.id || "");
    if (apuClave) apuClave.value = String(selectedAPU.clave || "");
    if (apuDirecto) apuDirecto.value = String(Number(selectedAPU.costo_directo || 0));
    if (apuResumen) {
      apuResumen.value = JSON.stringify({
        id: selectedAPU.id || null,
        clave: selectedAPU.clave || "",
        categoria: selectedAPU.categoria || "",
        concepto: selectedAPU.concepto || "",
        unidad: selectedAPU.unidad || "",
        directo: Number(selectedAPU.costo_directo || 0),
        venta: Number(selectedAPU.precio_unitario || 0),
        descripcion: selectedAPU.descripcion || "",
      });
    }
    if (originBadge) {
      originBadge.textContent = `Origen: APU ${selectedAPU.clave || selectedAPU.id || ""} · Directo $${fmtMoney(selectedAPU.costo_directo || 0)}`;
    }
    const line = (Number(cantidadEl.value)||0) * (Number(precio.value)||0);
    if (subtotalEl) subtotalEl.textContent = "$" + fmtMoney(line);
    [nombre, unidad, cantidadEl, precio, sistema, desc].forEach(triggerInput);
    document.getElementById("iva_porc")?.dispatchEvent(new Event("input", { bubbles: true }));
    Swal.fire({ icon: "success", title: "APU agregado", text: "El concepto del APU se agregó al cotizador.", timer: 1200, showConfirmButton: false });
  }

  async function preloadAPUFromQuery(){
    const params = new URLSearchParams(window.location.search);
    const apuId = params.get("apu_id");
    if (!apuId) return;

    try {
      selectedAPU = localCatalog.find((apu) => Number(apu.id) === Number(apuId)) || null;
      if (!selectedAPU) {
        selectedAPU = await fetchJSON(`/apu/api/${encodeURIComponent(apuId)}/resumen`);
      }
      searchInput.value = selectedAPU.concepto || "";
      if (params.get("cantidad")) {
        qtyInput.value = params.get("cantidad");
      }
      setResumen(selectedAPU);

      if (params.get("auto_add") === "1") {
        await new Promise((resolve) => setTimeout(resolve, 80));
        await addAPUToQuote();
      }
    } catch (err) {
      console.error("No se pudo precargar el APU en cotización", err);
      Swal.fire("Error", "No se pudo cargar el APU seleccionado desde MAR DATA.", "error");
    }
  }

  searchInput.addEventListener("input", () => buscarAPU(searchInput.value));
  addBtn.addEventListener("click", addAPUToQuote);
  document.addEventListener("click", (e) => { if (!suggestions.contains(e.target) && e.target !== searchInput) clearSuggestions(); });
  preloadAPUFromQuery();
});
