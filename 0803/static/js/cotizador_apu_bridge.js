\
(function () {
  function fmtMoney(n){ return (Number(n)||0).toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2}); }
  const searchInput = document.getElementById("apu_search");
  const suggestions = document.getElementById("apu_suggestions");
  const resumen = document.getElementById("apu_resumen");
  const qtyInput = document.getElementById("apu_cantidad");
  const addBtn = document.getElementById("btn-add-apu-to-quote");
  const addRowBtn = document.getElementById("btn-add-row");
  const itemsBody = document.getElementById("items-body");
  if (!searchInput || !suggestions || !resumen || !addBtn || !addRowBtn || !itemsBody) return;
  let selectedAPU = null;
  async function fetchJSON(url){ const r = await fetch(url); if(!r.ok) throw new Error("No se pudo cargar " + url); return await r.json(); }
  function clearSuggestions(){ suggestions.innerHTML = ""; }
  function setResumen(item){
    if (!item) { resumen.innerHTML = "Busca un APU y selecciónalo para cargarlo al cotizador."; return; }
    resumen.innerHTML = `<div><b>Concepto:</b> ${item.concepto || ""}</div><div><b>Unidad:</b> ${item.unidad || ""}</div><div><b>Precio unitario:</b> $${fmtMoney(item.precio_unitario)}</div><div><b>Costo directo:</b> $${fmtMoney(item.costo_directo || 0)}</div><div><b>Clave:</b> ${item.clave || ""}</div>`;
  }
  async function buscarAPU(q){
    if (!q || q.trim().length < 1) { clearSuggestions(); return; }
    const data = await fetchJSON(`/apu/api/suggest?q=${encodeURIComponent(q.trim())}`);
    clearSuggestions();
    data.forEach(item => {
      const div = document.createElement("div");
      div.className = "list-group-item list-group-item-action";
      div.textContent = `${item.clave ? item.clave + " — " : ""}${item.concepto} — ${item.unidad} — $${fmtMoney(item.precio_unitario)}`;
      div.onclick = async () => {
        searchInput.value = item.concepto;
        clearSuggestions();
        selectedAPU = await fetchJSON(`/apu/api/${item.id}/resumen`);
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
    const subtotalEl = row.querySelector(".item-subtotal");
    if (!nombre || !unidad || !cantidadEl || !precio) { Swal.fire("Error", "La fila del cotizador no coincide con la estructura esperada.", "error"); return; }
    nombre.value = selectedAPU.concepto || "";
    unidad.value = selectedAPU.unidad || "";
    cantidadEl.value = String(cantidad);
    precio.value = String(Number(selectedAPU.precio_unitario || 0));
    if (sistema) sistema.value = "MAR DATA";
    if (desc) desc.value = `Generado desde APU ${selectedAPU.clave || selectedAPU.id || ""}`.trim();
    const line = (Number(cantidadEl.value)||0) * (Number(precio.value)||0);
    if (subtotalEl) subtotalEl.textContent = "$" + fmtMoney(line);
    [nombre, unidad, cantidadEl, precio, sistema, desc].forEach(triggerInput);
    document.getElementById("iva_porc")?.dispatchEvent(new Event("input", { bubbles: true }));
    Swal.fire({ icon: "success", title: "APU agregado", text: "El concepto del APU se agregó al cotizador.", timer: 1200, showConfirmButton: false });
  }
  searchInput.addEventListener("input", () => buscarAPU(searchInput.value));
  addBtn.addEventListener("click", addAPUToQuote);
  document.addEventListener("click", (e) => { if (!suggestions.contains(e.target) && e.target !== searchInput) clearSuggestions(); });
})();
