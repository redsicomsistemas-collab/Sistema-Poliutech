// ============================================================
//  cotizador.js - renglones, autocompletar y totales
// ============================================================

function fmt(n){ 
  return (Number(n)||0).toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2}); 
}

function rowTemplate(){
  return `
    <tr>
      <td class="position-relative">
        <input type="text" class="form-control form-control-sm item-nombre" name="item_nombre_concepto[]" placeholder="Escribe para buscar..." autocomplete="off">
        <div class="list-group position-absolute w-100 item-suggest" style="z-index:1000; max-height:180px; overflow:auto;"></div>
      </td>
      <td><input type="text" class="form-control form-control-sm item-unidad" name="item_unidad[]"></td>
      <td><input type="number" step="0.01" class="form-control form-control-sm item-cantidad" name="item_cantidad[]" value="1"></td>
      <td><input type="number" step="0.01" class="form-control form-control-sm item-precio" name="item_precio[]" value="0"></td>
      <td><input type="text" class="form-control form-control-sm item-sistema" name="item_sistema[]" placeholder="Sistema"></td>
      <td class="text-end"><span class="item-subtotal">$0.00</span></td>
      <td><input type="text" class="form-control form-control-sm" name="item_descripcion[]"></td>
      <td class="text-center"><button type="button" class="btn btn-sm btn-outline-danger btn-del">×</button></td>
    </tr>
  `;
}

function bindRowEvents(tr){
  const nombre = tr.querySelector(".item-nombre");
  const unidad = tr.querySelector(".item-unidad");
  const cantidad = tr.querySelector(".item-cantidad");
  const precio = tr.querySelector(".item-precio");
  const sistema = tr.querySelector(".item-sistema");
  const desc = tr.querySelector('input[name="item_descripcion[]"]');
  const subtotalEl = tr.querySelector(".item-subtotal");
  const sug = tr.querySelector(".item-suggest");

  // Autocompletar concepto
  nombre.addEventListener("input", async ()=>{
    const q = nombre.value.trim();
    if(q.length < 1){ sug.innerHTML=""; return; }
    const res = await fetch("/api/conceptos/suggest?q="+encodeURIComponent(q));
    const data = await res.json();
    sug.innerHTML = "";
    data.forEach(it=>{
      const div = document.createElement("div");
      div.className = "list-group-item list-group-item-action";
      div.textContent = it.label;
      div.onclick = ()=>{
        nombre.value = it.nombre_concepto || it.label;
        unidad.value = it.unidad || "";
        precio.value = it.precio_unitario ?? 0;
        sistema.value = it.sistema || "";     // 👈 ahora “jala” sistema del catálogo
        desc.value = it.descripcion || "";
        sug.innerHTML="";
        recalcRow(); recalcTotals();
      };
      sug.appendChild(div);
    });
  });

  function recalcRow(){
    const c = Number(cantidad.value)||0;
    const p = Number(precio.value)||0;
    const line = c * p;
    subtotalEl.textContent = "$"+fmt(line);
  }
  [cantidad, precio].forEach(i=> i.addEventListener("input", ()=>{ recalcRow(); recalcTotals(); }));

  tr.querySelector(".btn-del").addEventListener("click", ()=>{ tr.remove(); recalcTotals(); });

  recalcRow();
}

function recalcTotals(){
  const rows = document.querySelectorAll("#items-body tr");
  let subtotal = 0;
  rows.forEach(tr=>{
    const cantidad = Number(tr.querySelector(".item-cantidad").value)||0;
    const precio = Number(tr.querySelector(".item-precio").value)||0;
    subtotal += cantidad * precio;
  });

  // --- descuento por zona ---
  const zonaEl = document.getElementById("zona");
  const zona = (zonaEl && zonaEl.value) ? String(zonaEl.value).trim() : "";
  const ZONA_PORC = {
    "Zona Norte": 10,
    "Zona Centro": 5,
    "Bajío": 10,
    "Zona Sur": 15,
    "Frontera": 8,
  };
  const descPorc = ZONA_PORC[zona] || 0;
  const descuento = subtotal * (descPorc / 100);
  const subtotalDesc = subtotal - descuento;

  const ivaPorc = Number(document.getElementById("iva_porc").value)||0;
  const iva = subtotalDesc * ivaPorc/100;
  const total = subtotalDesc + iva;

  document.getElementById("ui-subtotal").textContent = "$"+fmt(subtotal);
  const uiDesc = document.getElementById("ui-descuento");
  const uiSubDesc = document.getElementById("ui-subtotal-desc");
  if (uiDesc) uiDesc.textContent = "-$"+fmt(descuento);
  if (uiSubDesc) uiSubDesc.textContent = "$"+fmt(subtotalDesc);
  document.getElementById("ui-iva").textContent = "$"+fmt(iva);
  document.getElementById("ui-total").textContent = "$"+fmt(total);
}

document.addEventListener("DOMContentLoaded", ()=>{

  // ============================================================
  // 🔹 AUTOCOMPLETAR CLIENTE (UI superior) — sin RFC
  // ============================================================
  (function setupCliente(){
    const input = document.getElementById("cliente_input");
    const box = document.getElementById("cliente_suggestions");
    if(!input || !box) return;

    input.addEventListener("input", async ()=>{
      const q = input.value.trim();
      if(q.length<1){ box.innerHTML=""; return; }
      const res = await fetch("/api/clientes/suggest?q="+encodeURIComponent(q));
      const data = await res.json();
      box.innerHTML = "";

      data.forEach(it=>{
        const div = document.createElement("div");
        div.className = "list-group-item list-group-item-action";
        div.textContent = it.label;
        div.onclick = ()=>{
          input.value = it.nombre_cliente || "";
          document.getElementById("empresa").value = it.empresa || "";
          document.getElementById("responsable").value = it.responsable || "";
          document.getElementById("correo").value = it.correo || "";
          document.getElementById("telefono").value = it.telefono || "";
          document.getElementById("direccion").value = it.direccion || "";
          // ❌ sin RFC
          box.innerHTML="";
        };
        box.appendChild(div);
      });
    });

    document.addEventListener("click", (e)=>{
      if(!box.contains(e.target) && e.target!==input) box.innerHTML="";
    });
  })();

  // ============================================================
  // 🔹 MANEJO DE RENGLONES DEL COTIZADOR
  // ============================================================
  const tbody = document.getElementById("items-body");
  const btnAdd = document.getElementById("btn-add-row");
  function addRow(){
    const tmp = document.createElement("tbody");
    tmp.innerHTML = rowTemplate();
    const tr = tmp.firstElementChild;
    tbody.appendChild(tr);
    bindRowEvents(tr);
    recalcTotals();
  }
  if (btnAdd) btnAdd.addEventListener("click", addRow);
  addRow();

  const ivaField = document.getElementById("iva_porc");
  if (ivaField) ivaField.addEventListener("input", recalcTotals);

  const zonaField = document.getElementById("zona");
  if (zonaField) zonaField.addEventListener("change", recalcTotals);

  // ============================================================
  // 🔹 ENVÍO + ABRIR PDF NUEVA PESTAÑA
  // ============================================================
  const frm = document.getElementById("frm-cotizacion");
  if (frm) {
    frm.addEventListener("submit", async (e) => {
      e.preventDefault();

      const formData = new FormData(frm);
      const res = await fetch(frm.action, { method: "POST", body: formData });
      const text = await res.text();

      const folioMatch = text.match(/Folio:\s*<b>(.*?)<\/b>/i);
      const folio = folioMatch ? folioMatch[1] : null;

      if (text.includes("Cotización creada con éxito") && folio) {
        Swal.fire({
          icon: "success",
          title: "Cotización guardada",
          html: `Folio: <b>${folio}</b><br>Se abrirá el PDF en una nueva pestaña.`,
          confirmButtonText: "Ver PDF",
          timer: 2400,
          timerProgressBar: true,
        }).then(() => {
          window.open(`/cotizaciones/${folio}/export.pdf`.replace(/PTCH-\d+/, folio), "_blank");
          setTimeout(() => {
            window.location.href = "/";
          }, 800);
        });
      } else {
        Swal.fire("Error", "No se pudo guardar la cotización.", "error");
        console.warn("Respuesta inesperada:", text);
      }
    });
  }

});
