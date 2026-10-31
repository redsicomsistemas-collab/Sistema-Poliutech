// ============================================================
//  cotizador.js - renglones, autocompletar y totales
// ============================================================

function currentCurrency(){
  return document.getElementById("moneda")?.value || "MXN";
}

function fmt(n){
  return new Intl.NumberFormat("es-MX", {
    style: "currency",
    currency: currentCurrency(),
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number(n) || 0);
}

function rowTemplate(){
  return `
    <article class="quote-create-item" role="listitem">
      <div class="quote-create-item-header">
        <span class="quote-item-number">1</span>
        <span class="quote-item-preview">Concepto sin nombre</span>
        <span class="quote-item-header-total item-subtotal">${fmt(0)}</span>
        <div class="quote-row-actions">
          <button type="button" class="btn btn-sm btn-outline-secondary btn-move-up" title="Subir concepto" aria-label="Subir concepto">↑</button>
          <button type="button" class="btn btn-sm btn-outline-secondary btn-move-down" title="Bajar concepto" aria-label="Bajar concepto">↓</button>
          <button type="button" class="btn btn-sm btn-outline-danger btn-del" title="Eliminar concepto" aria-label="Eliminar concepto">×</button>
        </div>
      </div>
      <div class="quote-create-item-body">
        <div class="quote-item-core">
          <div class="quote-item-concept position-relative">
            <label>Concepto</label>
            <textarea class="form-control item-nombre quote-textarea" name="item_nombre_concepto[]" rows="2" placeholder="Buscar o describir concepto…" autocomplete="off" required></textarea>
            <div class="list-group position-absolute w-100 item-suggest quote-suggestions"></div>
          </div>
          <div><label>Unidad</label><input type="text" class="form-control item-unidad" name="item_unidad[]" placeholder="m²"></div>
          <div><label>Cantidad</label><input type="number" step="0.01" class="form-control item-cantidad text-end" name="item_cantidad[]" value="1"></div>
          <div><label>Precio unitario</label><input type="number" step="0.01" class="form-control item-precio text-end" name="item_precio[]" value="0"></div>
          <div class="quote-item-subtotal-wrap"><label>Importe</label><div class="quote-item-subtotal item-subtotal">${fmt(0)}</div></div>
        </div>
        <details class="quote-disclosure quote-item-advanced">
          <summary><span>Más detalles</span><small>Capítulo, sistema y descripción</small></summary>
          <div class="quote-disclosure-body"><div class="row g-3">
            <div class="col-md-4"><label class="form-label">Capítulo</label><input type="text" class="form-control item-capitulo" name="item_capitulo[]" placeholder="Capítulo"></div>
            <div class="col-md-4"><label class="form-label">Sistema</label><input type="text" class="form-control item-sistema" name="item_sistema[]" placeholder="Sistema"></div>
            <div class="col-md-4"><label class="form-label">Descripción adicional</label><textarea class="form-control quote-textarea" name="item_descripcion[]" rows="2" placeholder="Información complementaria"></textarea></div>
          </div></div>
        </details>
      </div>
    </article>
  `;
}

function bindRowEvents(tr){
  const nombre = tr.querySelector(".item-nombre");
  const capitulo = tr.querySelector(".item-capitulo");
  const unidad = tr.querySelector(".item-unidad");
  const cantidad = tr.querySelector(".item-cantidad");
  const precio = tr.querySelector(".item-precio");
  const sistema = tr.querySelector(".item-sistema");
  const desc = tr.querySelector('textarea[name="item_descripcion[]"]');
  const subtotalEls = tr.querySelectorAll(".item-subtotal");
  const previewEl = tr.querySelector(".quote-item-preview");
  const sug = tr.querySelector(".item-suggest");

  // Autocompletar concepto
  nombre.addEventListener("input", async ()=>{
    const q = nombre.value.trim();
    if(previewEl) previewEl.textContent = q.replace(/\s+/g, " ") || "Concepto sin nombre";
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
        if (capitulo) capitulo.value = "";
        unidad.value = it.unidad || "";
        precio.value = it.precio_unitario ?? 0;
        sistema.value = it.sistema || "";     // 👈 ahora “jala” sistema del catálogo
        desc.value = it.descripcion || "";
        sug.innerHTML="";
        if(previewEl) previewEl.textContent = nombre.value.trim().replace(/\s+/g, " ") || "Concepto sin nombre";
        recalcRow(); recalcTotals();
      };
      sug.appendChild(div);
    });
  });

  function recalcRow(){
    const c = Number(cantidad.value)||0;
    const p = Number(precio.value)||0;
    const line = c * p;
    subtotalEls.forEach(el => { el.textContent = fmt(line); });
  }
  [cantidad, precio].forEach(i=> i.addEventListener("input", ()=>{ recalcRow(); recalcTotals(); }));

  tr.querySelector(".btn-move-up").addEventListener("click", ()=>{
    const prev = tr.previousElementSibling;
    if(prev){
      tr.parentNode.insertBefore(tr, prev);
      updateMoveButtons();
    }
  });
  tr.querySelector(".btn-move-down").addEventListener("click", ()=>{
    const next = tr.nextElementSibling;
    if(next){
      tr.parentNode.insertBefore(next, tr);
      updateMoveButtons();
    }
  });
  tr.querySelector(".btn-del").addEventListener("click", ()=>{ tr.remove(); recalcTotals(); updateMoveButtons(); });

  recalcRow();
  updateMoveButtons();
}

function updateMoveButtons(){
  const rows = Array.from(document.querySelectorAll("#items-body .quote-create-item"));
  rows.forEach((tr, idx)=>{
    const up = tr.querySelector(".btn-move-up");
    const down = tr.querySelector(".btn-move-down");
    if(up) up.disabled = idx === 0;
    if(down) down.disabled = idx === rows.length - 1;
    const number = tr.querySelector(".quote-item-number");
    if(number) number.textContent = idx + 1;
  });
  const count = document.getElementById("quote-items-count");
  const empty = document.getElementById("quote-items-empty");
  if(count) count.textContent = rows.length;
  if(empty) empty.classList.toggle("d-none", rows.length > 0);
}

let descuentoEditadoManualmente = false;

function recalcTotals(){
  const rows = document.querySelectorAll("#items-body .quote-create-item");
  let subtotal = 0;
  rows.forEach(tr=>{
    const cantidad = Number(tr.querySelector(".item-cantidad").value)||0;
    const precio = Number(tr.querySelector(".item-precio").value)||0;
    const line = cantidad * precio;
    subtotal += line;
    tr.querySelectorAll(".item-subtotal").forEach(el => { el.textContent = fmt(line); });
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
  const descuentoField = document.getElementById("descuento_total");
  if (descuentoField && !descuentoEditadoManualmente) {
    descuentoField.value = descPorc.toFixed(2);
  }
  const descuentoPorcCapturado = Number(descuentoField ? descuentoField.value : 0) || 0;
  const descuentoPorcAplicado = Math.min(Math.max(descuentoPorcCapturado, 0), 100);
  const descuento = subtotal * (descuentoPorcAplicado / 100);
  const subtotalDesc = subtotal - descuento;

  const ivaPorc = Number(document.getElementById("iva_porc").value)||0;
  const iva = subtotalDesc * ivaPorc/100;
  const total = subtotalDesc + iva;

  document.getElementById("ui-subtotal").textContent = fmt(subtotal);
  const uiSubDesc = document.getElementById("ui-subtotal-desc");
  if (uiSubDesc) uiSubDesc.textContent = fmt(subtotalDesc);
  document.getElementById("ui-iva").textContent = fmt(iva);
  document.getElementById("ui-total").textContent = fmt(total);
  const currency = currentCurrency();
  ["quote-currency-chip", "quote-summary-currency"].forEach(id => {
    const el = document.getElementById(id);
    if(el) el.textContent = currency;
  });
  window.quoteTotals = { subtotal, descuento, subtotalDesc, iva, total, currency };
  return window.quoteTotals;
}

document.addEventListener("DOMContentLoaded", ()=>{
  const frm = document.getElementById("frm-cotizacion");

  // ============================================================
  // 🔹 AUTOCOMPLETAR CLIENTE (UI superior) — sin RFC
  // ============================================================
  (function setupCliente(){
    const input = document.getElementById("cliente_input");
    const box = document.getElementById("cliente_suggestions");
    if(!input || !box) return;

    input.addEventListener("input", async ()=>{
      const q = input.value.trim();
      updateClientSectionState();
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
          document.getElementById("correo").value = it.correo || "";
          document.getElementById("telefono").value = it.telefono || "";
          document.getElementById("direccion").value = it.direccion || "";
          box.innerHTML="";
          updateClientSectionState();
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
  const btnAddBottom = document.getElementById("btn-add-row-bottom");
  function addRow(){
    const tmp = document.createElement("div");
    tmp.innerHTML = rowTemplate();
    const tr = tmp.firstElementChild;
    tbody.appendChild(tr);
    bindRowEvents(tr);
    recalcTotals();
    return tr;
  }
  if (btnAdd) btnAdd.addEventListener("click", addRow);
  if (btnAddBottom) btnAddBottom.addEventListener("click", addRow);
  addRow();

  function focusNextQuoteRow(currentRow){
    let nextRow = currentRow.nextElementSibling;
    if(!nextRow) nextRow = addRow();
    const nextField = nextRow?.querySelector(".item-nombre, input, textarea, select");
    if(nextField){
      nextField.focus();
      nextField.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }

  tbody.addEventListener("keydown", (event)=>{
    if(event.key !== "Enter" || event.isComposing) return;
    const target = event.target;
    const currentRow = target.closest(".quote-create-item");
    if(!currentRow || target.matches("button")) return;
    // Shift + Enter conserva el salto de línea en Concepto y Descripción.
    if(target.matches("textarea") && event.shiftKey) return;
    event.preventDefault();
    event.stopPropagation();
    focusNextQuoteRow(currentRow);
  });

  frm?.addEventListener("keydown", (event)=>{
    if(event.key !== "Enter" || event.isComposing || event.target.closest("#items-body")) return;
    const target = event.target;
    if(target.matches("textarea, button")) return;
    event.preventDefault();
    const fields = Array.from(frm.querySelectorAll(
      "input:not([type='hidden']):not([type='button']):not([type='submit']):not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled])"
    )).filter((field)=> field.offsetParent !== null);
    const currentIndex = fields.indexOf(target);
    const nextField = currentIndex >= 0 ? fields[currentIndex + 1] : null;
    if(nextField) nextField.focus();
  });

  const ivaField = document.getElementById("iva_porc");
  if (ivaField) ivaField.addEventListener("input", recalcTotals);

  const zonaField = document.getElementById("zona");
  if (zonaField) zonaField.addEventListener("change", ()=>{
    descuentoEditadoManualmente = false;
    recalcTotals();
  });

  const descuentoField = document.getElementById("descuento_total");
  if (descuentoField) {
    descuentoField.addEventListener("input", ()=>{
      descuentoEditadoManualmente = true;
      recalcTotals();
    });
  }

  const regionField = document.getElementById("region");
  const monedaField = document.getElementById("moneda");
  const especialidadField = document.getElementById("especialidad");
  const clienteField = document.getElementById("cliente_input");
  const clientState = document.getElementById("client-section-state");

  function updateClientSectionState(){
    const complete = Boolean(clienteField?.value.trim() && especialidadField?.value && regionField?.value);
    if(clientState){
      clientState.textContent = complete ? "Completo" : "Pendiente";
      clientState.classList.toggle("is-complete", complete);
    }
  }

  function updateQuoteContext(){
    const regionChip = document.getElementById("quote-region-chip");
    if(regionChip) regionChip.textContent = regionField?.value || "Sin región";
    recalcTotals();
    updateClientSectionState();
  }

  regionField?.addEventListener("change", updateQuoteContext);
  monedaField?.addEventListener("change", updateQuoteContext);
  especialidadField?.addEventListener("change", updateClientSectionState);
  clienteField?.addEventListener("change", updateClientSectionState);
  updateQuoteContext();

  function escapeHtml(value){
    return String(value ?? "").replace(/[&<>'"]/g, char => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;"
    })[char]);
  }

  function openQuoteReview(){
    if(!frm) return;
    if(!frm.checkValidity()){
      frm.reportValidity();
      const invalid = frm.querySelector(":invalid");
      invalid?.scrollIntoView({ behavior: "smooth", block: "center" });
      return;
    }

    const rows = Array.from(document.querySelectorAll("#items-body .quote-create-item"));
    const items = rows.map(row => ({
      nombre: row.querySelector(".item-nombre")?.value.trim() || "",
      unidad: row.querySelector(".item-unidad")?.value.trim() || "",
      cantidad: Number(row.querySelector(".item-cantidad")?.value) || 0,
      precio: Number(row.querySelector(".item-precio")?.value) || 0,
    })).filter(item => item.nombre);

    if(!items.length){
      Swal.fire("Falta información", "Agrega al menos un concepto antes de revisar.", "warning");
      return;
    }

    const totals = recalcTotals();
    const review = document.getElementById("quote-review-content");
    if(!review) return;
    review.innerHTML = `
      <div class="quote-review-summary">
        <section class="quote-review-block">
          <h3>Cliente y proyecto</h3>
          <div class="quote-review-grid">
            <div><small>Cliente</small><strong>${escapeHtml(clienteField?.value || "Sin cliente")}</strong></div>
            <div><small>Proyecto</small><strong>${escapeHtml(document.getElementById("proyecto")?.value || "No indicado")}</strong></div>
            <div><small>Especialidad</small><strong>${escapeHtml(especialidadField?.value || "No indicada")}</strong></div>
            <div><small>Región</small><strong>${escapeHtml(regionField?.value || "")}</strong></div>
            <div><small>Moneda</small><strong>${escapeHtml(totals.currency)}</strong></div>
            <div><small>Correo</small><strong>${escapeHtml(document.getElementById("correo")?.value || "No indicado")}</strong></div>
          </div>
        </section>
        <section class="quote-review-block">
          <h3>${items.length} concepto${items.length === 1 ? "" : "s"}</h3>
          <ol class="quote-review-items">${items.map(item => `<li><strong>${escapeHtml(item.nombre)}</strong> · ${escapeHtml(item.unidad || "Sin unidad")} · ${item.cantidad} × ${fmt(item.precio)}</li>`).join("")}</ol>
        </section>
        <section class="quote-review-block">
          <div class="quote-review-grid">
            <div><small>Subtotal</small><strong>${fmt(totals.subtotal)}</strong></div>
            <div><small>Descuento</small><strong>−${fmt(totals.descuento)}</strong></div>
            <div><small>IVA</small><strong>${fmt(totals.iva)}</strong></div>
          </div>
          <div class="d-flex justify-content-between align-items-center mt-3 pt-3 border-top"><strong>Total</strong><strong class="quote-review-total">${fmt(totals.total)} ${escapeHtml(totals.currency)}</strong></div>
        </section>
      </div>`;
    bootstrap.Modal.getOrCreateInstance(document.getElementById("quoteReviewModal")).show();
  }

  document.getElementById("btn-review-quote")?.addEventListener("click", openQuoteReview);

  // ============================================================
  // 🔹 ENVÍO + ABRIR PDF NUEVA PESTAÑA
  // ============================================================
  if (frm) {
    frm.addEventListener("submit", async (e) => {
      e.preventDefault();

      const submitBtn = frm.querySelector('button[type="submit"]');
      if (submitBtn) submitBtn.disabled = true;

      Swal.fire({
        title: "Guardando cotización...",
        text: "Por favor espera un momento.",
        allowOutsideClick: false,
        allowEscapeKey: false,
        showConfirmButton: false,
        didOpen: () => Swal.showLoading(),
      });

      try {
        const formData = new FormData(frm);
        const res = await fetch(frm.action, { method: "POST", body: formData });
        const text = await res.text();

        const folioMatch = text.match(/Folio:\s*<b>(.*?)<\/b>/i);
        const folio = folioMatch ? folioMatch[1] : null;

        Swal.close();

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
          frm.dispatchEvent(new CustomEvent("mar:save-failed"));
          if (submitBtn) submitBtn.disabled = false;
          Swal.fire("Error", "No se pudo guardar la cotización.", "error");
          console.warn("Respuesta inesperada:", text);
        }
      } catch (err) {
        frm.dispatchEvent(new CustomEvent("mar:save-failed"));
        if (submitBtn) submitBtn.disabled = false;
        Swal.close();
        Swal.fire("Error", "No se pudo guardar la cotización.", "error");
        console.error(err);
      }
    });
  }

  // ============================================================
  // 🔹 VOZ EN COTIZADOR WEB
  // ============================================================
  (function setupVoiceWeb(){
    const openBtn = document.getElementById("btn-open-voice-web");
    const panel = document.getElementById("voice-web-panel");
    const hideBtn = document.getElementById("btn-voice-web-hide");
    const clearBtn = document.getElementById("btn-voice-web-clear");
    const recordCommandBtn = document.getElementById("btn-voice-record-command");
    const recordConditionsBtn = document.getElementById("btn-voice-record-conditions");
    const previewBtn = document.getElementById("btn-voice-preview-web");
    const applyBtn = document.getElementById("btn-voice-apply-web");
    const commandInput = document.getElementById("voice_command_input");
    const conditionsInput = document.getElementById("voice_conditions_input");
    const previewBox = document.getElementById("voice-web-preview");
    const statusBox = document.getElementById("voice-web-status");
    const statusText = document.getElementById("voice-web-status-text");
    const statusDot = document.getElementById("voice-web-dot");
    const statusSpinner = document.getElementById("voice-web-spinner");
    if(!openBtn || !panel || !recordCommandBtn || !previewBtn || !applyBtn || !commandInput) return;

    let mediaRecorder = null;
    let audioChunks = [];
    let activeTarget = "comando";
    let lastPreview = null;
    let wakeLock = null;

    function setStatus(message, mode){
      statusBox.classList.remove("d-none");
      statusBox.classList.remove("alert-secondary", "alert-danger", "alert-success", "alert-warning");
      statusSpinner.classList.add("d-none");
      statusDot.classList.add("d-none");
      if(mode === "recording"){
        statusBox.classList.add("alert-danger");
        statusDot.classList.remove("d-none");
        statusDot.animate(
          [{ opacity: 1, transform: "scale(1)" }, { opacity: 0.35, transform: "scale(1.25)" }],
          { duration: 650, iterations: Infinity, direction: "alternate" }
        );
      } else if(mode === "busy"){
        statusBox.classList.add("alert-warning");
        statusSpinner.classList.remove("d-none");
      } else if(mode === "success"){
        statusBox.classList.add("alert-success");
      } else if(mode === "error"){
        statusBox.classList.add("alert-danger");
      } else {
        statusBox.classList.add("alert-secondary");
      }
      statusText.textContent = message;
    }

    function clearStatus(){
      statusBox.classList.add("d-none");
      statusDot.getAnimations().forEach(anim => anim.cancel());
      statusSpinner.classList.add("d-none");
      statusDot.classList.add("d-none");
    }

    async function acquireWakeLock(){
      try {
        if("wakeLock" in navigator && navigator.wakeLock?.request){
          wakeLock = await navigator.wakeLock.request("screen");
        }
      } catch(_e){}
    }

    async function releaseWakeLock(){
      try {
        if(wakeLock){
          await wakeLock.release();
        }
      } catch(_e){}
      wakeLock = null;
    }

    function resetVoicePanel(){
      commandInput.value = "";
      if(conditionsInput) conditionsInput.value = "";
      previewBox.innerHTML = "";
      lastPreview = null;
      clearStatus();
    }

    async function startRecording(target){
      if(!navigator.mediaDevices?.getUserMedia || typeof MediaRecorder === "undefined"){
        setStatus("Este navegador no soporta grabación de audio.", "error");
        return;
      }
      if(mediaRecorder){
        stopRecording();
        return;
      }
      activeTarget = target;
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      audioChunks = [];
      mediaRecorder = new MediaRecorder(stream);
      mediaRecorder.ondataavailable = (event) => {
        if(event.data && event.data.size > 0){
          audioChunks.push(event.data);
        }
      };
      mediaRecorder.onstop = async () => {
        const blob = new Blob(audioChunks, { type: mediaRecorder?.mimeType || "audio/webm" });
        stream.getTracks().forEach(track => track.stop());
        mediaRecorder = null;
        await releaseWakeLock();
        await uploadAudio(blob, activeTarget);
      };
      await acquireWakeLock();
      mediaRecorder.start();
      setStatus(target === "condiciones" ? "Grabando condiciones..." : "Grabando comando...", "recording");
      recordCommandBtn.textContent = target === "comando" ? "Terminar comando" : "Grabar comando";
      if(recordConditionsBtn) recordConditionsBtn.textContent = target === "condiciones" ? "Terminar condiciones" : "Grabar condiciones";
    }

    function stopRecording(){
      if(!mediaRecorder) return;
      mediaRecorder.stop();
      setStatus("Subiendo audio...", "busy");
      recordCommandBtn.textContent = "Grabar comando";
      if(recordConditionsBtn) recordConditionsBtn.textContent = "Grabar condiciones";
    }

    async function uploadAudio(blob, target){
      const fd = new FormData();
      fd.append("target", target);
      fd.append("audio", blob, `${target}.webm`);
      setStatus("Transcribiendo audio...", "busy");
      const res = await fetch("/cotizador/voz/transcribir", { method: "POST", body: fd });
      const data = await res.json();
      if(!res.ok || !data.ok){
        setStatus(data.error || "No se pudo transcribir el audio.", "error");
        return;
      }
      const transcript = String(data.transcript || "").trim();
      if(target === "condiciones"){
        conditionsInput.value = [conditionsInput.value.trim(), transcript].filter(Boolean).join(" ").trim();
      } else {
        commandInput.value = [commandInput.value.trim(), transcript].filter(Boolean).join(" ").trim();
      }
      setStatus("Audio transcrito correctamente.", "success");
    }

    function money(n){
      return (Number(n)||0).toLocaleString("es-MX", { style:"currency", currency:currentCurrency() });
    }

    function renderVoicePreview(preview){
      const header = preview.datos_encabezado || {};
      const items = Array.isArray(preview.items) ? preview.items : [];
      const warnings = Array.isArray(preview.warnings) ? preview.warnings : [];
      previewBox.innerHTML = `
        <div class="border rounded p-3 bg-light">
          <div><strong>Cliente:</strong> ${preview.cliente || "En blanco"}</div>
          <div><strong>Empresa:</strong> ${header.empresa || "En blanco"}</div>
          <div><strong>Correo:</strong> ${header.correo || "En blanco"}</div>
          <div><strong>Teléfono:</strong> ${header.telefono || "En blanco"}</div>
          <div><strong>Ciudad:</strong> ${header.ciudad || "En blanco"}</div>
          <div class="mt-2"><strong>Partidas:</strong> ${items.length}</div>
          <div><strong>Total:</strong> ${money(preview?.resumen?.total || 0)}</div>
          ${items.length ? `<hr><div>${items.map((item, idx)=>`
            <div class="mb-2">
              <strong>${idx + 1}. ${item.nombre || "Sin concepto"}</strong><br>
              Unidad: ${item.unidad || "En blanco"} | Cantidad: ${item.cantidad || "En blanco"} | PU: ${item.precio_unitario || "En blanco"} | Sistema: ${item.sistema || "En blanco"}
            </div>
          `).join("")}</div>` : ""}
          ${warnings.length ? `<hr><div class="text-danger">${warnings.map(w=>`<div>- ${w}</div>`).join("")}</div>` : ""}
        </div>
      `;
    }

    function applyPreviewToForm(preview){
      const header = preview.datos_encabezado || {};
      const items = Array.isArray(preview.items) ? preview.items : [];
      document.getElementById("cliente_input").value = preview.cliente || "";
      document.getElementById("empresa").value = header.empresa || "";
      document.getElementById("correo").value = header.correo || "";
      document.getElementById("telefono").value = header.telefono || "";
      const proyectoInput = document.getElementById("proyecto");
      if(proyectoInput) proyectoInput.value = header.proyecto || preview.proyecto || "";
      document.getElementById("ciudad_trabajo").value = (header.ciudad || "").toUpperCase();
      if(Array.isArray(preview.condiciones) && preview.condiciones.length){
        document.getElementById("notas").value = preview.condiciones.join("\n");
      }

      tbody.innerHTML = "";
      if(!items.length){
        addRow();
      } else {
        items.forEach(item => {
          const tr = addRow();
          tr.querySelector(".item-nombre").value = item.nombre || "";
          tr.querySelector(".item-unidad").value = item.unidad || "";
          tr.querySelector(".item-cantidad").value = item.cantidad || "";
          tr.querySelector(".item-precio").value = item.precio_unitario || "";
          tr.querySelector(".item-sistema").value = item.sistema || "";
          const descField = tr.querySelector('textarea[name="item_descripcion[]"]');
          if(descField) descField.value = item.descripcion || "";
          const previewField = tr.querySelector(".quote-item-preview");
          if(previewField) previewField.textContent = item.nombre || "Concepto sin nombre";
        });
      }
      recalcTotals();
      updateClientSectionState();
      setStatus("La captura por voz se aplicó al cotizador.", "success");
    }

    async function previewVoice(){
      const comando = commandInput.value.trim();
      if(!comando){
        setStatus("Graba o pega un comando antes de previsualizar.", "error");
        return;
      }
      setStatus("Interpretando comando...", "busy");
      const res = await fetch("/cotizador/voz/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          comando,
          condiciones: conditionsInput ? conditionsInput.value.trim() : "",
          cliente: document.getElementById("cliente_input").value.trim(),
          notas: document.getElementById("notas").value.trim()
        })
      });
      const data = await res.json();
      if(!res.ok || !data.ok){
        setStatus(data.error || "No se pudo interpretar la voz.", "error");
        return;
      }
      lastPreview = data.preview;
      renderVoicePreview(lastPreview);
      setStatus("Previsualización lista.", "success");
    }

    openBtn.addEventListener("click", ()=>{
      panel.classList.remove("d-none");
      panel.scrollIntoView({ behavior: "smooth", block: "start" });
    });
    if(hideBtn) hideBtn.addEventListener("click", ()=> panel.classList.add("d-none"));
    if(clearBtn) clearBtn.addEventListener("click", resetVoicePanel);
    recordCommandBtn.addEventListener("click", ()=> mediaRecorder ? stopRecording() : startRecording("comando"));
    if(recordConditionsBtn) recordConditionsBtn.addEventListener("click", ()=> mediaRecorder ? stopRecording() : startRecording("condiciones"));
    previewBtn.addEventListener("click", previewVoice);
    applyBtn.addEventListener("click", async ()=>{
      if(!lastPreview){
        await previewVoice();
      }
      if(lastPreview){
        applyPreviewToForm(lastPreview);
      }
    });
  })();

});
