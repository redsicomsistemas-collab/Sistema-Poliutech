// ============================================================
//  cotizador.js - renglones, autocompletar y totales
// ============================================================

function fmt(n){ 
  return (Number(n)||0).toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2}); 
}

function rowTemplate(){
  return `
    <tr>
      <td><input type="text" class="form-control form-control-sm item-capitulo" name="item_capitulo[]" placeholder="Capítulo"></td>
      <td class="position-relative">
        <textarea class="form-control form-control-sm item-nombre quote-textarea" name="item_nombre_concepto[]" rows="3" placeholder="Escribe para buscar..." autocomplete="off"></textarea>
        <div class="list-group position-absolute w-100 item-suggest" style="z-index:1000; max-height:180px; overflow:auto;"></div>
      </td>
      <td><input type="text" class="form-control form-control-sm item-unidad" name="item_unidad[]"></td>
      <td><input type="number" step="0.01" class="form-control form-control-sm item-cantidad" name="item_cantidad[]" value="1"></td>
      <td><input type="number" step="0.01" class="form-control form-control-sm item-precio" name="item_precio[]" value="0"></td>
      <td><input type="text" class="form-control form-control-sm item-sistema" name="item_sistema[]" placeholder="Sistema"></td>
      <td class="text-end"><span class="item-subtotal">$0.00</span></td>
      <td><textarea class="form-control form-control-sm quote-textarea" name="item_descripcion[]" rows="2"></textarea></td>
      <td class="text-center"><button type="button" class="btn btn-sm btn-outline-danger btn-del">×</button></td>
    </tr>
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
        if (capitulo) capitulo.value = "";
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
    return tr;
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
      return (Number(n)||0).toLocaleString("es-MX", { style:"currency", currency:"MXN" });
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
        });
      }
      recalcTotals();
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
