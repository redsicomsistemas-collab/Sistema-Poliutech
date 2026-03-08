
(function () {
  function addAreaRow(containerId, values = {}) {
    const host = document.getElementById(containerId);
    if (!host) return;
    const row = document.createElement("div");
    row.className = "row g-2 mb-2 area-row";
    row.innerHTML = `
      <div class="col-md-3"><input type="number" step="0.01" class="form-control largo" placeholder="Largo" value="${values.largo || ''}"></div>
      <div class="col-md-3"><input type="number" step="0.01" class="form-control ancho" placeholder="Ancho" value="${values.ancho || ''}"></div>
      <div class="col-md-3"><input type="number" step="0.01" class="form-control piezas" placeholder="Piezas" value="${values.piezas || 1}"></div>
      <div class="col-md-3"><button type="button" class="btn btn-outline-danger btn-sm w-100 remove-row">Eliminar</button></div>
    `;
    row.querySelector(".remove-row").addEventListener("click", () => row.remove());
    host.appendChild(row);
  }

  async function postJSON(url, payload) {
    const res = await fetch(url, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(payload)
    });
    return await res.json();
  }

  function boot() {
    const areaRows = document.getElementById("area-rows");
    const addAreaRowBtn = document.getElementById("add-area-row");
    const calcAreaBtn = document.getElementById("calc-area");
    const wastePct = document.getElementById("waste_pct");
    const areaTotalHidden = document.getElementById("area_total");
    const areaTotalView = document.getElementById("area-total-view");
    const calcMaterialsBtn = document.getElementById("calc-materials");
    const materialsJson = document.getElementById("materials-json");
    const materialsOutput = document.getElementById("materials-output");
    const materialsHidden = document.getElementById("lista_materiales_json");
    const generateMemoryBtn = document.getElementById("generate-memory");
    const memoryJson = document.getElementById("memory-json");
    const memoryOutput = document.getElementById("memory-output");
    const memoryHidden = document.getElementById("memoria_tecnica");

    if (!areaRows) return;

    addAreaRowBtn?.addEventListener("click", ()=> addAreaRow("area-rows"));
    calcAreaBtn?.addEventListener("click", async ()=> {
      const rows = [...document.querySelectorAll("#area-rows .area-row")].map(r => ({
        largo: r.querySelector(".largo")?.value,
        ancho: r.querySelector(".ancho")?.value,
        piezas: r.querySelector(".piezas")?.value
      }));
      const data = await postJSON("/mar-data/area/calculate", {rows, waste_pct: wastePct?.value || 0});
      const total = Number(data.total_area || 0);
      if (areaTotalHidden) areaTotalHidden.value = total.toFixed(4);
      if (areaTotalView) areaTotalView.textContent = total.toFixed(2);
    });

    calcMaterialsBtn?.addEventListener("click", async ()=> {
      let mats = [];
      try { mats = JSON.parse(materialsJson?.value || "[]"); } catch(e){}
      const areaTotal = Number(areaTotalHidden?.value || 0);
      const data = await postJSON("/mar-data/materials/generate", {area_total: areaTotal, materials: mats});
      const pretty = JSON.stringify(data, null, 2);
      if (materialsOutput) materialsOutput.textContent = pretty;
      if (materialsHidden) materialsHidden.value = JSON.stringify(data);
    });

    generateMemoryBtn?.addEventListener("click", async ()=> {
      let payload = {};
      try { payload = JSON.parse(memoryJson?.value || "{}"); } catch(e){}
      const data = await postJSON("/mar-data/memory/generate", payload);
      if (memoryOutput) memoryOutput.textContent = data.memory_text || "";
      if (memoryHidden) memoryHidden.value = data.memory_text || "";
    });

    if (!document.querySelector("#area-rows .area-row")) {
      addAreaRow("area-rows", {largo:10, ancho:5, piezas:1});
    }
  }

  document.addEventListener("DOMContentLoaded", boot);
})();
