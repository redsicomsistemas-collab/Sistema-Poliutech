
(function () {
  const areaRows = document.getElementById("area-rows");
  const addAreaRowBtn = document.getElementById("add-area-row");
  const calcAreaBtn = document.getElementById("calc-area");
  const wastePct = document.getElementById("waste_pct");
  const areaOutput = document.getElementById("area-output");
  const calcMaterialsBtn = document.getElementById("calc-materials");
  const materialsJson = document.getElementById("materials-json");
  const materialsOutput = document.getElementById("materials-output");
  const generateMemoryBtn = document.getElementById("generate-memory");
  const memoryJson = document.getElementById("memory-json");
  const memoryOutput = document.getElementById("memory-output");

  function addAreaRow(values = {}) {
    const row = document.createElement("div");
    row.className = "rowx";
    row.innerHTML = `
      <input type="number" step="0.01" class="largo" placeholder="Largo" value="${values.largo || ''}">
      <input type="number" step="0.01" class="ancho" placeholder="Ancho" value="${values.ancho || ''}">
      <input type="number" step="0.01" class="piezas" placeholder="Piezas" value="${values.piezas || 1}">
      <button type="button" class="remove-row">Eliminar</button>
    `;
    row.querySelector(".remove-row").addEventListener("click", () => row.remove());
    areaRows.appendChild(row);
  }

  async function postJSON(url, payload) {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    return await res.json();
  }

  addAreaRowBtn?.addEventListener("click", () => addAreaRow());
  calcAreaBtn?.addEventListener("click", async () => {
    const rows = [...document.querySelectorAll("#area-rows .rowx")].map(r => ({
      largo: r.querySelector(".largo")?.value,
      ancho: r.querySelector(".ancho")?.value,
      piezas: r.querySelector(".piezas")?.value
    }));
    const data = await postJSON("/mar-data/area/calculate", {
      rows,
      waste_pct: wastePct?.value || 0
    });
    areaOutput.textContent = JSON.stringify(data, null, 2);
  });

  calcMaterialsBtn?.addEventListener("click", async () => {
    let mats = [];
    let areaTotal = 0;
    try {
      mats = JSON.parse(materialsJson.value);
      const parsedArea = JSON.parse(areaOutput.textContent || "{}");
      areaTotal = parsedArea.total_area || 0;
    } catch (e) {}
    const data = await postJSON("/mar-data/materials/generate", {
      area_total: areaTotal,
      materials: mats
    });
    materialsOutput.textContent = JSON.stringify(data, null, 2);
  });

  generateMemoryBtn?.addEventListener("click", async () => {
    let payload = {};
    try { payload = JSON.parse(memoryJson.value); } catch (e) {}
    const data = await postJSON("/mar-data/memory/generate", payload);
    memoryOutput.textContent = data.memory_text || JSON.stringify(data, null, 2);
  });

  addAreaRow({ largo: 10, ancho: 5, piezas: 1 });
})();
