// ============================================================
//  dashboard.js - KPIs + filtros + tabla + gráfica
// ============================================================

document.addEventListener("DOMContentLoaded", () => {
  const tbody = document.querySelector("#tbl-cot tbody");
  const btnBuscar = document.getElementById("btn-buscar");

  async function cargarCotizaciones() {
    const params = new URLSearchParams();
    const estatus = document.getElementById("f-estatus").value;
    const fi = document.getElementById("f-fi").value;
    const ff = document.getElementById("f-ff").value;
    const mmin = document.getElementById("f-mmin").value;
    const mmax = document.getElementById("f-mmax").value;

    if (estatus) params.append("estatus", estatus);
    if (fi) params.append("fi", fi);
    if (ff) params.append("ff", ff);
    if (mmin) params.append("mmin", mmin);
    if (mmax) params.append("mmax", mmax);

    const res = await fetch("/api/cotizaciones/search?" + params.toString());
    const data = await res.json();

    tbody.innerHTML = "";
    if (data.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7" class="text-center text-muted">No hay cotizaciones que coincidan.</td></tr>`;
      return;
    }

    data.forEach(c => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${c.folio}</td>
        <td>${c.cliente}</td>
        <td>${c.empresa}</td>
        <td>${c.fecha}</td>
        <td>${c.estatus}</td>
        <td>$${(c.total||0).toFixed(2)}</td>
        <td class="d-flex gap-2">
          <a href="${c.export_csv}" class="btn btn-sm btn-outline-primary">CSV</a>
          <a href="${c.export_pdf}" class="btn btn-sm btn-outline-danger" target="_blank" rel="noopener">PDF</a>
        </td>
      `;
      tbody.appendChild(tr);
    });
  }

  async function cargarGrafica() {
    try {
      const res = await fetch("/api/dashboard/metrics");
      const { series } = await res.json();
      const ctx = document.getElementById("chartTotales").getContext("2d");
      const labels = series.map(s => s.mes);
      const valores = series.map(s => s.total);

      new Chart(ctx, {
        type: "bar",
        data: {
          labels,
          datasets: [{
            label: "Total mensual ($)",
            data: valores,
            backgroundColor: "rgba(13, 110, 253, 0.6)",
            borderColor: "rgba(13, 110, 253, 1)",
            borderWidth: 1
          }]
        },
        options: { scales: { y: { beginAtZero: true }}, plugins: { legend: { display: false }} }
      });
    } catch (e) { console.error(e); }
  }

  btnBuscar.addEventListener("click", cargarCotizaciones);
  cargarCotizaciones();
  cargarGrafica();
});
