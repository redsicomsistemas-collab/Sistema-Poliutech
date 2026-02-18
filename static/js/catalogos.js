(function(){
  let page = 1, per_page = 50;
  async function load(){
    const params = new URLSearchParams({
      page, per_page,
      q: document.getElementById('q').value.trim(),
      unidad: document.getElementById('unidad').value,
      anio_min: document.getElementById('anio_min').value,
      anio_max: document.getElementById('anio_max').value,
    });
    const r = await fetch('/catalogos/list?'+params.toString()).then(r=>r.json());
    const tbody = document.querySelector('#tbl-cat tbody');
    tbody.innerHTML = '';
    r.items.forEach(x=>{
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${x.nombre}</td>
        <td>${x.descripcion||''}</td>
        <td>${x.unidad||''}</td>
        <td class="right">${(x.precio||0).toFixed(2)}</td>
        <td>${x.anio||''}</td>
      `;
      tbody.appendChild(tr);
    });
    document.getElementById('page').textContent = r.page;
  }
  document.getElementById('btn-buscar').addEventListener('click', ()=>{page=1; load();});
  document.getElementById('prev').addEventListener('click', ()=>{ if(page>1){page--; load();}});
  document.getElementById('next').addEventListener('click', ()=>{ page++; load();});
  document.getElementById('frm-import').addEventListener('submit', async (e)=>{
    e.preventDefault();
    const fd = new FormData(e.target);
    const res = await fetch('/catalogos/import', {method:'POST', body:fd}).then(r=>r.json());
    alert('Importados: '+(res.inserted||0));
    load();
  });
  load();
})();
