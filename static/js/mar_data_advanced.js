
async function postJSON(url, payload){
  const r = await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
  return await r.json();
}
(function(){
  const btnP = document.getElementById('btn-gen-propuesta');
  const btnC = document.getElementById('btn-gen-param');
  const outCalc = document.getElementById('adv-calc');
  const outProp = document.getElementById('adv-propuesta');
  const slug = document.getElementById('adv-slug');
  const area = document.getElementById('adv-area');
  const esp = document.getElementById('adv-espesor');
  const merma = document.getElementById('adv-merma');
  const rend = document.getElementById('adv-rend');
  async function payload(){ return {slug:slug.value, area_total:area.value, espesor_mm:esp.value, merma_pct:merma.value, rendimiento_m2_dia:rend.value}; }
  btnC?.addEventListener('click', async ()=>{ const data = await postJSON('/mar-data/advanced/api/parametric', await payload()); if(outCalc) outCalc.textContent = JSON.stringify(data,null,2); });
  btnP?.addEventListener('click', async ()=>{ const data = await postJSON('/mar-data/advanced/api/propuesta', await payload()); if(outCalc) outCalc.textContent = JSON.stringify(data.calculo,null,2); if(outProp) outProp.textContent = data.propuesta_texto; });
  const btnCosteo = document.getElementById('btn-costeo');
  const outCost = document.getElementById('costeo-output');
  btnCosteo?.addEventListener('click', async ()=>{ const venta=document.getElementById('costeo-venta').value; const costo_real=document.getElementById('costeo-real').value; const data=await postJSON('/mar-data/advanced/api/costeo',{venta,costo_real}); if(outCost) outCost.textContent = JSON.stringify(data,null,2); });
})();
