package com.poliutech.marstatuscotizacion

import android.view.LayoutInflater
import android.view.ViewGroup
import androidx.recyclerview.widget.RecyclerView
import com.poliutech.marstatuscotizacion.databinding.ItemQuoteBinding

class QuoteAdapter(
    private val onStatusTap: (QuoteItem) -> Unit,
    private val onOpenPdf: (QuoteItem) -> Unit,
) : RecyclerView.Adapter<QuoteAdapter.QuoteViewHolder>() {

    private val items = mutableListOf<QuoteItem>()

    fun submitList(newItems: List<QuoteItem>) {
        items.clear()
        items.addAll(newItems)
        notifyDataSetChanged()
    }

    inner class QuoteViewHolder(private val binding: ItemQuoteBinding) :
        RecyclerView.ViewHolder(binding.root) {
        fun bind(item: QuoteItem) {
            binding.txtFolio.text = item.folio
            binding.txtCliente.text = item.cliente.ifBlank { "Sin cliente" }
            binding.txtFecha.text = item.fecha
            binding.txtTotal.text = item.total
            binding.txtResponsable.text = item.responsable.ifBlank { "Sin responsable" }
            binding.btnStatus.text = item.estatus.ifBlank { "Sin estatus" }
            binding.btnStatus.setOnClickListener { onStatusTap(item) }
            binding.txtFolio.setOnClickListener { onOpenPdf(item) }
            binding.txtCliente.setOnClickListener { onOpenPdf(item) }
        }
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): QuoteViewHolder {
        val binding = ItemQuoteBinding.inflate(LayoutInflater.from(parent.context), parent, false)
        return QuoteViewHolder(binding)
    }

    override fun onBindViewHolder(holder: QuoteViewHolder, position: Int) {
        holder.bind(items[position])
    }

    override fun getItemCount(): Int = items.size
}
