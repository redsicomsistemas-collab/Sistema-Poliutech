package com.poliutech.marstatuscotizacion

import android.view.LayoutInflater
import android.view.ViewGroup
import androidx.recyclerview.widget.RecyclerView
import com.poliutech.marstatuscotizacion.databinding.ItemRegistroObraBinding

class RegistroObraAdapter(
    private val onEdit: (RegistroObraItem) -> Unit,
) : RecyclerView.Adapter<RegistroObraAdapter.RegistroObraViewHolder>() {

    private val items = mutableListOf<RegistroObraItem>()

    fun submitList(newItems: List<RegistroObraItem>) {
        items.clear()
        items.addAll(newItems)
        notifyDataSetChanged()
    }

    fun getSelectedIds(): List<Int> = items.filter { it.selected }.map { it.id }

    inner class RegistroObraViewHolder(private val binding: ItemRegistroObraBinding) :
        RecyclerView.ViewHolder(binding.root) {
        fun bind(item: RegistroObraItem) {
            binding.chkSelect.setOnCheckedChangeListener(null)
            binding.chkSelect.isChecked = item.selected
            binding.txtNumero.text = item.numero
            binding.txtObra.text = item.obra
            binding.txtMeta.text = listOf(item.ubicacion, item.encargado, item.telefono)
                .filter { it.isNotBlank() }
                .joinToString(" · ")
            binding.txtResponsable.text = item.responsable

            binding.chkSelect.setOnCheckedChangeListener { _, checked ->
                item.selected = checked
            }
            binding.btnEdit.setOnClickListener { onEdit(item) }
        }
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): RegistroObraViewHolder {
        val binding = ItemRegistroObraBinding.inflate(LayoutInflater.from(parent.context), parent, false)
        return RegistroObraViewHolder(binding)
    }

    override fun onBindViewHolder(holder: RegistroObraViewHolder, position: Int) {
        holder.bind(items[position])
    }

    override fun getItemCount(): Int = items.size
}
