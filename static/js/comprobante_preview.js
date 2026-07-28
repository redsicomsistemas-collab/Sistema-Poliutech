(function () {
  "use strict";

  const previewUrls = new WeakMap();
  const initializedInputs = new WeakSet();
  const supportedNames = /(comprobante|factura_archivo|pago_archivo|cotizacion_pdf)/i;
  const supportedTypes = /^(application\/pdf|image\/)/i;

  function isSupportedInput(input) {
    if (!(input instanceof HTMLInputElement) || input.type !== "file") return false;
    if (input.dataset.filePreview === "off") return false;
    return input.dataset.filePreview === "on" || supportedNames.test(input.name || input.id || "");
  }

  function isSupportedFile(file) {
    if (!file) return false;
    if (supportedTypes.test(file.type || "")) return true;
    return /\.(pdf|png|jpe?g|webp|gif|bmp)$/i.test(file.name || "");
  }

  function formatSize(bytes) {
    const value = Number(bytes || 0);
    if (value < 1024) return `${value} B`;
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
    return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  }

  function ensureStyles() {
    if (document.getElementById("marComprobantePreviewStyles")) return;
    const style = document.createElement("style");
    style.id = "marComprobantePreviewStyles";
    style.textContent = `
      .mar-file-preview { margin-top: .5rem; min-width: 210px; }
      .mar-file-preview-card {
        overflow: hidden; border: 1px solid var(--bs-border-color);
        border-radius: .5rem; background: var(--bs-body-bg);
      }
      .mar-file-preview-head {
        display: flex; align-items: center; justify-content: space-between;
        gap: .5rem; padding: .45rem .55rem; background: var(--bs-tertiary-bg);
      }
      .mar-file-preview-name {
        min-width: 0; font-size: .78rem; font-weight: 600;
        overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
      }
      .mar-file-preview-meta { color: var(--bs-secondary-color); font-size: .72rem; }
      .mar-file-preview-media {
        display: flex; align-items: center; justify-content: center;
        min-height: 105px; max-height: 155px; background: #e9ecef;
      }
      .mar-file-preview-media img {
        display: block; width: 100%; height: 145px; object-fit: contain;
      }
      .mar-file-preview-media iframe {
        display: block; width: 100%; height: 150px; border: 0;
      }
      .mar-file-preview-actions { display: flex; gap: .4rem; padding: .45rem .55rem; }
      #marFilePreviewModal .modal-body { min-height: 70vh; background: #20242a; }
      #marFilePreviewModal .mar-modal-image {
        display: block; max-width: 100%; max-height: 76vh; margin: auto; object-fit: contain;
      }
      #marFilePreviewModal iframe { width: 100%; height: 76vh; border: 0; background: #fff; }
    `;
    document.head.appendChild(style);
  }

  function ensureModal() {
    let modal = document.getElementById("marFilePreviewModal");
    if (modal) return modal;
    modal = document.createElement("div");
    modal.className = "modal fade";
    modal.id = "marFilePreviewModal";
    modal.tabIndex = -1;
    modal.setAttribute("aria-hidden", "true");
    modal.innerHTML = `
      <div class="modal-dialog modal-xl modal-dialog-centered">
        <div class="modal-content">
          <div class="modal-header">
            <div>
              <h5 class="modal-title mb-0">Vista previa del comprobante</h5>
              <div class="small text-muted js-preview-modal-name"></div>
            </div>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Cerrar"></button>
          </div>
          <div class="modal-body d-flex align-items-center justify-content-center p-2"></div>
        </div>
      </div>`;
    document.body.appendChild(modal);
    return modal;
  }

  function openLargePreview(file, url) {
    const modal = ensureModal();
    modal.querySelector(".js-preview-modal-name").textContent = `${file.name} · ${formatSize(file.size)}`;
    const body = modal.querySelector(".modal-body");
    body.innerHTML = "";
    if ((file.type || "").startsWith("image/") || /\.(png|jpe?g|webp|gif|bmp)$/i.test(file.name)) {
      const image = document.createElement("img");
      image.className = "mar-modal-image";
      image.alt = `Vista previa de ${file.name}`;
      image.src = url;
      body.appendChild(image);
    } else {
      const frame = document.createElement("iframe");
      frame.title = `Vista previa de ${file.name}`;
      frame.src = `${url}#toolbar=1&navpanes=0`;
      body.appendChild(frame);
    }
    bootstrap.Modal.getOrCreateInstance(modal).show();
  }

  function ensureContainer(input) {
    let container = input.parentElement?.querySelector(
      `.mar-file-preview[data-preview-for="${input.dataset.previewId || ""}"]`
    );
    if (container) return container;
    const previewId = input.dataset.previewId || `mar-preview-${Math.random().toString(36).slice(2)}`;
    input.dataset.previewId = previewId;
    container = document.createElement("div");
    container.className = "mar-file-preview d-none";
    container.dataset.previewFor = previewId;
    input.insertAdjacentElement("afterend", container);
    return container;
  }

  function clearPreview(input, clearInput) {
    const oldUrl = previewUrls.get(input);
    if (oldUrl) URL.revokeObjectURL(oldUrl);
    previewUrls.delete(input);
    if (clearInput) input.value = "";
    const container = ensureContainer(input);
    container.innerHTML = "";
    container.classList.add("d-none");
  }

  function renderPreview(input) {
    clearPreview(input, false);
    const file = input.files?.[0];
    if (!file) return;
    const container = ensureContainer(input);
    if (!isSupportedFile(file)) {
      container.innerHTML = `<div class="alert alert-warning py-2 px-3 mb-0 small">No se puede mostrar este tipo de archivo.</div>`;
      container.classList.remove("d-none");
      return;
    }

    const url = URL.createObjectURL(file);
    previewUrls.set(input, url);
    const isImage = (file.type || "").startsWith("image/") || /\.(png|jpe?g|webp|gif|bmp)$/i.test(file.name);
    container.innerHTML = `
      <div class="mar-file-preview-card">
        <div class="mar-file-preview-head">
          <div class="min-w-0">
            <div class="mar-file-preview-name"></div>
            <div class="mar-file-preview-meta"></div>
          </div>
          <span class="badge bg-success">Listo</span>
        </div>
        <div class="mar-file-preview-media"></div>
        <div class="mar-file-preview-actions">
          <button type="button" class="btn btn-sm btn-primary js-preview-open">Ver en grande</button>
          <button type="button" class="btn btn-sm btn-outline-danger js-preview-clear">Quitar</button>
        </div>
      </div>`;
    container.querySelector(".mar-file-preview-name").textContent = file.name;
    container.querySelector(".mar-file-preview-meta").textContent = `${isImage ? "Imagen" : "PDF"} · ${formatSize(file.size)}`;
    const media = container.querySelector(".mar-file-preview-media");
    if (isImage) {
      const image = document.createElement("img");
      image.alt = `Vista previa de ${file.name}`;
      image.src = url;
      media.appendChild(image);
    } else {
      const frame = document.createElement("iframe");
      frame.title = `Vista previa de ${file.name}`;
      frame.src = `${url}#toolbar=0&navpanes=0`;
      media.appendChild(frame);
    }
    container.querySelector(".js-preview-open").addEventListener("click", () => openLargePreview(file, url));
    container.querySelector(".js-preview-clear").addEventListener("click", () => {
      clearPreview(input, true);
      input.dispatchEvent(new Event("change", { bubbles: true }));
    });
    container.classList.remove("d-none");
  }

  function initialize(root) {
    const inputs = [];
    if (root instanceof HTMLInputElement) inputs.push(root);
    if (root?.querySelectorAll) inputs.push(...root.querySelectorAll('input[type="file"]'));
    inputs.filter(isSupportedInput).forEach((input) => {
      if (initializedInputs.has(input)) {
        if (!input.files?.length) clearPreview(input, false);
        return;
      }
      initializedInputs.add(input);
      if (input.nextElementSibling?.classList.contains("mar-file-preview")) {
        input.nextElementSibling.remove();
      }
      delete input.dataset.previewId;
      input.dataset.previewReady = "1";
      ensureContainer(input);
      input.addEventListener("change", () => renderPreview(input));
      if (input.files?.length) renderPreview(input);
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    ensureStyles();
    ensureModal();
    initialize(document);
    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => mutation.addedNodes.forEach((node) => {
        if (node instanceof Element) initialize(node);
      }));
    });
    observer.observe(document.body, { childList: true, subtree: true });
  });

  window.MARFilePreview = { initialize, renderPreview, clearPreview };
})();
