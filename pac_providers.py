from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import requests


class PacNotConfiguredError(RuntimeError):
    pass


@dataclass
class PacResult:
    ok: bool
    uuid: str | None = None
    xml: str | None = None
    pdf_bytes: bytes | None = None
    message: str = ""
    raw: dict[str, Any] | None = None


class BasePacProvider:
    name = "BASE"

    def timbrar(self, factura) -> PacResult:
        raise NotImplementedError

    def cancelar(self, factura, motivo: str = "02") -> PacResult:
        raise NotImplementedError


class PendingPacProvider(BasePacProvider):
    name = "PENDIENTE"

    def timbrar(self, factura) -> PacResult:
        return PacResult(
            ok=False,
            message="Falta configurar credenciales del PAC. La factura quedo como borrador lista para timbrar.",
        )

    def cancelar(self, factura, motivo: str = "02") -> PacResult:
        return PacResult(ok=False, message="Falta configurar credenciales del PAC para cancelar.")


class FacturamaProvider(BasePacProvider):
    name = "FACTURAMA"

    def __init__(self, username: str | None = None, password: str | None = None, sandbox: bool = True):
        self.username = username or os.getenv("FACTURAMA_USER", "").strip()
        self.password = password or os.getenv("FACTURAMA_PASSWORD", "").strip()
        self.base_url = (
            "https://apisandbox.facturama.mx"
            if sandbox
            else "https://api.facturama.mx"
        )

    def _ensure_configured(self) -> None:
        if not self.username or not self.password:
            raise PacNotConfiguredError("Configura FACTURAMA_USER y FACTURAMA_PASSWORD.")

    def timbrar(self, factura) -> PacResult:
        self._ensure_configured()
        # La forma exacta del payload depende del alta del emisor en Facturama.
        # Dejamos la clase preparada y evitamos timbrar hasta tener credenciales sandbox.
        return PacResult(
            ok=False,
            message="Facturama esta preparado, pero falta mapear el payload final con las credenciales sandbox.",
        )

    def cancelar(self, factura, motivo: str = "02") -> PacResult:
        self._ensure_configured()
        if not factura.uuid:
            return PacResult(ok=False, message="La factura no tiene UUID para cancelar.")
        response = requests.delete(
            f"{self.base_url}/api-lite/3/cfdis/{factura.uuid}",
            auth=(self.username, self.password),
            timeout=30,
        )
        if response.ok:
            return PacResult(ok=True, message="Solicitud de cancelacion enviada.", raw=response.json() if response.content else {})
        return PacResult(ok=False, message=response.text[:1000], raw={"status_code": response.status_code})


def get_pac_provider(config) -> BasePacProvider:
    if not config:
        return PendingPacProvider()
    pac = (config.pac or "").upper()
    sandbox = (config.pac_ambiente or "SANDBOX").upper() != "PRODUCCION"
    if pac == "FACTURAMA":
        return FacturamaProvider(username=config.pac_usuario, sandbox=sandbox)
    return PendingPacProvider()
