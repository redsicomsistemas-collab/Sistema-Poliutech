from __future__ import annotations


def can_access_contabilidad(user) -> bool:
    """Contabilidad es privada para Marco y para usuarios administradores."""
    if not getattr(user, "is_authenticated", False):
        return False
    role = (getattr(user, "rol", "") or "").strip().upper()
    identities = {
        (getattr(user, "nombre", "") or "").strip().casefold(),
        (getattr(user, "nombre_visible", "") or "").strip().casefold(),
    }
    return role == "ADMIN" or "marco" in identities or any(
        identity.startswith("marco ") for identity in identities
    )
