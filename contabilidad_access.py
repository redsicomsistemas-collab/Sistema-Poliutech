from __future__ import annotations


def _is_marco(user) -> bool:
    identities = {
        (getattr(user, "nombre", "") or "").strip().casefold(),
        (getattr(user, "nombre_visible", "") or "").strip().casefold(),
    }
    return "marco" in identities or any(identity.startswith("marco ") for identity in identities)


def can_access_contabilidad(user) -> bool:
    """Contabilidad es privada para Marco y para usuarios administradores."""
    if not getattr(user, "is_authenticated", False):
        return False
    role = (getattr(user, "rol", "") or "").strip().upper()
    return role == "ADMIN" or _is_marco(user)


def can_manage_contabilidad(user) -> bool:
    """Marco y los administradores pueden crear, editar y eliminar todo el módulo."""
    return can_access_contabilidad(user)
