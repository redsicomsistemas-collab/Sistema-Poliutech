from __future__ import annotations


def _is_authorized_account(user) -> bool:
    identities = {
        (getattr(user, "nombre", "") or "").strip().casefold(),
        (getattr(user, "nombre_visible", "") or "").strip().casefold(),
        (getattr(user, "correo", "") or "").strip().casefold(),
    }
    allowed_names = ("marco", "mescalera", "hjaramillo", "hansel")
    allowed_emails = {
        "mescalera@poliutech.com",
        "hjaramillo@poliutech.com",
    }
    return bool(identities & allowed_emails) or any(
        identity == allowed or identity.startswith(f"{allowed} ")
        for identity in identities
        for allowed in allowed_names
    )


def can_access_contabilidad(user) -> bool:
    """Contabilidad es privada para Marco, Mescalera, Hjaramillo y administradores."""
    if not getattr(user, "is_authenticated", False):
        return False
    role = (getattr(user, "rol", "") or "").strip().upper()
    return role == "ADMIN" or _is_authorized_account(user)


def can_manage_contabilidad(user) -> bool:
    """Las cuentas autorizadas pueden crear, editar y eliminar todo el módulo."""
    return can_access_contabilidad(user)
