from typing import TypedDict


class KeycloakClientRepresentation(TypedDict):
    clientId: str
    name: str
    serviceAccountsEnabled: bool
