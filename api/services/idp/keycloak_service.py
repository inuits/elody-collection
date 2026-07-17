from os import getenv
from typing import Any, Protocol, cast

from elody_types.base_entity_types import ElodyEntity
from keycloak import KeycloakAdmin
from keycloak.exceptions import KeycloakPostError
from services.idp.base_idp_service import BaseIdpService
from services.idp.types import KeycloakClientRepresentation


class ToKeycloakSerializer(Protocol):
    def from_elody_to_keycloak(
        self, document: ElodyEntity, *, extra_attributes: dict[str, Any] | None = None
    ) -> dict: ...


class KeycloakService(BaseIdpService):
    def __init__(self):
        self._service_client_id = getenv("SERVICE_CLIENT_ID", "service_client")
        self._service_client_secret = getenv("SERVICE_CLIENT_SECRET")
        self._server_url = getenv("KEYCLOAK_BASE_URL")
        self._realm_name = getenv("REALM_NAME")
        self._redirect_uri = getenv("REDIRECT_URI") or getenv("DAMS_FRONTEND_URL")
        self._client_id = getenv("OAUTH_CLIENT_ID", "podiumnet-dashboard-elody")
        self._keycloak_admin = KeycloakAdmin(
            server_url=self._server_url,
            realm_name=self._realm_name,
            client_id=self._service_client_id,
            client_secret_key=self._service_client_secret,
            verify=True,
        )
        self._idp_name = "keycloak"

    def create_user(
        self, user: ElodyEntity, serializer: type[ToKeycloakSerializer]
    ) -> str:
        user_serializer = serializer().from_elody_to_keycloak
        return self._keycloak_admin.create_user(user_serializer(user))

    def update_user(
        self,
        user: ElodyEntity,
        serializer: type[ToKeycloakSerializer],
        extra_attributes: dict[str, Any] | None = None,
    ):
        user_serializer = serializer().from_elody_to_keycloak
        serialized_user = user_serializer(user, extra_attributes=extra_attributes)
        user_id = self._keycloak_admin.get_user_id(serialized_user["email"])
        if user_id:
            self._keycloak_admin.update_user(user_id, serialized_user)
            return user_id

    def get_user(self, user: ElodyEntity, serializer: type[ToKeycloakSerializer]):
        user_serializer = serializer().from_elody_to_keycloak
        user_id = self._keycloak_admin.get_user_id(
            user_serializer(user)["username"],
        )
        if user_id:
            return self._keycloak_admin.get_user(
                user_id,
            )

    def disable_user(self, user: ElodyEntity, serializer: type[ToKeycloakSerializer]):
        user_serializer = serializer().from_elody_to_keycloak
        user_id = self._keycloak_admin.get_user_id(
            user_serializer(user)["username"],
        )
        if user_id:
            self._keycloak_admin.disable_user(
                user_id,
            )

    def delete_user(self, user: ElodyEntity, serializer: type[ToKeycloakSerializer]):
        user_serializer = serializer().from_elody_to_keycloak
        user_id = self._keycloak_admin.get_user_id(
            user_serializer(user)["username"],
        )
        if user_id:
            self._keycloak_admin.delete_user(
                user_id,
            )

    def send_user_activation_email(self, user_id: str, redirect_suffix=None):
        if redirect_suffix:
            redirect_uri = f"{self._redirect_uri}/{redirect_suffix}"
        else:
            redirect_uri = self._redirect_uri
        self._keycloak_admin.send_update_account(
            user_id,
            ["VERIFY_EMAIL", "UPDATE_PASSWORD"],
            self._client_id,
            redirect_uri=redirect_uri,
        )

    def create_client_and_set_email(
        self,
        client_repr: KeycloakClientRepresentation,
        partner_email: str,
        roles: list[str] | None = None,
    ) -> str:

        try:
            client_uuid = self._keycloak_admin.create_client(cast(dict, client_repr))
        except KeycloakPostError as e:
            if e.response_code == 409:
                return self._keycloak_admin.create_client(
                    cast(dict, client_repr), skip_exists=True
                )
            else:
                raise e
        if not client_uuid:
            raise Exception("Did not receive client uuid")

        service_account_user_rep = self._keycloak_admin.get_client_service_account_user(
            client_uuid
        )
        service_account_user_id = service_account_user_rep["id"]

        service_account_user_rep["email"] = partner_email

        self._keycloak_admin.update_user(
            service_account_user_id, service_account_user_rep
        )

        roles_resolved = []
        if roles:
            # We have to resolve the roles to their actual role I'm pretty sure
            dashboard_client_uuid = self._keycloak_admin.get_client_id(self._client_id)
            if not dashboard_client_uuid:
                raise Exception
            for role in roles:
                client_role = self._keycloak_admin.get_client_role(
                    dashboard_client_uuid, role
                )
                if client_role:
                    roles_resolved.append(client_role)

            if roles_resolved:
                self._keycloak_admin.assign_client_role(
                    service_account_user_id, dashboard_client_uuid, roles_resolved
                )

        return client_uuid
