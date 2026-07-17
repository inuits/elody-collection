from abc import ABC, abstractmethod

from elody_types.base_entity_types import ElodyEntity


class BaseIdpService(ABC):
    @abstractmethod
    def create_user(self, user: ElodyEntity, serializer) -> str:
        """Create user in IDP

        Args:
            user: user in elody representation: dict
            seralizer: serializer class with "from_elody_to_<idp>" method

        Returns:
            idp user id
        """

    @abstractmethod
    def send_user_activation_email(self, user_id: str, redirect_suffix=None):
        """Send user activation email

        Args:
            user_id: user id in the idp: str
            redirect_suffix: optional suffix to the redirect-uri to send users to a specific location in elody
        """

    @abstractmethod
    def update_user(self, user: ElodyEntity, serializer) -> str:
        """Update user in IDP and return user id.

        Args:
            user: user in elody represtentation: dict
            seralizer: serializer class with "from_elody_to_<idp>" method

        Returns:
            user_id
        """

    @abstractmethod
    def get_user(self, user: ElodyEntity, serializer) -> dict:
        """Get user from idp.

        Args:
            user: user in elody represtentation: dict
            seralizer: serializer class with "from_elody_to_<idp>" method

        Returns:
            Idp user represtentation: dict
        """

    @abstractmethod
    def disable_user(self, user: ElodyEntity, serializer):
        """Delete user from IDP.

        Args:
            user: user in elody representation: dict
            seralizer: serializer class with "from_elody_to_<idp>" method
        """

    @abstractmethod
    def delete_user(self, user: ElodyEntity, serializer):
        """Delete user from IDP.

        Args:
            user: user in elody representation: dict
            seralizer: serializer class with "from_elody_to_<idp>" method
        """

    @abstractmethod
    def create_client_and_set_email(
        self, client_repr, partner_email: str, roles: list[str] | None = None
    ) -> str:
        """Create client in IDP and set the underlying user email

        Args:
            client: Client representation for idp: dict
            partner_email: email for underlying user email: str
        """
