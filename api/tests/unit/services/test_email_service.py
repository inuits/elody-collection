"""Unit tests for the BaseEmailService."""

from email.message import EmailMessage
from enum import StrEnum
from unittest.mock import patch

import pytest
from services.emails import BaseEmailService, SmtpSecurity

SMTP_ENV_VARS = (
    "SMTP_SERVER_HOST",
    "SMTP_SERVER_PORT",
    "SMTP_FROM_EMAIL",
    "SMTP_USERNAME",
    "SMTP_PASSWORD",
    "SMTP_SECURITY",
    "SMTP_USE_TLS",
)


@pytest.fixture(autouse=True)
def clean_smtp_env(monkeypatch):
    for env_var in SMTP_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)


@pytest.fixture
def smtplib_mock():
    with patch("services.emails.email_service.smtplib") as mock:
        yield mock


def sent_message(server_class) -> EmailMessage:
    server = server_class.return_value.__enter__.return_value
    server.send_message.assert_called_once()
    return server.send_message.call_args.args[0]


@pytest.fixture
def template_service(tmp_path):
    (tmp_path / "greeting.html").write_text(
        '<p>Hello {{ name }}</p><a href="{{ link }}">Open</a>'
    )

    class TemplateEmailService(BaseEmailService):
        template_dir = str(tmp_path)

    return TemplateEmailService


class TestConfiguration:
    def test_defaults_without_env(self):
        service = BaseEmailService()

        assert service._smtp_server == "mailpit"
        assert service._smtp_port == 1025
        assert service._sender_email == "noreply@elody.eu"
        assert service._smtp_username is None
        assert service._smtp_password is None
        assert service._security == SmtpSecurity.NONE

    def test_reads_env(self, monkeypatch):
        monkeypatch.setenv("SMTP_SERVER_HOST", "smtp.example.com")
        monkeypatch.setenv("SMTP_SERVER_PORT", "587")
        monkeypatch.setenv("SMTP_FROM_EMAIL", "env@example.com")
        monkeypatch.setenv("SMTP_USERNAME", "user")
        monkeypatch.setenv("SMTP_PASSWORD", "secret")
        monkeypatch.setenv("SMTP_SECURITY", "starttls")

        service = BaseEmailService()

        assert service._smtp_server == "smtp.example.com"
        assert service._smtp_port == 587
        assert service._sender_email == "env@example.com"
        assert service._smtp_username == "user"
        assert service._smtp_password == "secret"
        assert service._security == SmtpSecurity.STARTTLS

    def test_arguments_override_env(self, monkeypatch):
        monkeypatch.setenv("SMTP_SERVER_HOST", "smtp.example.com")
        monkeypatch.setenv("SMTP_FROM_EMAIL", "env@example.com")
        monkeypatch.setenv("SMTP_SECURITY", "starttls")

        service = BaseEmailService(
            smtp_server="other.example.com",
            smtp_port=2525,
            sender_email="arg@example.com",
            security=SmtpSecurity.SSL,
        )

        assert service._smtp_server == "other.example.com"
        assert service._smtp_port == 2525
        assert service._sender_email == "arg@example.com"
        assert service._security == SmtpSecurity.SSL

    def test_subclass_default_sender_email(self, monkeypatch):
        class ClientEmailService(BaseEmailService):
            default_sender_email = "client@example.com"

        assert ClientEmailService()._sender_email == "client@example.com"

        monkeypatch.setenv("SMTP_FROM_EMAIL", "env@example.com")
        assert ClientEmailService()._sender_email == "env@example.com"

    @pytest.mark.parametrize("value", ["true", "True", "TRUE"])
    def test_legacy_use_tls_means_ssl(self, monkeypatch, value):
        monkeypatch.setenv("SMTP_USE_TLS", value)

        assert BaseEmailService()._security == SmtpSecurity.SSL

    def test_legacy_use_tls_false_means_none(self, monkeypatch):
        monkeypatch.setenv("SMTP_USE_TLS", "false")

        assert BaseEmailService()._security == SmtpSecurity.NONE

    def test_security_env_takes_precedence_over_legacy_use_tls(self, monkeypatch):
        monkeypatch.setenv("SMTP_USE_TLS", "true")
        monkeypatch.setenv("SMTP_SECURITY", "starttls")

        assert BaseEmailService()._security == SmtpSecurity.STARTTLS

    def test_invalid_security_raises(self, monkeypatch):
        monkeypatch.setenv("SMTP_SECURITY", "bogus")

        with pytest.raises(ValueError):
            BaseEmailService()


class TestSendEmail:
    def test_builds_multipart_message(self, smtplib_mock):
        service = BaseEmailService(sender_email="noreply@example.com")

        assert service.send_email(
            "to@example.com",
            "Subject",
            '<html><body><p>Hi &amp; bye</p><a href="https://x.y">link</a></body></html>',
        )

        message = sent_message(smtplib_mock.SMTP)
        assert message["From"] == "noreply@example.com"
        assert message["To"] == "to@example.com"
        assert message["Subject"] == "Subject"
        assert message["Message-ID"].endswith("@example.com>")
        plain = message.get_body(("plain",)).get_content()
        assert "Hi & bye" in plain
        assert "link: https://x.y" in plain
        assert "<p>" in message.get_body(("html",)).get_content()

    def test_plain_connection_without_credentials_skips_login(self, smtplib_mock):
        service = BaseEmailService(smtp_server="host", smtp_port=25)

        assert service.send_email("to@example.com", "Subject", "<p>Hi</p>")

        smtplib_mock.SMTP.assert_called_once_with("host", 25)
        smtplib_mock.SMTP_SSL.assert_not_called()
        server = smtplib_mock.SMTP.return_value.__enter__.return_value
        server.starttls.assert_not_called()
        server.login.assert_not_called()

    def test_logs_in_when_credentials_are_set(self, smtplib_mock):
        service = BaseEmailService(smtp_username="user", smtp_password="secret")

        assert service.send_email("to@example.com", "Subject", "<p>Hi</p>")

        server = smtplib_mock.SMTP.return_value.__enter__.return_value
        server.login.assert_called_once_with("user", "secret")

    def test_starttls(self, smtplib_mock):
        service = BaseEmailService(
            smtp_server="host",
            smtp_port=587,
            smtp_username="user",
            smtp_password="secret",
            security=SmtpSecurity.STARTTLS,
        )

        assert service.send_email("to@example.com", "Subject", "<p>Hi</p>")

        smtplib_mock.SMTP.assert_called_once_with("host", 587)
        server = smtplib_mock.SMTP.return_value.__enter__.return_value
        call_names = [name for name, *_ in server.mock_calls]
        assert call_names.index("starttls") < call_names.index("login")
        server.login.assert_called_once_with("user", "secret")

    def test_ssl(self, smtplib_mock):
        service = BaseEmailService(
            smtp_server="host", smtp_port=465, security=SmtpSecurity.SSL
        )

        assert service.send_email("to@example.com", "Subject", "<p>Hi</p>")

        smtplib_mock.SMTP_SSL.assert_called_once_with("host", 465)
        smtplib_mock.SMTP.assert_not_called()
        sent_message(smtplib_mock.SMTP_SSL)

    def test_returns_false_on_failure(self, smtplib_mock):
        smtplib_mock.SMTP.side_effect = OSError("connection refused")

        assert not BaseEmailService().send_email("to@example.com", "S", "<p>Hi</p>")


class TestTemplates:
    def test_render_uses_template_dir_and_autoescapes(self, template_service):
        html = template_service.render_html_template(
            "greeting.html", {"name": "<b>Ann</b>", "link": "https://x.y"}
        )

        assert "Hello &lt;b&gt;Ann&lt;/b&gt;" in html
        assert 'href="https://x.y"' in html

    def test_render_accepts_str_enum(self, template_service):
        class TemplateName(StrEnum):
            GREETING = "greeting.html"

        html = template_service.render_html_template(
            TemplateName.GREETING, {"name": "Ann", "link": ""}
        )

        assert "Hello Ann" in html

    def test_send_template_email(self, template_service, smtplib_mock):
        service = template_service()

        assert service.send_template_email(
            "to@example.com", "Subject", "greeting.html", {"name": "Ann", "link": ""}
        )

        message = sent_message(smtplib_mock.SMTP)
        assert "Hello Ann" in message.get_body(("html",)).get_content()
