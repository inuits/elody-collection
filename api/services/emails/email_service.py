import re
import smtplib
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from functools import cache
from html import unescape
from os import getenv

from elody.util import get_boolean_env
from jinja2 import Environment, FileSystemLoader
from logging_elody.log import log

from .constants import SmtpSecurity


@cache
def _get_template_environment(template_dir: str) -> Environment:
    return Environment(loader=FileSystemLoader(template_dir), autoescape=True)


def _get_security_from_env() -> SmtpSecurity:
    if security := getenv("SMTP_SECURITY"):
        return SmtpSecurity(security.lower())
    # NOTE: SMTP_USE_TLS is kept for backwards compatibility, it always meant implicit TLS
    if get_boolean_env("SMTP_USE_TLS", False):
        return SmtpSecurity.SSL
    return SmtpSecurity.NONE


class BaseEmailService:
    template_dir: str = "/app/assets/html"
    default_sender_email: str = "noreply@elody.eu"

    def __init__(
        self,
        smtp_server: str | None = None,
        smtp_port: int | None = None,
        sender_email: str | None = None,
        smtp_username: str | None = None,
        smtp_password: str | None = None,
        security: SmtpSecurity | None = None,
    ):
        self._smtp_server = smtp_server or getenv("SMTP_SERVER_HOST", "mailpit")
        self._smtp_port = smtp_port or int(getenv("SMTP_SERVER_PORT", "1025"))
        self._sender_email = (
            sender_email or getenv("SMTP_FROM_EMAIL") or self.default_sender_email
        )
        self._smtp_username = smtp_username or getenv("SMTP_USERNAME")
        self._smtp_password = smtp_password or getenv("SMTP_PASSWORD")
        self._security = security or _get_security_from_env()

    @staticmethod
    def _html_to_text(email_html: str) -> str:
        text = re.sub(
            r'<a\s[^>]*href="([^"]*)"[^>]*>(.*?)</a\s*>',
            r"\2: \1",
            email_html,
            flags=re.DOTALL,
        )
        text = re.sub(r"<(head|style)[^>]*>.*?</\1>", "", text, flags=re.DOTALL)
        text = re.sub(r"<[^>]+>", "", text)
        text = re.sub(r"[ \t]*\n[ \t]*", "\n", re.sub(r"[ \t]+", " ", unescape(text)))
        return re.sub(r"\s*\n\s*(\n\s*)+", "\n\n", text).strip()

    def send_email(self, recipient_email: str, subject: str, email_html: str) -> bool:
        msg = EmailMessage()
        msg["From"] = self._sender_email
        msg["To"] = recipient_email
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid(domain=self._sender_email.split("@")[-1])
        msg.set_content(self._html_to_text(email_html))
        msg.add_alternative(email_html, subtype="html")

        smtp_class = (
            smtplib.SMTP_SSL if self._security == SmtpSecurity.SSL else smtplib.SMTP
        )
        try:
            with smtp_class(self._smtp_server, self._smtp_port) as server:
                if self._security == SmtpSecurity.STARTTLS:
                    server.starttls()
                if self._smtp_username:
                    server.login(self._smtp_username, self._smtp_password or "")
                server.send_message(msg)
                return True
        except Exception as e:  # noqa: BLE001
            log.error(f"Failed to send email: {e}")
            return False

    def send_template_email(
        self,
        recipient_email: str,
        subject: str,
        template_name: str,
        template_vars: dict[str, str | list],
    ) -> bool:
        email_html = self.render_html_template(template_name, template_vars)
        return self.send_email(recipient_email, subject, email_html)

    @classmethod
    def render_html_template(
        cls, template_name: str, template_vars: dict[str, str | list]
    ) -> str:
        # NOTE: str() turns StrEnum members into plain names, jinja chokes on their repr
        template = _get_template_environment(cls.template_dir).get_template(
            str(template_name)
        )
        return template.render(**template_vars)
