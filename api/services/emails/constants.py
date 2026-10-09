from enum import StrEnum


# fmt: off
class SmtpSecurity(StrEnum):
    NONE     = "none"
    STARTTLS = "starttls"
    SSL      = "ssl"
# fmt: on
