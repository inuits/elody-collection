from enum import StrEnum


# fmt: off
class TechnicalOrigins(StrEnum):
    ORIGINAL  = "original"
    TRANSCODE = "transcode"
    DOWNLOAD  = "download"
    OCR       = "ocr"
    THUMBNAIL = "thumbnail"
# fmt: on
