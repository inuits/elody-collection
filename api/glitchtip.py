import sentry_sdk

from os import getenv


def init_glitchtip(app, api):

    if getenv("GLITCH_TIP_DSN"):
        __init_glitch_tip(
            glitchtip_dsn=getenv("GLITCH_TIP_DSN"),
            http_proxy=getenv("GLITCH_TIP_HTTP_PROXY"),
            https_proxy=getenv("GLITCH_TIP_HTTPS_PROXY"),
            traces_sample_rate=0,
        )


def __init_glitch_tip(
    glitchtip_dsn,
    http_proxy=None,
    https_proxy=None,
    traces_sample_rate=None,
    custom_env_name=None,
    debug=False,
    exception_only=True,
):
    def before_send(event, hint):
        if exception_only:
            if event.get("level") == "error" and "exception" in event:
                return event
            return None
            if "exc_info" in hint:
                exc_type, exc_value, tb = hint["exc_info"]
                status_code = getattr(exc_value, "code", None)

                if status_code:
                    if 400 <= status_code < 500:
                        return None
        return event

    sentry_sdk.init(
        dsn=glitchtip_dsn,
        integrations=[],
        traces_sample_rate=1.0 if traces_sample_rate is None else traces_sample_rate,
        environment=(
            getenv("GLITCH_TIP_ENVIRONMENT", "env")
            if not custom_env_name
            else custom_env_name
        ),
        release=getenv("RELEASE_COMMIT_SHA", None) or "unknown",
        debug=debug,
        http_proxy=http_proxy,
        https_proxy=https_proxy,
        before_send=before_send,
    )
