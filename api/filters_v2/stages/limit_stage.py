def build(limit: int) -> list[dict]:
    return [{"$limit": limit}] if limit else []
