def build(skip: int) -> list[dict]:
    return [{"$skip": skip}] if skip else []
