class FilterOption:
    def __init__(self, label: str, value: str):
        self.label = label
        self.value = value

    def __hash__(self):
        return hash(str(self.to_dict()))

    def __eq__(self, other):
        return self.label == other.label and self.value == other.value

    def to_dict(self):
        return {"label": self.label, "value": self.value}
