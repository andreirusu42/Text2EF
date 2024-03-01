class Field:
    def __init__(self, name: str, alias: str = None, parent: str = None):
        self.name = name
        self.alias = alias
        self.parent = parent

    def __repr__(self):
        return f'Field(name={self.name}, alias={self.alias}, parent={self.parent})'
