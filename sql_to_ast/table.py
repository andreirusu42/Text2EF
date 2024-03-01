class Table:
    def __init__(self, name: str, alias: str = None):
        self.name = name
        self.alias = alias

    def __repr__(self):
        return f'Table(name={self.name}, alias={self.alias})'
