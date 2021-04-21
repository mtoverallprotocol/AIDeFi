# All sql are defined here to centralize the core-logic of analysis

class sqlQueries:
    def ___init__(self, name):
        self.name = name
    
    def selectAll(self, _dataFrame):
        return "Select * from " + _dataFrame