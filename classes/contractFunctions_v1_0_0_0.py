import re
    
class contractFunctions:
    
    def ___init__(self, name):
        self.name = name
        
        
        
    #################################################################################################################
    
    # @dev: This function allow us to save function names in an array from ABI
    # Input: ABI of a smart contract
    # Output: array with events name
    
    def getFunctionsFromABI(self, _ABI):
        
        functions = []
        for match_output in re.finditer(r'\"\,\"outputs', _ABI):
            for match_name in re.finditer(r'\"name\"\:\"', _ABI[0:match_output.start()]):
                True
                #no-op
            functions.append(_ABI[match_name.end():match_output.start()])
        return functions
    