class contractEvents:
    
    def ___init__(self, name):
        self.name = name
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to save events name in an array from ABI
    # Input: ABI of a smart contract
    # Output: array with events name
    
    def getEventsFromABI(self, _ABI):
        
        import re
        
        events = []
        for match_type in re.finditer(r'\"\,\"type":\"event\"', _ABI):
            for match_name in re.finditer(r'\"name\"\:\"', _ABI[0:match_type.start()]):
                True
                #no-op
            events.append(_ABI[match_name.end():match_type.start()])
        return events
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to have an array with callable smart contract events
    # Input: contract address, contract datatype
    # Output: array with callable smart contract events  (event list are in the same order of data/smartcontracts/{contract address}/events.csv)
            
    def eventList(self, _contract):
        contractName = _contract.address
        contractName = contractName.lower()
        
        eventList = []
        
        # dynamic.generateDynamicSmartContractEventsAndFunctions was executed when the smart contract folder was created
        from past.builtins import execfile
        execfile('classes/dynamicEvents.py')
            
        return eventList