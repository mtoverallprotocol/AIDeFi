from past.builtins import execfile
from . import loadVersioning as loadVersioning
loadVersioning = loadVersioning.loadVersioning()

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
    
    def getFunctionsFromSmartContract(self, _contract):
        
        
        try:
            functions = list(filter(lambda k: '__' not in k, dir(_contract.functions))) 
        except:
            functions = []
            
        return functions
    
    def getFunctionsArgsFromPartialABI(self, _partialABI): #to do
        
        import re
        
        # INPUT
        
        functionInputList = []
        for match_output in re.finditer(r'\"inputs\"\:\[', _partialABI):
            for match_name in re.finditer(r'\]\,\"name\"\:\"', _partialABI[match_output.start():]):
                True
                #no-op
        functions = _partialABI[match_output.end():match_name.start()]
        indexInternalType = 0
        indexName = 1
        indexType = 2
        indexMetadata = 0
        indexData = 1
        elements = int((len(functions.split(",")) + 1) / 3) # 3-ple of elements for each input
        for inputElement in range(0, elements):
            functionInputList.append(functions.split(",")[inputElement * 3 + indexName].split(":")[indexData].replace('"', '').replace('}',''))

        # OUTPUT 
        
        functionOutputList = []
        for match_output in re.finditer(r'\"outputs\"\:\[', _partialABI):
            for match_name in re.finditer(r'\]\,\"payable\"\:', _partialABI[match_output.end():]):
                True
                #no-op
        functions = _partialABI[match_output.end():match_output.end()+match_name.start()]
        elements = int((len(functions.split(",")) + 1) / 3) # 3-ple of elements for each output
        for inputElement in range(0, elements):
            functionOutputList.append(functions.split(",")[inputElement * 3 + indexName].split(":")[indexData].replace('"', '').replace('}',''))

        return functionInputList, functionOutputList
    
    def getPartialABIFromFunctionName(self, _ABI, _functionName):
        
        import re

        for f in re.finditer(r'\"{0}\"'.format(_functionName), _ABI):
            True #no-op, take the last one

        start = f.start()
        end = f.end()

        for match_open in re.finditer(r'\"inputs\"\:', _ABI[:start]):
            True #no-op, take the last one

        for match_close in re.finditer(r'\"payable\"\:', _ABI[end:]):
            break #no-op, take the first one

        return _ABI[match_open.start()-1:f.end() + match_close.end()]

        
        #################################################################################################################
    
    # @dev: This function allow us to have an array with callable smart contract function. For now we will take only views on smart contract available functions
    # Input: contract address, function name (the same as we have ibsude the csv file), arguments (array with undefined number of values to put such as inputs of our callable function)
    # Output: the result of smart contract function)
            
    
    def callFunction(self, _ABI, _functionName, args):
        
        from . import dynamics_v1_0_0_0 as dynamics #dynimics loader doesn't work here, check why!
        dynamics = dynamics.dynamics()
        
        dynamics.generateCallableFunction(_ABI, _functionName, args)
        
        execfile('classes/smartContracts/dynamicFunction_{0}.py'.format(_functionName))
        
        
    def listenFunction(self, _contract, _functionName, _sleepTime): #start and stop with I/O input
        import time
        import os.path
        
        listenerID = _functionName
        openListener = 'classes/listeners/open_{}.listener'.format(listenerID) # only for the first call of the function
        openState = 0

        f = open(openListener,'w')
        f.close()
        isLoop = True

        while isLoop:
            try:
                f = open(openListener,'r')
                f.close()
                openState = 1
            except:
                isLoop = False
                print('Listener Stopped')
                openState = 2
            finally:
                if openState == 1: #the file was removed
                    print(_contract.functions[_functionName]().call()) #do something
                    time.sleep(_sleepTime)
