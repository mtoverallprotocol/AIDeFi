from . import settings as settings
settings = settings.settings()

frameworkInfo = settings.getFrameworkInfo()
CONNECTION_STRING = frameworkInfo['ConnectionString']['connectionString']
ETHERSCAN_APIKEY = frameworkInfo['APIKeys']['etherscan']
BSCSCAN_APIKEY = frameworkInfo['APIKeys']['bscscan']
BITQUERY_APIKEY = frameworkInfo['APIKeys']['bitquery']
CLASSES_FOLDER = frameworkInfo['Folders']['classes']
CONTRACTS_FOLDER = frameworkInfo['Folders']['contracts']
TRANSACTIONS_FOLDER = frameworkInfo['Folders']['transactions']
LISTENERS_FOLDER = frameworkInfo['Folders']['listeners']

class listeners:
    
    def ___init__(self, name):
        self.name = name
        
    def newListener(self, _ID, _listenerNameList, _sleepList = [0], _sleepAfter = 0):
        
        import time
        
        # Fill _sleepList if needed
        
        if len(_sleepList) < len(_listenerNameList):
            last = _sleepList[len(_sleepList) - 1]
            for i in range(0, len(_listenerNameList) - len(_sleepList)):
                _sleepList.append(last)
                
        import os.path
        
        listenerID = _ID
        openListener = '{}/{}.listener'.format(LISTENERS_FOLDER, listenerID) # only for the first call of the function
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
                if openState == 1: 
                    index = 0
                    for listener in _listenerNameList:
                        self.__runListener(listener, _sleepAfter)
                        #globals()['self.__{0}'.format(listener)]()
                        time.sleep(_sleepList[index]) # seconds
                    time.sleep(_sleepAfter)
                    
    def __runListener(self, _listenerName, _sleepAfter = 0):
        
        if _listenerName == 'test': self.test()
        if _listenerName == 'test2': self.test2()
        if _listenerName == 'test3': self.test3()

    
    def __deleteListener(self, _listenerName):
        try:
            os.remove('{}/{}.listener'.format(LISTENERS_FOLDER, _listenerName))
        except:
            print('Listener not found')
        
    def __createWaiter(self, _IDWaiter):
        f = open('{}/{}.wait'.format(LISTENERS_FOLDER, _IDWaiter),'w')
        f.close()
        
    def __deleteWaiter(self, _IDWaiter):
        import os
        try:
            os.remove('{}/{}.wait'.format(LISTENERS_FOLDER, _IDWaiter))
        except:
            print('Waiter {} not found'.format(_IDWaiter))
        
    def __checkWaiter(self, _IDWaiter, _sleepAfter = 0):
        import os
        import time

        print('Waiting {}'.format(_IDWaiter))
        fileExist = False
        while not fileExist:
            try:
                fileExist = os.path.isfile('{}/{}.wait'.format(LISTENERS_FOLDER, _IDWaiter))
                time.sleep(_sleepAfter)
            except:
                fileExist = False
        print('Waiter {} available'.format(_IDWaiter))
        return True
    
    ############################ LIST OF CALLABLE LISTENERS ########################
    
    def test(self):
        print('test')
        self.__createWaiter('waiter1')
        
    def test2(self):
        print('test2')
        
    def test3(self):
        self.__checkWaiter('waiter1')
        print('test3')
        self.__deleteWaiter('waiter1')
        
        
        