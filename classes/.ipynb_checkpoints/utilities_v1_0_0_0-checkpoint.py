import os
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

class utilities:
    
    def ___init__(self, name):
        self.name = name

    #################################################################################################################
    
    # @dev: Function that allow us to know if a text is hex or not without errors
    # Input: text to be analysed
    # Output: Boolean
    
    def is_hex(self, _arg):
        try:
            _arg.hex()
            return True
        except:
            return False
        
        
        
    #################################################################################################################
    
    # @dev: Function that allow us to know the index of an array where a specific regular expression (string) occours
    # Input: array list of elements, value to search
    # Output: Integer
    
    def linearReSearch(self, _arrayList, _valueToSearch):
        
        import regex as re
        
        result = -1
        index = 0
        for element in _arrayList:
            if(re.search("(\{0})".format(_valueToSearch), element)):
                result = index
            index = index + 1
        return result

    
    
    #################################################################################################################
    
    # @dev: Function that allow us to merge csv files
    # Input: source is the folder under the export one #####to be fixed, it can be for every folder
    # Output: merged file
    
    def mergeFiles(self, _source): #merge with fileList
        
        # OS libraries

        from datetime import datetime
        import os
        from os import listdir
        from os import path
        from os.path import isfile, join, isdir
        from datetime import datetime
        import sys

        import pandas as pd

        EXPORT_FOLDER = 'export'
        
        if(path.exists('{0}/{1}'.format(EXPORT_FOLDER, _source)) == False):
            os.mkdir('{0}/{1}'.format(EXPORT_FOLDER, _source))
        
        SAVE_FOLDER = '{0}/{1}'.format(EXPORT_FOLDER, _source)
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")

        export_files = [f for f in listdir(EXPORT_FOLDER) if isfile(join(EXPORT_FOLDER, f))]
        
        fileList = ['TRANSACTION_LIST', 'CUSTOMERS_LIST_FROM', 'CUSTOMERS_TRANSACTIONS_FROM']
        
        for indexFileList in fileList:
            checkedColumns = False
            for indexFile in export_files:
                if(indexFile[0:len(indexFileList)] == indexFileList):
                    if(checkedColumns):
                        df = pd.read_csv('{0}/{1}'.format(EXPORT_FOLDER, indexFile))
                        df_result = pd.concat([df_result, df])
                        os.remove('{0}/{1}'.format(EXPORT_FOLDER, indexFile))
                    else:
                        checkedColumns = True
                        df_result = pd.read_csv('{0}/{1}'.format(EXPORT_FOLDER, indexFile))
                        os.remove('{0}/{1}'.format(EXPORT_FOLDER, indexFile))
            try:
                df_result.drop_duplicates()
                df_result.to_csv('{0}/{1}_{2}_{3}.csv'.format(SAVE_FOLDER, indexFileList ,'MERGED', dt))
                print('File Merged at {0}/{1}_{2}_{3}.csv'.format(SAVE_FOLDER, indexFileList ,'MERGED', dt))
            except:
                print("No File Detected with prefix {0}".format(indexFileList))
                
    
    def createFolder(self, _folder):
        
        from os import path
        import os
        
        if(path.exists('{0}'.format(_folder)) == False):
            os.mkdir('{0}'.format(_folder))
            
    def include(filename):
        
        import os
        
        if os.path.exists(filename): 
            execfile(filename)
      
    
    
    #################################################################################################################
    
    # @dev: Function that allow us to know the framework info
    # Input: nothing
    # Output: Dictionary
    


    def plotLogs(self, _arrayColumn, _fileLogDf, _savePlot = False):
        columnsIndex = _arrayColumn
        plt.figure(figsize = (16,8))
        fig, axs = plt.subplots(len(columnsIndex), 1, figsize=(16, 5*len(columnsIndex)), sharex=True, sharey=False)
        i = 0
        columns = []
        cond = []
        names = []
        values = []

        for index in columnsIndex :
            try:
                columns.append(_fileLogDf.keys()[index])
                print(columns[i])
                cond.append(_fileLogDf[columns[i]].fillna(0).astype(float) > 0)
                names.append(list(_fileLogDf[cond[i]]['blockNumber'].astype(float)))
                values.append(list(_fileLogDf[cond[i]][columns[i]]))
                axs[i].scatter(names[i], values[i])
                axs[i].set_title(columns[i] + ' - min: ' + str(min(values[i])) + ' - max: ' + str(max(values[i]))) #add min max
                axs[i].axes.get_yaxis().set_visible(False)
                i = i + 1
            except:
                print('Error on: {}'.format(index))

        dt = datetime.now().strftime("%Y%m%d_%H%M%S")

        fig.suptitle('plotLogs_{}'.format(dt))
        
        if(_savePlot):
            fig.savefig('plotLogs_{}'.format(dt)) 
            
    def listener(self, _ID, _listenerNameList, _sleepList = [0], _sleepAfter = 0):
        
        import time
        
        # Fill _sleepList if needed
        
        if len(_sleepList) < len(_listenerNameList):
            last = _sleepList[len(_sleepList) - 1]
            for i in range(0, len(_listenerNameList) - len(_sleepList)):
                _sleepList.append(last)
                
        import os.path
        
        listenerID = _ID
        openListener = '{}/open_{}.listener'.format(LISTENERS_FOLDER, listenerID) # only for the first call of the function
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
                    index = 0
                    for listener in _listenerNameList:
                        self.__runListener(listener)
                        time.sleep(_sleepList[index]) # seconds
                    time.sleep(_sleepAfter)
                    
    def __runListener(self, _listenerName):
        
        if _listenerName == 'test': print("test")
    
    def __deleteListener(self, _listenerName):
        try:
            os.remove('{}.listener'.format(_listenerName))
        except:
            print('Listener not found')
        
    def __createWaiter(self, _IDWaiter):
        f = open('{}.wait'.format(_IDWaiter),'w')
        f.close()
        
    def __deleteWaiter(self, _IDWaiter):
        try:
            os.remove('{}.wait'.format(_IDWaiter))
        except:
            print('Waiter not found')
        
    def __checkWaiter(self, _IDWaiter, _afterSleep):
        try:
            while not os.path.isfile('{}.wait'.format(_IDWaiter)):
                True #no-op
                time.sleep(_afterSleep)
        except:
            True #no-op
        return True