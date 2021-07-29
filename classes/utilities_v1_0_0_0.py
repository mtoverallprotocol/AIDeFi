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
            
     #################################################################################################################
    
    # @dev: 
    # Input: array of dataframe columns (or array of elements) and array of filter
    # Output: array of indexes of _keys where _arrayFilter is present
    # Note: for merge logs table, it is useful to catch some specific columns indipendently from address or event
    def filterColumns(self, _keys, _arrayFilter):
        
        indexList = []
        index = 0
        for key in _keys:
            for filt in _arrayFilter:
                if filt in key: indexList.append(index) 
            index = index + 1
        
        return indexList