
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
    
    def getFrameworkInfo(self):
        
        import pathlib
        from os import path, listdir
        import os
        import configparser
        import json
        
        # Reading Configs
        config = configparser.ConfigParser()
        path = str(pathlib.Path(__file__).parent.absolute())
        pathArray = path.split('/')
        pathArray = pathArray[0:len(pathArray)-1]
        path = "/".join(pathArray)
        config.read("{0}/config.ini".format(path))
        
        return config

