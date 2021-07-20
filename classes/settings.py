class settings:
    
    def ___init__(self, name):
        self.name = name
        
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
    
    def loader(self, _className):
        fileName = "classes/loaders/{0}Loader.py".format(_className)
        file = open(fileName, 'w')
        config = self.getFrameworkInfo()
        configVersion = config['Metadata']['version']
        file.writelines("from . import {0}_v{1} as {0}\n\n".format(_className, configVersion))
        file.writelines("{0} = {0}.{0}()".format(_className, configVersion))
        file.close()
        
        return fileName
    
        
