from . import utilities_v1_0_0_0 as utilities
from . import blockchain_v1_0_0_0 as blockchain

utilities = utilities.utilities()
frameworkInfo = utilities.getFrameworkInfo()

blockchain = blockchain.blockchain()

from past.builtins import execfile
libraryPath = '{0}/libraries_v{1}.py'.format(frameworkInfo['Folders']['classes'], frameworkInfo['Metadata']['version'])
execfile(libraryPath)

class analysis:
    
    def ___init__(self, name):
        self.name = name
        
    def reportDexCustomers(self, _source):

        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        EXPORT_FOLDER = 'export/{0}'.format(_source)
        export_files = [f for f in listdir(EXPORT_FOLDER) if isfile(join(EXPORT_FOLDER, f))]
        print(export_files)
        for indexFile in export_files:
            fileMatch = 'CUSTOMERS_TRANSACTIONS_FROM'
            if(indexFile[0:len(fileMatch)] == fileMatch):
                lastTxDex = indexFile
        
        for indexFile in export_files:
            fileMatch = 'CUSTOMERS_LIST_FROM'
            if(indexFile[0:len(fileMatch)] == fileMatch):
                lastCList = indexFile
                
        print(lastTxDex)
        print(lastCList)
        transactionsFromUsersDex = pd.read_csv('{0}/{1}'.format(EXPORT_FOLDER, lastTxDex))
        print("Opening file {0}/{1}".format(EXPORT_FOLDER, lastTxDex))
        transactionsFromUsersDex.set_index(transactionsFromUsersDex['from'])
        
        transactionsFromUsersDex.rename(columns={'to':'toAddress'}, inplace=True)
        transactionsFromUsersDex.rename(columns={'from':'fromAddress_'}, inplace=True)
        transactionsFromUsersDex['fromAddress'] = transactionsFromUsersDex['fromAddress'].str.upper()
        transactionsFromUsersDex['fromAddress_'] = transactionsFromUsersDex['fromAddress_'].str.upper()
        transactionsFromUsersDex['toAddress'] = transactionsFromUsersDex['toAddress'].str.upper()

        address = []
        name = []
        
        for index in range(0, blockchain.getNumberOfMaps(_source)):
            dex = blockchain.getMaps(index, _source)
            if(dex[0] != -1):
                address.append(str(dex[1]).upper())
                name.append(str(dex[0]).upper())
                dfDex = pd.DataFrame({'DEX_Address' : address, 'DEX_Name' : name})
                dfDex.to_csv(r'DEX.csv')
            else:
                print('Index {0} not found'.format(index))
        mantissa = 1
        
        # Define mantissa for each source category, if not declared mantissa = 1
        if(_source == 'DEX'):
            mantissa = 10 ** 18 # = 10 ^ 18
         
        transactionsFromUsersDex['value'] = transactionsFromUsersDex['value'].astype(str)
        
        sql = 'Select a.*, CASE WHEN fromAddress = fromAddress_ then "OUT" else "IN" END AS IN_OUT, {0} as mantissa from transactionsFromUsersDex a'.format(mantissa)
        df = psql.sqldf(sql)
        dexList = []
        caseWhenStatement_IN = []
        caseWhenStatement_OUT = []
        sumCaseWhenStatement = []
            
        for dexIndex in range(0, blockchain.getNumberOfMaps(_source)):
            name = str(blockchain.getMaps(dexIndex, _source)[0])
            address = str(blockchain.getMaps(dexIndex, _source)[1])
            df[name + '_IN'] = np.where(df['fromAddress'] == address.upper(), True, False)
            dexList.append(name + '_IN')
            df[name + '_OUT'] = np.where(df['toAddress'] == address.upper(), True, False)
            dexList.append(name + '_OUT')
            caseWhenStatement_IN.append("WHEN {0} = True THEN 'SUM_{0}'".format(name + '_IN'))
            caseWhenStatement_OUT.append("WHEN {0} = True THEN 'SUM_{0}'".format(name + '_OUT'))
            sumCaseWhenStatement.append("SUM(CASE WHEN {0} = True THEN 1 ELSE 0 END) AS CNT_{0}, SUM(CASE WHEN {0} = True THEN value / {1} ELSE 0 END) AS SUM_{0}".format(name + '_OUT', mantissa))
        dexList = ", ".join(dexList)

        caseWhenStatement_IN = " ".join(caseWhenStatement_IN)
        caseWhenStatement_OUT = " ".join(caseWhenStatement_OUT)
        sumCaseWhenStatement = ", ".join(sumCaseWhenStatement)

        sql = 'With toDexTransactions As ' \
              '(Select a.* ' \
              'From df a inner join dfDex b ' \
              'on (a.toAddress = b.DEX_Address or a.fromAddress = b.DEX_Address)) ' \
              'Select fromAddress, count(*) as CNT_{2}_TX, ' \
              'SUM(CASE WHEN IN_OUT = "OUT" THEN value / {1} END) as TOTAL_{2}_OUT, ' \
              'MIN(blockNumber) as MIN_BLOCK_ANALYZED, ' \
              'MAX(blockNumber) as MAX_BLOCK_ANALYZED, ' \
              '(MAX(blockNumber)-MIN(blockNumber)) / count(*) as FREQ_CNT_{2}_TX, ' \
              'MAX(CASE WHEN IN_OUT = "OUT" THEN value / {1} END) as MAX_{2}_OUT, ' \
              '{0} ' \
              'From toDexTransactions ' \
              'Group by fromAddress ' \
              'Order By TOTAL_{2}_OUT desc'.format(sumCaseWhenStatement, mantissa, _source)
        
        result = psql.sqldf(sql)

        q = [.5, .65, .8, .9, .95, .995]
        totalDex_OUT_q = np.quantile(result['TOTAL_{0}_OUT'.format(_source)] , q, axis = 0)
        plot_df = pd.DataFrame(totalDex_OUT_q, columns = ['TOTAL_{0}_OUT_Q'.format(_source)])
        plot_df['QUANTILE'] = pd.Series(q)
        sql = "Select *, " \
              "CASE " \
              "WHEN TOTAL_{6}_OUT >= 0 AND TOTAL_{6}_OUT <= {0} THEN 'MINIMAL' " \
              "WHEN TOTAL_{6}_OUT > {0} AND TOTAL_{6}_OUT <= {1} THEN 'LOW' " \
              "WHEN TOTAL_{6}_OUT > {1} AND TOTAL_{6}_OUT <= {2} THEN 'BRONZE' " \
              "WHEN TOTAL_{6}_OUT > {2} AND TOTAL_{6}_OUT <= {3} THEN 'SILVER' " \
              "WHEN TOTAL_{6}_OUT > {3} AND TOTAL_{6}_OUT <= {4} THEN 'GOLD' " \
              "WHEN TOTAL_{6}_OUT > {4} AND TOTAL_{6}_OUT <= {5} THEN 'PLATINUM' " \
              "WHEN TOTAL_{6}_OUT > {5} THEN 'PRIVILEGE' " \
              "END AS ADDRESS_CATEGORY " \
              "FROM result".format(totalDex_OUT_q[0], totalDex_OUT_q[1], totalDex_OUT_q[2], totalDex_OUT_q[3], totalDex_OUT_q[4], totalDex_OUT_q[5], _source)
        result = psql.sqldf(sql)
        result.to_csv(r"export/CUSTOMER_{0}_ANALYSIS_{1}.csv".format(_source, dt))
        print("File saved at export/CUSTOMER_{0}_ANALYSIS_{1}.csv".format(_source, dt))

    def analyzeDexCustomers(self, _apiKey, _lenght):
        
        from . import blockchain_v1_0_0_0 as blockchain
        from . import utilities_v1_0_0_0 as utilities
        blockchain = blockchain.blockchain()
        utilities = utilities.utilities()
        
        blockchain.importAllDexTransactions(_apiKey, 'ETHERSCAN', 'TXLIST', 'DEX', 'MAX', 0, _lenght) # 24 hours
        blockchain.importTransactionsFromDexCustomers(_apiKey, 'ETHERSCAN', 'TXLIST', 'DEX')
        utilities.mergeFiles('DEX')
        self.reportDexCustomers('DEX')
        
    def analyzeTokenCustomers(self, _apiKey, _lenght):
        
        from . import blockchain_v1_0_0_0 as blockchain
        from . import utilities_v1_0_0_0 as utilities
        blockchain = blockchain.blockchain()
        utilities = utilities.utilities()
        
        blockchain.importAllDexTransactions(_apiKey, 'BSCSCAN', 'TXLIST', 'TOKEN', 'MAX', 0, _lenght)
        blockchain.importTransactionsFromDexCustomers(_apiKey, 'BSCSCAN', 'TXLIST', 'TOKEN')
        utilities.mergeFiles('TOKEN')
        self.reportDexCustomers('TOKEN')