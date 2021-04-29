import math
import requests
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, LSTM
import matplotlib.pyplot as plt

class models:
    def ___init__(self, name, tables):
        self.name = name
    
    # This function return a dataframe with all stock quotes between two dates
    def getStockQuote(self, _web, _ticker, _provider, _start, _end):
        return _web.DataReader(_ticker, data_source = _provider, start = _start, end = _end)
    
    def getNasdaqData(self):
        
        headers = {
            'authority': 'api.nasdaq.com',
            'accept': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
            'origin': 'https://www.nasdaq.com',
            'sec-fetch-site': 'same-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.nasdaq.com/',
            'accept-language': 'en-US,en;q=0.9',
        }

        params = (
            ('tableonly', 'true'),
            ('limit', '25'),
            ('offset', '0'),
            ('download', 'true'),
        )

        r = requests.get('https://api.nasdaq.com/api/screener/stocks', headers=headers, params=params)
        data = r.json()['data']
        return pd.DataFrame(data['rows'], columns=data['headers'])

    def modelBase(self, _dfFiltered, _windowLenght, _percTraining, _epochs, _plotData):
        #data = df.filter(['Close'])
        data = _dfFiltered
        dataset = _dfFiltered.values
        
        
        training_data_len = math.ceil(len(dataset) * _percTraining) #80/20 or 70/30
        
        scaler = MinMaxScaler(feature_range=(0,1))
        scaled_data = scaler.fit_transform(dataset)
        train_data = scaled_data[0:training_data_len, :]
        
        #Creating training dataset

        x_train = []
        y_train = []
        
        windowLenght = _windowLenght
        
        for i in range(windowLenght, len(train_data)):
            x_train.append(train_data[i - windowLenght:i, 0])
            y_train.append(train_data[i, 0])
        
        # Convert and reshape x_train and y_train to numpy

        x_train, y_train = np.array(x_train), np.array(y_train)
        x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))
        
        #Build the LSTM Model (50/50/25 are the number of neurons)

        model = Sequential()
        model.add(LSTM(50, return_sequences = True, input_shape = (x_train.shape[1], 1)))
        model.add(LSTM(50, return_sequences = False))
        model.add(Dense(25))
        model.add(Dense(1))
        
        #Compile the model

        model.compile(optimizer='adam', loss='mean_squared_error', metrics=['mse', 'mae', 'mape'])
        
        #Train the model
        
        model.fit(x_train, y_train, batch_size = 1, epochs = _epochs)
        
        #Creating testing dataset
        
        test_data = scaled_data[training_data_len - windowLenght: , :]
        
        #Create dataset x_test and y_test
        
        x_test = []
        y_test = dataset[training_data_len: , :]
        
        for i in range(windowLenght, len(test_data)):
            x_test.append(test_data[i - windowLenght:i, 0])
            
        x_test = np.array(x_test)
        
        x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))
        
        #Get the models predicted price values
        predictions = model.predict(x_test)
        predictions = scaler.inverse_transform(predictions)
        
        #Get the root mean squared error (RMSE)
        rmse = np.sqrt(np.mean(predictions - y_test) ** 2)
        
        #Plot data
        if(_plotData):
            train = data[:training_data_len]
            valid = data[training_data_len:]
            valid.insert(1,'Predictions',predictions, True)
            plt.figure(figsize=(16,8))
            plt.title('Model')
            plt.xlabel('Date', fontsize=18)
            plt.ylabel('Close Price USD ($)', fontsize=18)
            plt.plot(train['Close'])
            plt.plot(valid[['Close', 'Predictions']])
            plt.legend(['Train', 'Val', 'Predictions'], loc = 'lower right')
            plt.show()
        
        #Return data
        return model, rmse