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

    #################################################################################################################
    
    # @dev: Function that allow us to creare a time-series model
    # Input: 1 column dataframe with time serialized data, the dimension of windows that will be used to predict the new data,
    # the percentage age of training (usually set to 70 or to 80), epochs (number of time the model will be defined), plotData is
    # a boolean that allow us to see a graph of data and prediction result
    # Output: tuple of different data: Model (that can be saved outside the function), root mean squared error, correlation
    
    def modelBase(self, _dfFiltered, _windowLenght, _percTraining, _epochs, _plotData):
        #data = df.filter(['Close'])
        data = _dfFiltered
        dataset = _dfFiltered.values
        data = data[data.columns[0]].to_frame()
        
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
        
        train = data[:training_data_len]
        valid = data[training_data_len:]
        valid.insert(1,'Predictions',predictions, True)
            
        if(_plotData):
            plt.figure(figsize=(16,8))
            plt.title('Model')
            plt.xlabel('Spectrum', fontsize=18)
            plt.ylabel('Price', fontsize=18)
            plt.plot(train[data.columns[0]])
            plt.plot(valid[[data.columns[0], 'Predictions']])
            plt.legend(['Train', 'Val', 'Predictions'], loc = 'lower right')
            plt.show()
            
            print()
            #2 plot 0 - 10 days
            
            valid = data[training_data_len:training_data_len+10]
            valid.insert(1,'Predictions',predictions[0:10], True)
            plt.figure(figsize=(16,8))
            plt.title('Model')
            plt.xlabel('Spectrum', fontsize=18)
            plt.ylabel('Price', fontsize=18)
            plt.plot(valid[[data.columns[0], 'Predictions']])
            plt.legend(['Val', 'Predictions'], loc = 'lower right')
            plt.show()
        #Return data
        
        corr = valid[data.columns[0]].corr(valid['Predictions'])
        return model, rmse, corr