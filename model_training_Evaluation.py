from statsmodels.tsa.statespace.sarimax import SARIMAX
import pandas as pd
import numpy as np
import pickle
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
from math import sqrt

# Load the data from pickle files
with open('tra_Y_tr.pkl', 'rb') as f:
    tra_Y_tr = pickle.load(f)

# Define function to create a DataFrame for each sensor location
def create_features(location_index):
    traffic_flow_series = tra_Y_tr[location_index, :]
    time_index = pd.date_range(start="2017-01-01", periods=len(traffic_flow_series), freq="15min")
    df_traffic_flow = pd.DataFrame({'Traffic Flow': traffic_flow_series}, index=time_index)
    return df_traffic_flow

def train_and_forecast_sarimax(df, location_index, forecast_steps=96, split_ratio=0.8):
    # Split the data into training and testing sets
    train_size = int(len(df) * split_ratio)
    train_data = df.iloc[:train_size]
    test_data = df.iloc[train_size:train_size+forecast_steps]  # Assuming we have enough data for forecasting

    # Define the SARIMAX model
    model = SARIMAX(train_data['Traffic Flow'], order=(50,1,0), seasonal_order=(0,0,0,96))
    model_fit = model.fit(disp=False)

    # Forecast future traffic flows
    forecast_result = model_fit.forecast(steps=forecast_steps)

    # Evaluate forecast
    actuals = test_data['Traffic Flow']
    mae = mean_absolute_error(actuals, forecast_result[:len(actuals)])
    rmse = sqrt(mean_squared_error(actuals, forecast_result[:len(actuals)]))
    print(f"Sensor {location_index + 1} - MAE: {mae:.2f}, RMSE: {rmse:.2f}")

    # Plot actual and forecasted values
    plt.figure(figsize=(14, 7))
    plt.plot(df.index[:train_size+forecast_steps], df['Traffic Flow'][:train_size+forecast_steps], label='Actual')
    future_dates = pd.date_range(start=train_data.index[-1], periods=forecast_steps+1, freq='15min')[1:]
    plt.plot(future_dates, forecast_result, label='Forecasted', linestyle='--')
    plt.title(f'Traffic Flow Prediction - Sensor {location_index + 1}')
    plt.xlabel('Time')
    plt.ylabel('Traffic Flow')
    plt.legend()
    plt.grid(True)
    plt.show()

    return forecast_result, mae, rmse

# Train and forecast using SARIMAX for each sensor
for location_index in range(36):  # Assuming 36 sensors
    print(f"\nTraining SARIMAX model for Sensor {location_index + 1}...")
    df_traffic_flow = create_features(location_index)
    future_traffic_flows, mae, rmse = train_and_forecast_sarimax(df_traffic_flow, location_index)
    print(f"Future Traffic Flows for Sensor {location_index + 1}: {future_traffic_flows.tolist()}")
