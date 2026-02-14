#app.py
import streamlit as st
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib

import os
import sys

LOAD_PATH = "models/random_forest_model.joblib"

if not os.path.exists(LOAD_PATH):
    print(f"{LOAD_PATH} does not exist! Verify if your trained a usable model with the command: uv run machine_learning.py")
    sys.exit(1)

model:RandomForestRegressor = joblib.load(LOAD_PATH)

# Streamlit app title
st.title("NYC Yellow Taxi Price - Prediction App")

# Input fields for features
st.header("Enter Feature Values")
pu_location = st.number_input("PickUp Location", min_value=1, max_value=300, value=1)
do_location = st.number_input("DropOff Location", min_value=1, max_value=300, value=1)
passenger_count = st.number_input("Passenger Count", min_value=1, max_value=4, value=1)
fare_amount = st.number_input("Fare amount", min_value=-50.0, max_value=50.0, value=10.0)
extra = st.number_input("Extra", min_value=0.0, max_value=15.0, value=0.0)
tip_amount = st.number_input("Tip Amount", min_value=0.0, max_value=15.0, value=0.0)
trip_distance = st.number_input("Trip Distance", min_value=0.0, max_value=500.0, value=5.0)

# Button to trigger prediction
if st.button("Predict a price"):
    # Prepare input data as a DataFrame (adjust column names as needed)
    input_data = pd.DataFrame({
        'PULocationID': [pu_location],
        'DOLocationID': [do_location],
        'passenger_count': [passenger_count],
        'fare_amount': [fare_amount],
        'extra': [extra],
        'tip_amount': [tip_amount],
        'trip_distance': [trip_distance],
    })
    # Make prediction
    prediction = model.predict(input_data)
    st.subheader("Prediction Result")
    st.write(f"The predicted price is: {prediction[0]}$")