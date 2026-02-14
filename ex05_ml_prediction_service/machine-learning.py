# -*- coding: utf-8 -*-
"""
Machine Learning module for training and evaluating a Random Forest Regressor model.

This module provides functionality to:
- Train a Random Forest Regressor model
- Evaluate the model using RMSE metric
- Save the trained model if performance meets threshold
- Handle data loading and preprocessing through external module

Typical usage example:
    python machine_learning.py
"""

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import joblib
import numpy as np
import argparse
import sys
from pathlib import Path
from data_preprocessing import get_x_y

SEED = 42
"""int: Random seed for reproducibility across all random operations."""

SAVE_PATH = "models/random_forest_model.joblib"
"""str: Path where the trained model will be saved."""

def train_model(X_train, y_train):
    """
    Train a Random Forest Regressor model with specified hyperparameters.

    Args:
        X_train (array-like): Training features
        y_train (array-like): Training target values

    Returns:
        RandomForestRegressor: Trained model instance
    """
    model = RandomForestRegressor(
        n_estimators=100,       # Number of trees in the forest
        max_depth=15,           # Maximum depth of each tree
        random_state=SEED,      # Random seed for reproducibility
        n_jobs=-1               # Use all available CPU cores
    )

    print("Training Phase of RandomForestRegressor")
    model.fit(X_train, y_train)
    return model

def test_model(model: RandomForestRegressor, X_test, y_test):
    """
    Evaluate the trained model on test data using RMSE metric.

    Args:
        model (RandomForestRegressor): Trained model to evaluate
        X_test (array-like): Test features
        y_test (array-like): True target values for test set

    Returns:
        float: Root Mean Squared Error (RMSE) of the model's predictions
    """
    print("Testing Phase of RandomForestRegressor")
    preds = model.predict(X_test)
    # RMSE: Root Mean Square Error = (MSE)**1/2
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    return rmse

def save_model(model: RandomForestRegressor):
    """
    Save the trained model to disk using joblib.

    Args:
        model (RandomForestRegressor): Trained model to save

    Note:
        Creates parent directories if they don't exist
    """
    Path(SAVE_PATH).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, SAVE_PATH)

def main():
    """
    Main execution function that orchestrates the model training pipeline.

    Steps:
        1. Load and split data
        2. Train model
        3. Evaluate model
        4. Save model if performance meets threshold (RMSE < 10)
    """
    print("Loading Clean Data")
    X, y = get_x_y()
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,          # 20% of data for testing
        random_state=SEED       # For reproducibility
    )

    model = train_model(X_train, y_train)
    rmse = test_model(model, X_test, y_test)
    print(f"RMSE: {rmse}")

    if rmse < 10:
        save_model(model)
    else:
        print("Current ML model not saved for underperforming, RMSE should be under 10!")

if __name__ == "__main__":
    main()
