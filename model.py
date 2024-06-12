# This file deploys a machine learning model that uses the training data
# to predict whether or not the transactions from the testing data
# are fraudulent or not.

# A logistic regression model is used where 'is_fraud' is the dependent
# variable, and all other relevent variables are independent.

import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, classification_report
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import matplotlib.pyplot as plt
import seaborn as sns

# connect to the database
engine = create_engine('postgresql+psycopg2://postgres:Yellow12@localhost:5432/fraud_detection')
# read the data from the processed_fraud_data table into a dataframe
df = pd.read_sql('SELECT * FROM processed_fraud_data;', engine)


# --- Preprocessing the data ---

# drop the rows with missing target values
df = df.dropna(subset=['is_fraud'])
# fill any remaining missing values
df = df.fillna(0)
# convert the target variable (is_fraud) to a numeric value
df['is_fraud'] = df['is_fraud'].astype(int)

# define target and features
target = 'is_fraud'
x = df.drop(columns=[target])
y = df[target]

# identify numerical and categorical features
numerical_features = X.select_dtypes(include=['int64', 'float64']).columns.tolist()
categorical_features = X.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()


