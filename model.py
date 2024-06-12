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

# initialize empty lists to hold training and testing data
train_list = []
test_list = []

# group the data by account number
grouped = df.groupby('acct_num')

# iterate through each account and split it's transactions
for acct_num, group in grouped:
    train, test = train_test_split(group, test_size=0.3, random_state=42, stratify=group['is_fraud'])
    train_list.append(train)
    test_list.append(test)

# combine all training sets and testing sets
train_df = pd.concat(train_list)
test_df = pd.concat(test_list)

# separate features and target variable
x_train = train_df.drop('is_fraud', axis=1)
y_train = train_df['is_fraud']
x_test = test_df.drop('is_fraud', axis=1)
y_test = test_df['is_fraud']

# drop columns here that are not features or not relevant to the model
# right now we will train on all columns and then tune the model based
# on each columns relevance to the model

# train the logistic regression model
model = LogisticRegression(max_iter=10000)
model.fit(x_train, y_train)

