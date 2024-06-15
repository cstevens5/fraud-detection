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
import matplotlib.pyplot as plt
import seaborn as sns
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline

# connect to the database
engine = create_engine('postgresql+psycopg2://postgres:Yellow12@localhost:5432/fraud_detection')
# read the data from the processed_fraud_data table into a dataframe
df = pd.read_sql('SELECT * FROM transactions;', engine)

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
grouped = df.groupby('acct_num', as_index=False)

# iterate through each account and split it's transactions
for acct_num, group in grouped:
    train, test = train_test_split(group, test_size=0.3, random_state=42, stratify=group['is_fraud'])
    train_list.append(train)
    test_list.append(test)

# combine all training sets and testing sets
train_df = pd.concat(train_list)
test_df = pd.concat(test_list)

# Exclude irrelevant columns
#irrelevant_columns = [
#    'ssn', 'cc_num', 'first', 'last', 'gender', 'street', 'zip', 'unix_time', 
#    'merchant', 'profile', 'trans_num', 'trans_date', 'trans_time', 'acct_num'
#]
x_train = train_df.drop(columns=['is_fraud'])
y_train = train_df['is_fraud']
x_test = test_df.drop(columns=['is_fraud'])
y_test = test_df['is_fraud']

# Identify categorical and numerical columns
categorical_cols = x_train.select_dtypes(include=['object', 'bool']).columns
numerical_cols = x_train.select_dtypes(include=['number']).columns

# Define the preprocessing for both numerical and categorical columns
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numerical_cols),
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols)
    ])

# weights for the weighted logistic regression - calculated using a formula for the weights
#weights = {0: 0.502, 1: 137}

# apply RandomUnderSampler to handle the class imbalance
#x_train_res, y_train_res = RandomUnderSampler(random_state=42).fit_resample(x_train, y_train)

# Create a pipeline that first transforms the data and then applies the logistic regression model
model_pipeline = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('smote', SMOTE(random_state=42)),
    ('classifier', LogisticRegression(max_iter=10000, class_weight='balanced'))
])

# Train the logistic regression model
model_pipeline.fit(x_train, y_train)

# Make predictions on the test set
y_pred = model_pipeline.predict(x_test)

# Compare predictions with actual values
#results = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
#print(results.head())

# Evaluate the model
accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)

print(f'Accuracy: {accuracy}')
print(f'Precision: {precision}')
print(f'Recall: {recall}')
print(f'F1-Score: {f1}')

# Confusion Matrix
conf_matrix = confusion_matrix(y_test, y_pred)
sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues')
plt.xlabel('Predicted')
plt.ylabel('Actual')
plt.title('Confusion Matrix')
plt.show()

# Classification Report
report = classification_report(y_test, y_pred)
print("Classification Report:")
print(report)

