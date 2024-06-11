# Script that uses feature engineering to generate new features that
# will be used to train the fraud detection model. The generated features
# will then be stored into a new table that can then be accessed and
# used to train the model

import pandas as pd
import numpy as np
import psycopg2
from geopy.distance import geodesic
from sqlalchemy import create_engine

# Database connection
def connect_to_db():
    conn = psycopg2.connect(
        dbname='fraud_detection',
        user='postgres',
        password='Yellow12',
        host='localhost',
        port='5432'
    )
    return conn

# Load data from database
def load_data(conn):
    query = "SELECT * FROM transactions;"
    df = pd.read_sql(query, conn)
    return df

# Transaction-Specific Feature Engineering
def transaction_specific_feature_engineering(df):
    # Ensure trans_time is a string
    df['trans_time'] = df['trans_time'].astype(str)

    # ensure trans_date is a string
    df['trans_date'] = df['trans_date'].astype(str)
    
    # Combine trans_date and trans_time into a single datetime column specific to each transaction
    df['trans_date_time'] = pd.to_datetime(df['trans_date'] + ' ' + df['trans_time'])

    # Transaction Time Features specific to each transaction
    df['trans_hour'] = df['trans_date_time'].dt.hour
    df['trans_day_of_week'] = df['trans_date_time'].dt.dayofweek
    
    # Categorical Encoding specific to each transaction
    df = pd.get_dummies(df, columns=['category'])
    
    return df

# User-Specific Feature Engineering
def user_specific_feature_engineering(df):
    # Demographic Features specific to each user
    df['dob'] = pd.to_datetime(df['dob'])

    # Calculate age
    today = pd.Timestamp('today')
    df['age'] = today.year - df['dob'].dt.year
    df.loc[df['dob'].dt.month > today.month, 'age'] -= 1
    df.loc[(df['dob'].dt.month == today.month) & (df['dob'].dt.day > today.day), 'age'] -= 1
            
    # Categorical Encoding specific to each user
    df = pd.get_dummies(df, columns=['job', 'state', 'city'])
    
    return df

# User-Specific Transaction Features
def user_transaction_features(df):
    # Initialize feature columns
    df['distance_from_home'] = np.nan
    df['distance_from_prev'] = np.nan
    df['amt_mean'] = np.nan
    df['amt_std'] = np.nan
    df['amt_dev'] = np.nan
    df['rolling_amt_sum'] = np.nan
    df['rolling_amt_mean'] = np.nan
    
    # User-Specific Calculations
    for user in df['acct_num'].unique():
        user_df = df[df['acct_num'] == user].copy()
        
        # Calculate distance from home for each transaction
        user_df.loc[:, 'distance_from_home'] = user_df.apply(lambda row: geodesic(
            (row['lat'], row['long']), (row['merch_lat'], row['merch_long'])).miles, axis=1)
        
        # Calculate distance from previous transaction
        user_df.loc[:, 'prev_merch_lat'] = user_df['merch_lat'].shift(1)
        user_df.loc[:, 'prev_merch_long'] = user_df['merch_long'].shift(1)
        user_df.loc[:, 'distance_from_prev'] = user_df.apply(lambda row: 0 if pd.isna(
            row['prev_merch_lat']) else geodesic((row['prev_merch_lat'], row['prev_merch_long']), 
            (row['merch_lat'], row['merch_long'])).miles, axis=1)
        
        # Transaction Amount Features
        user_df.loc[:, 'amt_mean'] = user_df['amt'].mean()
        user_df.loc[:, 'amt_std'] = user_df['amt'].std()
        user_df.loc[:, 'amt_dev'] = (user_df['amt'] - user_df['amt_mean']) / user_df['amt_std']
        
        # Rolling Window Features
        user_df.loc[:, 'rolling_amt_sum'] = user_df['amt'].rolling(window=3).sum()
        user_df.loc[:, 'rolling_amt_mean'] = user_df['amt'].rolling(window=3).mean()
        
        # Update the main dataframe
        df.loc[user_df.index, ['distance_from_home', 'distance_from_prev', 'amt_mean', 'amt_std', 'amt_dev',
                               'rolling_amt_sum', 'rolling_amt_mean']] = \
            user_df[['distance_from_home', 'distance_from_prev', 'amt_mean', 'amt_std', 'amt_dev',
                     'rolling_amt_sum', 'rolling_amt_mean']]
    
    return df

# Save the processed data back to the database
def save_processed_data(df, conn):
    engine = create_engine('postgresql+psycopg2://postgres:Yellow12@localhost:5432/fraud_detection')
    df.to_sql('processed_fraud_data', engine, if_exists='replace', index=False)

# Main script
def main():
    conn = connect_to_db()
    df = load_data(conn)
    df = transaction_specific_feature_engineering(df)
    df = user_specific_feature_engineering(df)
    df = user_transaction_features(df)
    save_processed_data(df, conn)
    conn.close()

if __name__ == "__main__":
    main()

