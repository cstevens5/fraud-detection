import pandas as pd
import psycopg2
import os

# connect to the database
conn = psycopg2.connect(
    dbname="fraud_detection",
    user="postgres",
    password="Yellow12",
    host="localhost"
)

# create a cursor object
cur = conn.cursor()

# create a table for the urban data
cur.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        ssn VARCHAR(255),
        cc_num VARCHAR(255),
        first VARCHAR(255),
        last VARCHAR(255),
        gender CHAR(1),
        street VARCHAR(255),
        city VARCHAR(255),
        state CHAR(2),
        zip VARCHAR(255),
        lat FLOAT(8),
        long FLOAT(8),
        city_pop INTEGER,
        job VARCHAR(255),
        dob DATE,
        acct_num VARCHAR(255),
        profile VARCHAR(255),
        trans_num VARCHAR(255),
        trans_date DATE,
        trans_time TIME,
        unix_time VARCHAR(255),
        category VARCHAR(255),
        amt FLOAT(8),
        is_fraud CHAR(1),
        merchant VARCHAR(255),
        merch_lat FLOAT(8),
        merch_long FLOAT(8)
    );
''')

# --- Now insert the data from each file into the table

# create a list of the files to be processed
files = [
    'adults_2550_female_rural_0-1.csv',
    'adults_2550_female_rural_2-3.csv',
    'adults_2550_female_rural_4-5.csv',
    'adults_2550_female_rural_6-7.csv',
    'adults_2550_female_rural_8-9.csv',
    'adults_2550_female_urban_0-1.csv',
    'adults_2550_female_urban_2-3.csv',
    'adults_2550_female_urban_4-5.csv',
    'adults_2550_female_urban_6-7.csv',
    'adults_2550_female_urban_8-9.csv',
    'adults_2550_male_rural_0-1.csv',
    'adults_2550_male_rural_2-3.csv',
    'adults_2550_male_rural_4-5.csv',
    'adults_2550_male_rural_6-7.csv',
    'adults_2550_male_rural_8-9.csv',
    'adults_2550_male_urban_0-1.csv',
    'adults_2550_male_urban_2-3.csv',
    'adults_2550_male_urban_4-5.csv',
    'adults_2550_male_urban_6-7.csv',
    'adults_2550_male_urban_8-9.csv',
    'adults_50up_female_rural_0-1.csv',
    'adults_50up_female_rural_2-3.csv',
    'adults_50up_female_rural_4-5.csv',
    'adults_50up_female_rural_6-7.csv',
    'adults_50up_female_rural_8-9.csv',
    'adults_50up_female_urban_0-1.csv',
    'adults_50up_female_urban_2-3.csv',
    'adults_50up_female_urban_4-5.csv',
    'adults_50up_female_urban_6-7.csv',
    'adults_50up_female_urban_8-9.csv',
    'adults_50up_male_rural_0-1.csv',
    'adults_50up_male_rural_2-3.csv',
    'adults_50up_male_rural_4-5.csv',
    'adults_50up_male_rural_6-7.csv',
    'adults_50up_male_rural_8-9.csv',
    'adults_50up_male_urban_0-1.csv',
    'adults_50up_male_urban_2-3.csv',
    'adults_50up_male_urban_4-5.csv',
    'adults_50up_male_urban_6-7.csv',
    'adults_50up_male_urban_8-9.csv',
    'young_adults_female_rural_0-1.csv',
    'young_adults_female_rural_2-3.csv',
    'young_adults_female_rural_4-5.csv',
    'young_adults_female_rural_6-7.csv',
    'young_adults_female_rural_8-9.csv',
    'young_adults_female_urban_0-1.csv',
    'young_adults_female_urban_2-3.csv',
    'young_adults_female_urban_4-5.csv',
    'young_adults_female_urban_6-7.csv',
    'young_adults_female_urban_8-9.csv',
    'young_adults_male_rural_0-1.csv',
    'young_adults_male_rural_2-3.csv',
    'young_adults_male_rural_4-5.csv',
    'young_adults_male_rural_6-7.csv',
    'young_adults_male_rural_8-9.csv',
    'young_adults_male_urban_0-1.csv',
    'young_adults_male_urban_2-3.csv',
    'young_adults_male_urban_4-5.csv',
    'young_adults_male_urban_6-7.csv',
    'young_adults_male_urban_8-9.csv'
]

# list of all of the required columns
required_columns = ['ssn', 'cc_num', 'first', 'last', 'gender', 'street', 'city', 'state', 'zip', 'lat', 'long', 'city_pop', 'job', 'dob', 'acct_num', 'profile', 'trans_num', 'trans_date', 'trans_time', 'unix_time', 'category', 'amt', 'is_fraud', 'merchant', 'merch_lat', 'merch_long']

# loop through all of the files and insert the data
for file in files:
    # convert csv file to a data frame
    df = pd.read_csv(os.path.join('training_data', file), sep='|')

    # ensure the data frame includes all of the required columns
    # even if some are empty
    df = df[required_columns]
    
    # convert the data frame into a list of tuples - each tuple
    # represents a single row of data
    rows = [tuple(row) for row in df.to_numpy()]

    if len(rows) > 0:
        print(file)
        cur.executemany('''
            INSERT INTO transactions (ssn, cc_num, first, last, gender, street, city, state, zip, lat, long, city_pop, job, dob, acct_num, profile, trans_num, trans_date, trans_time, unix_time, category, amt, is_fraud, merchant, merch_lat, merch_long)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        ''', rows)


# commit changes
conn.commit()

# close cursor and connection
cur.close()
conn.close()

