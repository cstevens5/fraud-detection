import pandas as pd
import psycopg2

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
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        gender CHAR(1),
        street VARCHAR(255),
        city VARCHAR(255),
        state CHAR(2),
        zipcode VARCHAR(255),
        latitude FLOAT(8),
        longitude FLOAT(8),
        city_population INTEGER,
        job VARCHAR(255),
        date_of_birth DATE,
        account_num VARCHAR(255),
        profile VARCHAR(255),
        transaction_num VARCHAR(255),
        transaction_date DATE,
        transaction_time TIME,
        unix_time VARCHAR(255),
        category VARCHAR(255),
        amount FLOAT(8),
        is_fraud BOOLEAN,
        merchant VARCHAR(255),
        merchant_latitude FLOAT(8),
        merchant_longitude FLOAT(8)
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
    'customers.csv',
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

# loop through all of the files and insert the data
for file in files:
    # convert csv file to a data frame
    df = pd.read_csv(f'/training_data/{file}')
    
    # convert the data frame into a list of tuples - each tuple
    # represents a single row of data
    rows = [tuple(row) for row in df.to_numpy()]

    cur.executemany('''
        INSERT INTO fraud_detection (ssn, cc_num, first_name, last_name, gender, street, city, state, zipcode, latitude, longitude, city_population, job, date_of_birth, account_num, profile, transaction_num, transaction_date, transaction_time, unix_time, category, is_fraud, merchant, merchant_latitude, merchant_longitude)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    ''')


# commit changes
conn.commit()

# close cursor and connection
cur.close()
conn.close()

