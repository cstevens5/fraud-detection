# fraud_detection

System that analyzes transaction data and determines whether or not specific transactions are fraudulent.

## Overview

The purpose of this project was to implement a machine learning model that
detects whether specific transactions are fraudulent or not. The first step
in this process was to generate mock transaction data. Mock data was
generated using the Sparkov_Data_Generation github repository; instructions
on how to generate the data is located within the orignal repository. Once
the mock data was generated, I created a file called 'insert_data.py'. This
file took the mock transaction data and inserted it into a database that
I created using postgresql. Then I implemented and trained a logistic
regression model in the 'logistic_regression.py' file, which was able to
detect fraudulent transactions.

## Logistic Regression Model

The Logistic Regression Model was implemented using the sklearn module.
The model was trained and then tested on the mock data, and proved to
be fairly efficient. Some basics statistics that measure the model's
effectiveness are listed below:

```
Accuracy: 0.9976346433770015
Precision: 0.8461538461538461
Recall: 0.7096774193548387
F1-Score: 0.7719298245614035
```

Below is the Classification Report for the model:

```
Classification Report:
              precision    recall  f1-score   support

           0       1.00      1.00      1.00      5465
           1       0.85      0.71      0.77        31

    accuracy                           1.00      5496
   macro avg       0.92      0.85      0.89      5496
weighted avg       1.00      1.00      1.00      5496
```

As demonstrated above, the model if effective and accurately detects
whether transactions are fraudulent or not.

## Issues

Initially the model was performing very well in accuracy, but it was not
performing well in recall and precision. This was because the dataset
being used is very imbalanced. An imbalanced dataset is inevitable when
dealing with financial fraud, because fraudulent transactions are naturally
much rarer than normal transactions.
To deal with this issue, Sythetic Minorty Oversampling Techinque (SMOTE)
was used. This is a technique where synthetic data is generated for the
minority class (fraudulent transactions in this case), in order to
make the dataset more balanced. After applying SMOTE, the accuracy of the
model decreased slightly, but the precision, recall, and the f1-score
increased greatly. This makes for a better and more effective model.
