# Ecolyst

This project consists of a pipeline that consumes and processes data from the Bureau of Labor Statistics and performs a linear regression with stock quotes from the NYSE since the start of 2015.  This is a Java / Spark / MySQL implementation.

It is meant to act as a platform where more economic reports can be added without breaking existing code.  As such, I will be adding more economic report data over time.  The idea being that the more economic analysis that is contributed, the better the stock price predictions will be.  Altimately it should act as an agent for making daily buy/sell decisions.

The stock market data is downloaded from a Tiingo.com API, where a key is needed.  You will need to buy a key and update the Constants.java file with its value.  A key from Tiingo is $10/month.  Alternatively you can write your own quote class and replace the Tiingo step in the pipeline with your class.

You will also need your own API key from the Bureau of Labor Statistics, which are free, see https://www.bls.gov/developers/

Each pipeline step stores its state in the database (MySQL), making the Java implementation stateless.  Each pipeline step is also idempotent, so it can be run many concurrent times without changing state (i.e. new rows or values will not be added to the database).  This was important for developing and testing.

You'll need to create the database first by running the script dbModel.sql in MySQL.

At this point, downloading quotes from Tiingo takes the most amount of time (about 8 hours), followed by the steps to pivot the data and run the regression.  On that note, these steps may have better performance if executed in another programming language, perhaps Python.

