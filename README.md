# Ecolyst

This project consists of a pipeline that consumes and processes data from the Bureau of Labor Statistics and performs a linear regression with stock quotes from the NYSE since the start of 2015.  This is a Java / Spark / MySQL implementation.

It is meant to act as a platform where more economic reports can be added without breaking existing code.  As such, more economic report data can be added over time.  The idea being that the more economic analysis that is contributed, the better the stock price predictions will be.  

The end result is that a linear regression will be performed on every stocks' daily price movement for the following BLS reports:
 - Consumer Price Index 
 - Producer Price Index 
 - Import / Export, and
 - Employment numbers
 
Ultimately it should act as an agent for making daily buy/sell decisions.

The stock market data is downloaded from a Tiingo.com API, where a key is needed.  You will need to buy a key and update the Constants.java file with its value.  A key from Tiingo is $10/month.  Alternatively you can write your own quote class and replace the Tiingo step in the pipeline with your class.

You will also need your own API key from the Bureau of Labor Statistics, which are free, see https://www.bls.gov/developers/

Each pipeline step stores its state in the database (MySQL), making the Java implementation stateless.  Each pipeline step is also idempotent, so it can be run many concurrent times without changing state (i.e. new rows or values will not be added to the database).  This was important for developing and testing.

You'll need to create the database first by running the script EcolystDbModel.sql in MySQL.  Username/password are root/password.

At this point, downloading quotes from Tiingo takes the most amount of time (about 8 hours), followed by the steps to pivot the data and run the regression.  

Future work
 - Data manipulation and regression in Java is slow, and it may be better handled in Python
 
 - Currently only stock market data is retrieved for dates of BLS reports.  All data should be received for every day.
 
 - This code should be deployed to an application server, allowing for scheduling and a UI (next point)
 
 - A scheduling system needs to be put in place for end-of-day stock market data, and for retrieving BLS data.  Currently the time frame is 2015-01-01 to current date, and is run on-demand
 
 - Create a UI 


