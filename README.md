# Module 8 challenge
This challenge is to read the data from source files and then perform data cleaning/ transformation and finally load into the database
### How to run challenge.py
To run this file need to pass 3 file names with full path and should have correct database credentials to connect to database. Script required to have installed sqlalchamy and psycopg2 python libraries.
### Overview
This takes input files and extract its data.
Then performs transform process to clean the data to create desirable data frame
After trasforming data into desirable format, connects to database and puts data into table.
### Output
The output of this program should look something like below
Please wait running the script
Connected!
Please wait adding data to table: movies
Number of record appended:[(6051,)]
importing rows 0 to 1000000...Please wait adding data to table: ratings

Number of rows imported [(1000000,)]
Done. 249.41 total seconds elapsed
importing rows 1000000 to 2000000...Please wait adding data to table: ratings

Number of rows imported [(2000000,)]
Done. 687.72 total seconds elapsed
importing rows 2000000 to 3000000...Please wait adding data to table: ratings

Number of rows imported [(3000000,)]
Done. 35415.39 total seconds elapsed
importing rows 3000000 to 4000000...Please wait adding data to table: ratings

Number of rows imported [(4000000,)]
Done. 35527.51 total seconds elapsed
importing rows 4000000 to 5000000...Please wait adding data to table: ratings

Number of rows imported [(5000000,)]
Done. 35624.97 total seconds elapsed
importing rows 5000000 to 6000000...Please wait adding data to table: ratings

Number of rows imported [(6000000,)]
Done. 35725.48 total seconds elapsed
importing rows 6000000 to 7000000...Please wait adding data to table: ratings

Number of rows imported [(7000000,)]
Done. 35836.11 total seconds elapsed
importing rows 7000000 to 8000000...Please wait adding data to table: ratings

Number of rows imported [(8000000,)]
Done. 35945.10 total seconds elapsed
importing rows 8000000 to 9000000...Please wait adding data to table: ratings

Number of rows imported [(9000000,)]
Done. 36056.53 total seconds elapsed
importing rows 9000000 to 10000000...Please wait adding data to table: ratings

Number of rows imported [(10000000,)]
Done. 36173.80 total seconds elapsed
importing rows 10000000 to 11000000...Please wait adding data to table: ratings

Number of rows imported [(11000000,)]
Done. 36291.74 total seconds elapsed
importing rows 11000000 to 12000000...Please wait adding data to table: ratings

Number of rows imported [(12000000,)]
Done. 36410.94 total seconds elapsed
importing rows 12000000 to 13000000...Please wait adding data to table: ratings

Number of rows imported [(13000000,)]
Done. 36531.21 total seconds elapsed
importing rows 13000000 to 14000000...Please wait adding data to table: ratings

Number of rows imported [(14000000,)]
Done. 36653.23 total seconds elapsed
importing rows 14000000 to 15000000...Please wait adding data to table: ratings

Number of rows imported [(15000000,)]
Done. 36776.34 total seconds elapsed
importing rows 15000000 to 16000000...Please wait adding data to table: ratings

Number of rows imported [(16000000,)]
Done. 36899.40 total seconds elapsed
importing rows 16000000 to 17000000...Please wait adding data to table: ratings

Number of rows imported [(17000000,)]
Done. 37024.06 total seconds elapsed
importing rows 17000000 to 18000000...Please wait adding data to table: ratings

Number of rows imported [(18000000,)]
Done. 37148.68 total seconds elapsed
importing rows 18000000 to 19000000...Please wait adding data to table: ratings

Number of rows imported [(19000000,)]
Done. 37277.55 total seconds elapsed
importing rows 19000000 to 20000000...Please wait adding data to table: ratings

Number of rows imported [(20000000,)]
Done. 37403.53 total seconds elapsed
importing rows 20000000 to 21000000...Please wait adding data to table: ratings

Number of rows imported [(21000000,)]
Done. 37531.18 total seconds elapsed
importing rows 21000000 to 22000000...Please wait adding data to table: ratings

Number of rows imported [(22000000,)]
Done. 37662.02 total seconds elapsed
importing rows 22000000 to 23000000...Please wait adding data to table: ratings

Number of rows imported [(23000000,)]
Done. 37790.18 total seconds elapsed
importing rows 23000000 to 24000000...Please wait adding data to table: ratings

Number of rows imported [(24000000,)]
Done. 37926.28 total seconds elapsed
importing rows 24000000 to 25000000...Please wait adding data to table: ratings

Number of rows imported [(25000000,)]
Done. 38055.78 total seconds elapsed
importing rows 25000000 to 26000000...Please wait adding data to table: ratings

Number of rows imported [(26000000,)]
Done. 38188.79 total seconds elapsed
importing rows 26000000 to 26024289...Please wait adding data to table: ratings

Number of rows imported [(26024289,)]
Done. 38194.71 total seconds elapsed
Total time to import ratings data. 38194.71 total seconds elapsed
### Assumptions
* Assuption that we have movies json file and corrosponding movies metadata and ratings csv files. 
* Requirs pandas, sqlalchamy, psycopg2 python libraries.
* Movies in json file should have corrosponding IDs in rating and kaggle data files.
* During data cleaning process make sure each column has proper type.
* While mearging assuming that it has same common column.
* Should have proper database credentails. Password is defined in config.py file.
* While appending or inserting data into table assuming data has same column names and also same type.
* If data file is large then reading files in chunks that will succeed in transfering data.
