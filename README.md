# PROJECT DESCRIPTION
The project consists in processing JSON files and uploading them into a database related to the Sparkify (an streaming app).
Some JSON files contain data collected from the user activity on the streaming app (directory: '/data/log_data'). Others files contain metadata on the songs in the app (directory: '/data/song_data', source: http://millionsongdataset.com/).
The database design was based on the star schema in order to optimize queries on song play analysis. 

# DATABASE DESIGN
The database contains 4 dimension tables:
- songs: songs in music database.
- artists: artists in music database.
- users: users in the app (from log files).
- time: timestamps of records in songplays broken down into specific units.

And the fact table:
- songplays: records in log data associated with song plays i.e. records with page NextSong.

Temporary table:
- temp_log: a temporary table used to load the data on the tables users and songplays.
For further details, check the ER diagram sparkifydb_erd.png and the file sql_queries.py.

# ETL PROCESS

The tables 'songs' and 'artists' are loaded with the JSON files found in the directory '/data/song_data'.

The tables 'users' and 'time' are loaded with the JSON files found in the directory '/data/log_data'. User level column (users table) is updated in accordance with the log files.

JSON files are coverted to pandas DataFrames and they are handled before updating the database. After that, they are loaded in the PostgreSQL trough 'psycopg2' package. The SQL statements can be found in the file 'sql_queries.py'.

'Songplays' table is loaded with the data from the JSON log files and also with data queried from 'songs' and 'artists' tables.

The log files are imported to a temporary table (temp_log).

For further details, check the file 'etl.py'.

# REPOSITORY FILES
- 'sql_queries.py' contains the commands to create and delete tables, insert and delete data.
- 'create_tables.py' creates the database and tables. It also connects to the database created.
- 'etl.py' contains the steps to process the JSON files and import the data to the respective tables.
- 'etl.ipynb' contains some examples of processed data and import steps.
- 'test.ipynb' connects to the database and query the tables in order to check if the data was properly inserted.
- 'test_etl.ipynb' runs the create_tables.py and etl.py script
- 'db_schema.py' generates the .png of the database schema.

# STEPS
Before running etl.py or etl.ipynb it is necessary to run 'create_tables.py'. It may be necessary to restart the kernel.
To run db_schema.py is required to have sqlalchemy_schemadisplay installed (pip install sqlalchemy_schemadisplay).


