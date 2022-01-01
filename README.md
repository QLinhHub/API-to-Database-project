# API-to-Database-project
This project aims to understand how to fetch data from an API into a database (SQL). The objective is to build a database describing the relationship between the number of football competitions and the number of covid positive cases in the country. So we want to know if the covid-19 pandemic is due to the matches where there are a lot of fans who come to watch football.

Therefore, we need to construct two tables:
- <pre><code>CREATE TABLE Covid19(country_name VARCHAR(50) NOT NULL,
                        population INT,
                        location VARCHAR(50),
                        life_expectancy VARCHAR(10),
                        positives INT NOT NULL,
                        deaths INT NOT NULL,
                        PRIMARY KEY (country_name)).</code></pre>

- <pre><code>CREATE TABLE Competitions(league_id INT NOT NULL,
                        country_name VARCHAR(50) NOT NULL,
                        league_name VARCHAR(50),
                        currentMatchday INT,
                        PRIMARY KEY (league_id, country_name),
                        FOREIGN KEY (country_name) REFERENCES Covid19(country_name)).</code></pre>


# APIs used in this project
- Covid-19: https://covid-api.mmediagroup.fr/v1/cases
- Competitions: http://api.football-data.org/v2/competitions

# Beforehand 
- Python version = 3.10.0.
- Make sure that you have SQL Server installed on your machine. If not, install it: [Windows](https://www.microsoft.com/en-us/sql-server/sql-server-downloads), [Linux](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).

# Quick start
1. Install the required packages: 
    <pre><code>pip install -r requirements.txt.</code></pre>
2. Creat new database, new tables (Covid19, Competitions) and fetch data from API into the database: 
    <pre><code>python api.py --newDatabase True  --server "your server ip adresse"  --user "your user name" --password "your password".</code></pre>

- To see more options: <pre><code>python api.py -h.</code></pre>
    <pre><code>usage: api.py [-h] [--newDatabase NEWDATABASE] [--newTable NEWTABLE] [--db_name DB_NAME] [--server SERVER] [--port PORT] [--user USER] [--password PASSWORD]

    Fetch data from API to SQL Server.

    optional arguments:
    -h, --help            show this help message and exit
    --newDatabase NEWDATABASE   create new database or not?
    --newTable NEWTABLE   create new table or not?
    --db_name DB_NAME     database name.
    --server SERVER       ip adresse of sql server.
    --port PORT           port of sql server.
    --user USER           user name.
    --password PASSWORD   password of the sql server.</code></pre>

# Troubleshooting
1. SQL Server Cannot drop database because it is currently in use:
https://dba.stackexchange.com/questions/2387/sql-server-cannot-drop-database-dbname-because-it-is-currently-in-use-but-n/2391

2. Error 1807 Could not obtain Exclusive lock on database ‘model’. Retry the operation later(During Create database):
http://www.nazmulhuda.info/error-1807-could-not-obtain-exclusive-lock-on-database-model-retry-the-operation-later-during-create-database
 

