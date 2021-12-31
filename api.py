import requests
import pymssql
import argparse


def createNewDB(db_name='API2SQL', server='127.0.0.1', port='1433', user='sa', password='MSsql@123456'):
    conn = pymssql.connect(server=server,
                            port=port,
                            user=user,
                            password=password)    
    conn.autocommit(True)
    cur = conn.cursor()
    cur.execute('CREATE DATABASE ' + db_name)
    conn.autocommit(False)
    conn.close()

def connectToDatabse(db_name, server, port, user, password):
    conn = pymssql.connect(server=server,
                            port=port,
                            user=user,
                            password=password,
                            database=db_name)
    conn.autocommit(True)
    return conn

def fetchDataFromCovidAPI(cursor):
    responses = requests.get('https://covid-api.mmediagroup.fr/v1/cases')
    print('Covid19 API:', responses)

    data_json=responses.json()
    data_extracted = []
    countries_name = []
    for key in data_json:
        if key != 'Global':
            country_name = key
            population = data_json[key]['All'].get('population')
            location = data_json[key]['All'].get('location')
            life_expectancy = data_json[key]['All'].get('life_expectancy')
            positives = data_json[key]['All'].get('confirmed')
            deaths = data_json[key]['All'].get('deaths')

            data_extracted.append((country_name, population, location, life_expectancy, positives, deaths))
            countries_name.append(country_name)
    # insert data into database
    cursor.executemany("INSERT INTO Covid19 (country_name, population, location, life_expectancy, positives, deaths) VALUES (%s, %d, %s, %s, %d, %d)",
                        data_extracted)

    return countries_name
        
def fetchDataFromCompetitionsAPI(cursor, countries_name):
    responses = requests.get('http://api.football-data.org/v2/competitions')
    print('Competitins API: ', responses)

    data_json = responses.json()
    data_extracted = []
    for competition in data_json['competitions']:
        if competition['area']['name'] in countries_name:
            country_name = competition['area']['name']
            league_id = competition['id'] 
            league_name = competition['name']
            currentMatchday = competition['currentSeason']['currentMatchday'] if competition['currentSeason'] is not None else None

            data_extracted.append((league_id, country_name, league_name, currentMatchday))

    # insert data into database
    cursor.executemany("INSERT INTO Competitions (league_id, country_name, league_name, currentMatchday) VALUES (%d, %s, %s, %d)",
                        data_extracted)

def fetchDataFromAPI(new_database, db_name, server, port, user, password):
    # if create new database
    if new_database:
        createNewDB(db_name, server, port, user, password)
    
    # connect to sql server with a specific database
    conn = connectToDatabse(db_name, server, port, user, password)
    cursor = conn.cursor()

    # create table 'Covid19' in the database db_name
    cursor.execute("""
                    CREATE TABLE Covid19(
                    country_name VARCHAR(50) NOT NULL,
                    population INT,
                    location VARCHAR(50),
                    life_expectancy VARCHAR(10),
                    positives INT NOT NULL,
                    deaths INT NOT NULL,
                    PRIMARY KEY (country_name))
                    """)

    # create table 'Competitions' in the database db_name
    cursor.execute("""
                    CREATE TABLE Competitions(
                    league_id INT NOT NULL,
                    country_name VARCHAR(50) NOT NULL,
                    league_name VARCHAR(50),
                    currentMatchday INT,
                    PRIMARY KEY (league_id, country_name),
                    FOREIGN KEY (country_name) REFERENCES Covid19(country_name))
                    """)

    # insert data into the table 'Covid19'
    countries_name = fetchDataFromCovidAPI(cursor)

    #insert data into the table Competitions
    fetchDataFromCompetitionsAPI(cursor, countries_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch data from API to SQL Server.')
    parser.add_argument('--newDatabase', type=bool, default=False,
                        help='create new database or not?')
    parser.add_argument('--db_name', type=str, default='API2SQL',
                        help='database name.')
    parser.add_argument('--server', type=str,
                        help='ip adresse of sql server.')
    parser.add_argument('--port', type=str, default='1433',
                        help='port of sql server.')
    parser.add_argument('--user', type=str, default='sa',
                        help='user name.')
    parser.add_argument('--password', type=str,
                        help='password of the sql server')
    args = parser.parse_args()

    fetchDataFromAPI(args.newDatabase, args.db_name, args.server, args.port, args.user, args.password)
    # fetchDataFromCovidAPI(cursor=1)