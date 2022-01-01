import requests
import pymssql
import argparse


def createNewDB(db_name, server, port, user, password):
    """[Create a new database in the sql server.]

    Args:
        db_name ([str]): [name of database.]
        server ([str]): [sql server adresse.] 
        port ([str]): [sql server port.] 
        user ([str]): [user name.] 
        password ([str]): [user password.] 
    """    
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
    """[Connect to sql server using pymssql.]

    Args:
        db_name ([str]): [database name.]
        server ([str]): [sql server adresse.]
        port ([str]): [sql server port.]
        user ([str]): [user name.]
        password ([str]): [user password.]

    Returns:
        [pymssql connection object]: [connection object with authorized autocommit option.]
    """    
    conn = pymssql.connect(server=server,
                            port=port,
                            user=user,
                            password=password,
                            database=db_name)
    conn.autocommit(True)

    return conn


def fetchDataFromCovidAPI(cursor):
    """[Fetch data from the covid19 API: https://covid-api.mmediagroup.fr/v1/cases]

    Args:
        cursor ([pymssql cursor object]): [cursor object, that can be used to make queries and fetch results from the database.]

    Returns:
        [list]: [list of country names, that can be used to make sure that primary key and foreign key must match.]
    """    
    
    responses = requests.get('https://covid-api.mmediagroup.fr/v1/cases') # request data from api.
    print('Covid19 API:', responses)

    data_json=responses.json() # convert data to json format.
    data_extracted = [] # data extracted from the api, used to insert into database (table: Covid19).
    country_names = [] # list of country names.

    # extract the data from covid19 API.
    # because the name of some countries in two API (covid19 and football competitions) is not the same, we need to process it to make sure that primary key and foreign key must match.
    for key in data_json:
        if key != 'Global':
            if key == 'United Kingdom':  
                for sub_key in data_json[key]:
                    if sub_key == 'England' or sub_key == 'Scotland' or sub_key == 'Northern Ireland' or sub_key == 'Wales':          
                        country_name = sub_key
                        positives = data_json[key][sub_key].get('confirmed')
                        deaths = data_json[key][sub_key].get('deaths')
                        population = data_json[key][sub_key].get('population')
                        location = data_json[key][sub_key].get('location')
                        life_expectancy = data_json[key][sub_key].get('life_expectancy')
                        country_names.append(country_name)

                        data_extracted.append((country_name, population, location, life_expectancy, positives, deaths))
            else:
                if key == 'US':
                    country_name = 'United States'
                elif key == 'Czechia':
                    country_name = 'Czech Republic'
                else:
                    country_name = key
                positives = data_json[key]['All'].get('confirmed')
                deaths = data_json[key]['All'].get('deaths')
                population = data_json[key]['All'].get('population')
                location = data_json[key]['All'].get('location')
                life_expectancy = data_json[key]['All'].get('life_expectancy')
                country_names.append(country_name)

                data_extracted.append((country_name, population, location, life_expectancy, positives, deaths))

    # insert data into database
    cursor.executemany("INSERT INTO Covid19 (country_name, population, location, life_expectancy, positives, deaths) VALUES (%s, %d, %s, %s, %d, %d)",
                        data_extracted)

    return country_names
        

def fetchDataFromCompetitionsAPI(cursor, country_names):
    """[Fetch data from the football competitions API: http://api.football-data.org/v2/competitions]

    Args:
        cursor ([pymssql cursor object]): [cursor object, that can be used to make queries and fetch results from the database.]
        country_names ([list]): [list of country names from the Covid19 API to constraint the foreign key in the table: Competitions]
    """    
    responses = requests.get('http://api.football-data.org/v2/competitions') # request data from api.
    print('Competitins API: ', responses)

    data_json = responses.json() #  convert data to json format.
    data_extracted = [] # data extracted from the api, used to insert into database (table: Competitions).
    for competition in data_json['competitions']:
        if competition['area']['name'] in country_names:
            country_name = competition['area']['name']
            league_id = competition['id'] 
            league_name = competition['name']
            currentMatchday = competition['currentSeason']['currentMatchday'] if competition['currentSeason'] is not None else None

            data_extracted.append((league_id, country_name, league_name, currentMatchday))

    # insert data into database
    cursor.executemany("INSERT INTO Competitions (league_id, country_name, league_name, currentMatchday) VALUES (%d, %s, %s, %d)",
                        data_extracted)


def fetchDataFromAPI(new_database, db_name, server, port, user, password, create_table=True):
    """[Fetch data from two API.]

    Args:
        new_database ([bool]): [create or do not create a new database.]
        db_name ([str]): [name of database.]
        server ([str]): [sql server adresse.] 
        port ([str]): [sql server port.] 
        user ([str]): [user name.] 
        password ([str]): [user password.] 
        create_table (bool, optional): [create or do not create new tables (Covid19, Competitions)]. Defaults to True.
    """    
    # if create new database
    if new_database:
        createNewDB(db_name, server, port, user, password)
    
    # connect to sql server with a specific database
    conn = connectToDatabse(db_name, server, port, user, password)
    cursor = conn.cursor()
    if create_table:
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
    country_names = fetchDataFromCovidAPI(cursor)

    #insert data into the table Competitions
    fetchDataFromCompetitionsAPI(cursor, country_names)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch data from API to SQL Server.')
    parser.add_argument('--newDatabase', type=bool, default=False,
                        help='create new database or not?')
    parser.add_argument('--newTable', type=bool, default=True,
                        help='create new table or not?')
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

    fetchDataFromAPI(args.newDatabase, args.db_name, args.server, args.port, args.user, args.password, args.newTable)
