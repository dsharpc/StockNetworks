# StockNetworks
Exploring the use of graph networks through Neo4j for visualising relationships between stocks in the London Stock Exchange.

## Structure

The project is built using Docker Containers through a Docker Compose.
1. **App Container**  

Includes the functions and classes to:
- Read the data from the CloudIEX API
- Write it to the Postgres database
- Format it to obtain percentage change 
- Calculate correlations
- Push back into the Neo4j database

2. **Postgres Database**

3. **Neo4j Database**

## Execution

**Requirements**  
Make sure there exists a file called vars.env with the correct credential information.

To start up the containers run:

```
docker-compose up -d
```

Then go into the 'app' container by using:

```
docker-compose exec app bash
```

Inside this container you can now execute the different commands using the CLI:

```
root@d0df06670440:/app# python3 main.py -h
usage: main.py [-h] {gs,p,cor,neo} ...

Stock exploration using Graph Networks

positional arguments:
  {gs,p,cor,neo}  Action type to perform
    gs            'gs' for getting Symbols from the API
    p             'p' for fetching prices from the API
    cor           + 'cor' for calculating the pairwise correlations between
                  the stocks, this command has 3 arguments: + '-sd' : start
                  date for price data (YYYY-MM-DD) + '-ed' : end data for
                  price data + '-ns' : number of stocks to correlate (these
                  are ordered by transaction volume over the previous period)
    neo           Add stock data to Neo4j database

optional arguments:
  -h, --help      show this help message and exit
```

The arguments for each of the functions can be found by adding the -h flag to the command. For example, to see arguments for the 'cor' function, use:

```
python3 main.py cor -h
```
 Which returns:

 ```
 root@d0df06670440:/app# python3 main.py cor -h
usage: main.py cor [-h] [-sd START_DATE] [-ed END_DATE] [-ns NUM_STOCKS]

optional arguments:
  -h, --help            show this help message and exit
  -sd START_DATE, --start-date START_DATE
                        Format is YYYY-MM-DD
  -ed END_DATE, --end-date END_DATE
                        Format is YYYY-MM-DD
  -ns NUM_STOCKS, --num-stocks NUM_STOCKS
```

To explore the data in the Postgres database use a Database IDE to connect to the Postgres database, with the following parameters:

- host: localhost
- port: 5432  
User, password and database should match those defined in the Docker-Compose file.

For access to the Neo4j database, use the built in GUI. It can be accessed from ```localhost:7474```