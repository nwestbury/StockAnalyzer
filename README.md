# StockAnalyzer
A collection of tools to help analyze the stock exchange.

# Setup
Install python3 & numpy/scipy/sklearn
Install postgres

Run `python3 src/sa/models.py` to create the database table/models needed (change `src/sa/database.py` as needed).

# Install Library
```bash
sudo python3 src/setup.py install # install sa library
```

# Scrapers
```bash
cd src/scrapers/
python3 listmanager.py -q # loads the listings table (do this first)
python3 listmanager.py -e # loads the event_history table (dividends/splits)
python3 listmanager.py -h # loads the price_history table

python3 morningstarscraper.py # loads morning star data
```

# Bots
```bash
cd src/bots/

python3 [botname.py] # run a given bot (some bots need manual code changes)
```

# Future Ideas
-Twitter Emotionality as features
