# Topmerger

Utility script to update database entries from [PLPR-Scraper](https://github.com/datenschule/plpr-scraper) with their
respective agenda topics from [topscraper](https://github.com/datenschule/topscraper).


## Install
```
pip install -r requirements.txt
```

## Run
e.g: 
```
main.py --tops_path=data/tops.json --session_path=data/out.json --classes_path=data/classes.json --db_url=<DB_url>
```

* tops_path = Tops scraped via [Topscraper](https://github.com/Datenschule/topscraper)
* session_path = Tops scraped via [Agendascraper](https://github.com/Datenschule/agendas)
* classes_path = classified tops like from data/classes.json
* db_url = SQLIte or Postgres database with schema from [Pretty Session Protocols](https://github.com/Datenschule/pretty_session_protocols)
