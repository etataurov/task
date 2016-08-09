Flixbus Data Engineer/Backend Developer homework
========
All the data processed beforehead, that allows to simplify server code, move all computations to background, possibly on another server.

Both `pax` and `revenue` could be easily appended with values from new data.

# Install

## Install redis

```
brew install redis
```
or
```
apt-get install redis-server
```

## Install requirements

```
pip install -r requirements.txt
```

## Run tests

```
py.test
```

## Import data to redis

Put all *.csv files to the root of project

```
python prepare_data.py
```
## Start server

```
export FLASK_APP=flixbus_server/app.py
flask run
```

Access it at `http://127.0.0.1:5000`

# Issues in data

During data processing I've found 2 issues in data

1. ride 401631 from stop 2 to stop 45 uses route 1151, but this route doesn't have stop 45
2. ride 1601341 from stop 1 to stop 10 uses route 4751, but it has only one segment from stop 1 to stop 10