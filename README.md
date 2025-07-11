# St. Louis Restaurants

Gets data on STL restaurants.


## Getting initial restaurant dataset

```
uv run scrapy crawl restaurants -o restaurants.jsonl
```

## Geocoding

```
uv run geocode.py restaurants.jsonl -o geocoded_restaurants.jsonl
```

## Converting to CSV

```
uv run convert.py geocoded_restaurants.jsonl geocoded.csv
```
