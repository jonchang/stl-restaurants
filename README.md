# St. Louis Restaurants

Gets data on STL restaurants. These are scraped from the open data portal run by the City of St. Louis,
[Food Facility Inspections Dataset](https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=71).
Results are geocoded using the [Geocode Service Dataset](https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=72)
or, when this is insufficient, with OpenStreetMap's [Nominatim instance](https://nominatim.openstreetmap.org).

Python and [uv](https://docs.astral.sh/uv/) are required to run these scripts.

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
