#!/usr/bin/env python3 

import argparse
import itertools
import json
import time
from collections import namedtuple
from pathlib import Path
from subprocess import run

import requests

GeocodeResult = namedtuple("GeocodeResult", ["lat", "lon", "geocode_score", "result_name", "geocoder"])

API_USER_EMAIL = run(["git", "config", "--get", "user.email"], capture_output=True, text=True).stdout.strip()
RESERVED_FIELDS = {"lat", "lon", "result_name", "geocode_score", "geocoder"}

def possibly_strip_city(address):
    """The STL City geocoder assumes that all of its inputs are in St. Louis City, so try to detect and strip that out if it's present."""
    address_only, city = address.split(",", 1)
    match city.strip().lower():
        case "st. louis" | "st louis" | "saint louis":
            return address_only
        case _:
            return address

def format_esri_batch(addresses):
    """Format addresses for the STL City ESRI geocoder."""
    records = []
    for idx, address in enumerate(addresses):
        records.append({
            "attributes": {
                "OBJECTID": idx + 1,
                "SingleLine": possibly_strip_city(address),
            }
        })
    return json.dumps({"records": records})

def geocode_stl_batch(addresses): 
    """Geocode a batch of addresses using the STL City ESRI geocoder."""
    base_url = "https://maps6.stlouis-mo.gov/arcgis/rest/services/GEOCODERS/COMPOSITE_GEOCODE/GeocodeServer/geocodeAddresses"
    params = {
        "addresses": format_esri_batch(addresses),
        "category": "",
        "sourceCountry": "",
        "matchOutOfRange": "true",
        "langCode": "",
        "locationType": "",
        "searchExtent": "",
        "outSR": "4326",
        "f": "json",
    }
    response = requests.post(base_url, data=params)
    data = json.loads(response.text)
    # Must sort by ResultID to match up the results with the input addresses.
    for location in sorted(data["locations"], key=lambda x: x["attributes"]["ResultID"]):
        yield GeocodeResult(
            lat=location["location"]["y"],
            lon=location["location"]["x"],
            geocode_score=location["score"],
            result_name=location["address"],
            geocoder="stl_esri",
        )

def geocode_stl(addresses):
    """Geocodes a bunch of addresses using the STL City ESRI geocoder.
    
    Abstracts over the batching logic so downstream code doesn't have to worry about it.
    """
    esri_batch_size = 100
    for esri_batch in itertools.batched(addresses, esri_batch_size):
        esri_results = geocode_stl_batch(esri_batch)
        yield from esri_results


def geocode_nominatim_single(address):
    """Geocode a single address using the OSM Nominatim geocoder."""
    base_url = f"https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,
        "format": "jsonv2",
        "countrycodes": "us",
        "layer": "address",
        "limit": 1,
        "email": API_USER_EMAIL,
    }
    response = requests.get(base_url, params=params)
    data = json.loads(response.text)
    if not data:
        return None
    return GeocodeResult(
        lat=data[0]["lat"],
        lon=data[0]["lon"],
        geocode_score=data[0]["importance"],
        result_name=data[0]["display_name"],
        geocoder="openstreetmap",
    )

def geocode_nominatim_batch(addresses):
    """Geocode a batch of addresses using the OSM Nominatim geocoder."""
    for address in addresses:
        result = geocode_nominatim_single(address)
        yield result
        # https://operations.osmfoundation.org/policies/nominatim/
        # No heavy uses (an absolute maximum of 1 request per second).
        time.sleep(1)

def geocode_nominatim(addresses):
    """Geocodes a bunch of addresses using the OSM Nominatim geocoder.
    
    We don't need this abstraction, but it's here just to have a consistent naming scheme.
    """
    yield from geocode_nominatim_batch(addresses)

def geocode(addresses):
    """Geocode a bunch of addresses.
    
    First attempt to geocode with the STL City ESRI geocoder. If it fails (possibly due to a
    bad address, or a non-St. Louis address), then fall back to the OSM Nominatim geocoder.

    The STL City geocoder is preferred because we can send batch requests, and it works for
    St. Louis City addresses. The OSM Nominatim geocoder is a fallback because it's free and
    works for all addresses, but it's slower and has a rate limit.
    """
    for address, result_stl in zip(addresses, geocode_stl(addresses)):
        if not result_stl.result_name:
            result_osm = next(geocode_nominatim([address]))
            # This returns a GeocodeResult or None.
            yield result_osm
        else:
            yield result_stl

def main(jsonl: Path, output: Path):
    output_data = []
    addresses_to_geocode = []

    # First pass: read the input file and collect the addresses to geocode.
    with jsonl.open() as f:
        for line in f:
            data = json.loads(line)
            if RESERVED_FIELDS.intersection(data.keys()):
                raise ValueError(f"Input data contains reserved fields: {RESERVED_FIELDS.intersection(data.keys())}")
            addresses_to_geocode.append(data["location"])
            output_data.append(data)

    # Second pass: geocode the addresses and match them back up to the original data.
    for idx, result in enumerate(geocode(addresses_to_geocode)):
        if result is None:
            continue
        output_data[idx]["lat"] = result.lat
        output_data[idx]["lon"] = result.lon
        output_data[idx]["result_name"] = result.result_name
        output_data[idx]["geocode_score"] = result.geocode_score
        output_data[idx]["geocoder"] = result.geocoder

    with output.open("w") as f:
        for data in output_data:
            f.write(json.dumps(data) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl", help="JSONL file to geocode. It should have a 'location' field; the other fields will be ignored and passed through to the output. Errors on reserved fields: " + ", ".join(RESERVED_FIELDS), type=Path)
    parser.add_argument("-o", "--output", help="Output jsonl file to write geocoded data to", type=Path, required=True)
    args = parser.parse_args()
    main(args.jsonl, args.output)
