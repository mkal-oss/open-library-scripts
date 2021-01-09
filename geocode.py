import json
import os
import requests
from geopy.geocoders import Nominatim
from tqdm import tqdm


if __name__ == '__main__':
    geolocator = Nominatim(user_agent="openlibrary-locations")
    unmatches = json.load(open('location-unmatches.json', 'r'))
    pbar = tqdm(total=len(unmatches))
    results = json.load(open('location-geocoding.json', 'r'))
    save_every = 100
    errors = open('location-geocoding-errors.log', 'a', encoding='utf-8')
    for unmatch in unmatches:
        if unmatch in results:
            pbar.update(1)
            continue
        try:
            results[unmatch] = None
            location = geolocator.geocode(unmatch.strip())
        except:
            errors.write(unmatch + '\n')

        if location is not None:
            results[unmatch] = {
                'address': location.address,
                'lat': location.latitude,
                'long': location.longitude
            }
        pbar.update(1)
        if (pbar.n % save_every) == 0:
            with open('location-geocoding.json', 'w') as fp:
                json.dump(results, fp)
    pbar.close()