#! /bin/env python3
import csv
import gc
import json
import logging
from pprint import pprint

import humanize
import time
import sys
import os
from tqdm import tqdm
import re
import csv
import os
import time

import ast
from pony.orm import Database, Json, Optional, PrimaryKey, Required, Set, StrArray, commit, count, db_session, select
from collections import namedtuple

from postgres_import import define_entities

# from wikipedia
countries = [
    'Afghanistan',
    'Albania',
    'Algeria',
    'Andorra',
    'Angola',
    'Antigua and Barbuda',
    'Argentina',
    'Armenia',
    'Australia',
    'Austria',
    'Azerbaijan',
    'Bahamas',
    'Bahrain',
    'Bangladesh',
    'Barbados',
    'Belarus',
    'Belgium',
    'Belize',
    'Benin',
    'Bhutan',
    'Bolivia',
    'Bosnia and Herzegovina',
    'Botswana',
    'Brazil',
    'Brunei Darussalam',
    'Bulgaria',
    'Burkina Faso',
    'Burundi',
    'Cambodia',
    'Cameroon',
    'Canada',
    'Cape Verde',
    'Central African Republic',
    'Chad',
    'Chile',
    'China',
    'Colombia',
    'Comoros',
    'Congo',
    'Costa Rica',
    'Côte d\'Ivoire',
    'Croatia',
    'Cuba',
    'Cyprus',
    'Czech Republic',
    'Denmark',
    'Djibouti',
    'Dominica',
    'Dominican Republic',
    'Ecuador',
    'Egypt',
    'El Salvador',
    'Equatorial Guinea',
    'Eritrea',
    'Estonia',
    'Ethiopia',
    'Fiji',
    'Finland',
    'France',
    'Gabon',
    'Gambia',
    'Georgia',
    'Germany',
    'Ghana',
    'Greece',
    'Grenada',
    'Guatemala',
    'Guinea',
    'Guinea-Bissau',
    'Guyana',
    'Haiti',
    'Honduras',
    'Hungary',
    'Iceland',
    'India',
    'Indonesia',
    'Iran',
    'Iraq',
    'Ireland',
    'Israel',
    'Italy',
    'Jamaica',
    'Japan',
    'Jordan',
    'Kazakhstan',
    'Kenya',
    'Kiribati',
    'North Korea',
    'South Korea',
    'Kuwait',
    'Kyrgyzstan',
    'Laos',
    'Latvia',
    'Lebanon',
    'Lesotho',
    'Liberia',
    'Libya',
    'Liechtenstein',
    'Lithuania',
    'Luxembourg',
    'North Macedonia',
    'Madagascar',
    'Malawi',
    'Malaysia',
    'Maldives',
    'Mali',
    'Malta',
    'Marshall Islands',
    'Mauritania',
    'Mauritius',
    'Mexico',
    'Micronesia',
    'Moldova',
    'Monaco',
    'Mongolia',
    'Montenegro',
    'Morocco',
    'Mozambique',
    'Myanmar',
    'Namibia',
    'Nauru',
    'Nepal',
    'Netherlands',
    'New Zealand',
    'Nicaragua',
    'Niger',
    'Nigeria',
    'Norway',
    'Oman',
    'Pakistan',
    'Palau',
    'Panama',
    'Papua New Guinea',
    'Paraguay',
    'Peru',
    'Philippines',
    'Poland',
    'Portugal',
    'Qatar',
    'Romania',
    'Russian Federation',
    'Rwanda',
    'Saint Kitts and Nevis',
    'Saint Lucia',
    'Saint Vincent and the Grenadines',
    'Samoa',
    'San Marino',
    'São Tomé and Príncipe',
    'Saudi Arabia',
    'Senegal',
    'Serbia',
    'Seychelles',
    'Sierra Leone',
    'Singapore',
    'Slovakia',
    'Slovenia',
    'Solomon Islands',
    'Somalia',
    'South Africa',
    'South Sudan',
    'Spain',
    'Sri Lanka',
    'Sudan',
    'Suriname',
    'Swaziland',
    'Sweden',
    'Switzerland',
    'Syria',
    'Tajikistan',
    'Tanzania',
    'Thailand',
    'Timor-Leste',
    'Togo',
    'Tonga',
    'Trinidad and Tobago',
    'Tunisia',
    'Turkey',
    'Turkmenistan',
    'Tuvalu',
    'Uganda',
    'Ukraine',
    'United Arab Emirates',
    'United Kingdom',
    'United States',
    'Uruguay',
    'Uzbekistan',
    'Vanuatu',
    'Venezuela',
    'Vietnam',
    'Yemen',
    'Zambia',
    'Zimbabwe'
]
# also from wikipedia
states = [
    'Alabama',
    'Alaska',
    'Arizona',
    'Arkansas',
    'California',
    'Colorado',
    'Connecticut',
    'Delaware',
    'Florida',
    'Georgia',
    'Hawaii',
    'Idaho',
    'Illinois',
    'Indiana',
    'Iowa',
    'Kansas',
    'Kentucky',
    'Louisiana',
    'Maine',
    'Maryland',
    'Massachusetts',
    'Michigan',
    'Minnesota',
    'Mississippi',
    'Missouri',
    'Montana',
    'Nebraska',
    'Nevada',
    'New Hampshire',
    'New Jersey',
    'New Mexico',
    'New York',
    'North Carolina',
    'North Dakota',
    'Ohio',
    'Oklahoma',
    'Oregon',
    'Pennsylvania',
    'Rhode Island',
    'South Carolina',
    'South Dakota',
    'Tennessee',
    'Texas',
    'Utah',
    'Vermont',
    'Virginia',
    'Washington',
    'West Virginia',
    'Wisconsin',
    'Wyoming'
]
countries = [c for c in countries]
states = [s for s in states]

bulk_geocode_results = json.load(open('location-geocoding.json', 'r'))


def do_location_match(loc):
    loc = str(loc)
    matched = None
    for country in countries:
        if country in loc:
            matched = country
            break
    if not matched:
        for state in states:
            if state in loc:
                matched = 'United States'
                break
    if not matched:
        if len(re.findall(r'\d{5}', loc)) > 0:
            matched = 'United States'

    if not matched:
        if ', uk' in loc:
            matched = 'United Kingdom'
    if not matched:
        if ', usa' in loc:
            matched = 'United States'

    if not matched:
        if loc in bulk_geocode_results and bulk_geocode_results[loc] is not None:
            country = bulk_geocode_results[loc]['address'].split(',')[-1]
            if country in countries:
                matched = country

    return matched


def get_row_locations(row):
    locations = set()
    for field in row[2:-2]:
        if field == '':
            continue
        # print(field)
        try:
            values = json.loads(field)
        except:
            try:
                values = [json.loads(s) for s in ast.literal_eval(field)]
            except:
                try:
                    values = [s for s in ast.literal_eval(field)]
                except:
                    # print(field)
                    continue

        if not isinstance(values, list):
            # print(values)
            continue

        for value in values:
            if isinstance(value, list):
                for v in value:
                    locations.add(v)
            else:
                locations.add(value)
    return locations


def build_usa_dataset():
    dataset = {}
    with open('us-edition-genders.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            is_woman = row[-1] == 'female'
            for year in re.findall(r'\d{4}', row[1]):
                year = int(year.strip())
                if year < 1500 or year > 2020:
                    continue
                if year not in dataset:
                    dataset[year] = {
                        'm': 0,
                        'f': 0
                    }
                if is_woman:
                    dataset[year]['f'] = 1 + dataset[year]['f']
                else:
                    dataset[year]['m'] = 1 + dataset[year]['m']

    for year in sorted(list(dataset.keys())):
        print(f"{year}\t{dataset[year]['m']}\t{dataset[year]['f']}")


if __name__ == '__main__':
    # dates = set()
    # with open('edition-genders.csv', 'r', encoding='utf-8') as csvfile:
    #     reader = csv.reader(csvfile)
    #     for row in reader:
    #         dates.add(row[1])
    # with open('dates.json', 'w') as fp:
    #     json.dump(list(dates), fp)


    # years = set()
    # dates = json.load(open('dates.json', 'r'))
    # for d in dates:
    #     for year in re.findall(r'\d{4}', d):
    #         years.add(year)
    # with open('years.json', 'w') as fp:
    #     json.dump(sorted(list(years)), fp)


    # dataset = {}
    # with open('edition-genders.csv', 'r', encoding='utf-8') as csvfile:
    #     reader = csv.reader(csvfile)
    #     for row in reader:
    #         is_woman = row[-1] == 'True'
    #         for year in re.findall(r'\d{4}', row[1]):
    #             year = year.strip()
    #             if year not in dataset:
    #                 dataset[year] = {
    #                     'm': 0,
    #                     'f': 0
    #                 }
    #             if is_woman:
    #                 dataset[year]['f'] = 1 + dataset[year]['f']
    #             else:
    #                 dataset[year]['m'] = 1 + dataset[year]['m']
    #     with open('year-genders.json', 'w') as fp:
    #         json.dump(dataset, fp)

    # dataset = json.load(open('year-genders.json', 'r'))
    # for year in sorted(list(dataset.keys())):
    #     print(f"{year}\t{dataset[year]['m']}\t{dataset[year]['f']}")

    # locations = set()
    # with open('edition-genders.csv', 'r', encoding='utf-8') as csvfile:
    #     reader = csv.reader(csvfile)
    #     for row in reader:
    #         locs = get_row_locations(row)
    #         for loc in locs:
    #             locations.add(loc)
    # # locations = list(locations)
    # print(len(locations))
    # with open('locations-genders.json', 'w') as fp:
    #     json.dump(locations, fp)

    matches = set()
    unmatches = set()
    locations = json.load(open('locations-genders.json', 'r'))
    pbar = tqdm(total=len(locations))
    for loc in locations:
        matched = do_location_match(loc)

        if matched is not None:
            matches.add(loc)
        else:
            unmatches.add(loc)
        pbar.update(1)
    pbar.close()
    # print(matches)
    print(len(matches))
    print(len(unmatches))

    with open('location-matches.json', 'w') as fp:
        json.dump(list(matches), fp)
    with open('location-unmatches.json', 'w') as fp:
        json.dump(list(unmatches), fp)

    # # https://docs.microsoft.com/en-us/rest/api/maps/search/postsearchfuzzybatch
    # unmatches = json.load(open('location-unmatches.json', 'r'))
    # zipcodes = 0
    # for um in unmatches:
    #     if len(re.findall(r'\d{5}', um)) > 0:
    #         zipcodes+=1
    # print(zipcodes)
