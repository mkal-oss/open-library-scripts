import csv
import re
import string

from tqdm import tqdm

from get_unique_locations import do_location_match, get_row_locations


def build_csv():
    globalwriter = csv.writer(open('global-country-year-genders.csv', 'w', encoding='utf-8', newline=''))
    globalwriter.writerow(['Country', 'Year', 'Gender'])

    with open('edition-genders.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        pbar = tqdm(total=10116788)
        for row in reader:
            is_woman = row[-1] == 'True'
            for year in re.findall(r'\d{4}', row[1]):
                year = year.strip()
                if is_woman:
                    gender = 'female'
                else:
                    gender = 'male'

                locs = get_row_locations(row)
                for loc in locs:
                    result_location = do_location_match(loc)
                    if result_location is not None:
                        globalwriter.writerow([string.capwords(result_location), year, gender])
            pbar.update(1)
        pbar.close()


if __name__ == '__main__':
    build_csv()
