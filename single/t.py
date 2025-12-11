# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "geoip2",
# ]
# ///

import geoip2.database


def main():
    with geoip2.database.Reader("/Users/sandyanz/dev/data/GeoLite2-City.mmdb") as reader:
        response = reader.city('182.3.104.40')
        print(response.country.iso_code)
        print(response.city.name)
        print(response.location.latitude)
        print(response.location.longitude)


if __name__ == "__main__":
    main()
