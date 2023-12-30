from datetime import date, timedelta
import os
import urllib.request, urllib.error
import sys
import tarfile
import dask.dataframe as dd
import pandas as pd


def download_all():
    """Download the open vector flight data from OpenSky Network for all Mondays between 2020-05-25 and 2022-06-27"""

    # Ensure that the data directory exists
    os.makedirs("data", exist_ok=True)

    # Publicly accessible archives are available for two years worth of Mondays from 2020-2022
    start_date = date(2020, 5, 25)
    end_date = date(2022, 6, 27)
    curr_date = start_date

    # For all the dates between the start and end date, and for all 24 hours of the day, download the tar csv file
    # containing the vector flight data for that date and hour
    while curr_date <= end_date:
        for hour in range(24):
            # format the url below to download the data for the given date and hour, making sure to zero-pad the hour the hour
            csv_name = f"states_{curr_date}-{hour:02}.csv"
            url = f"https://opensky-network.org/datasets/states/{curr_date}/{hour:02}/{csv_name}.tar"
            dest_path = f"data/{csv_name}.tar"
            unpacked_path = f"data/{csv_name}.gz"

            # if the source file already exists, skip downloading it
            # if the unpacked file already exists, skip downloading it
            if os.path.exists(dest_path) or os.path.exists(unpacked_path):
                print(f"Skipping: {url}")
                continue

            print(f"Download: {url}")

            # given the url, download the data to a directory named "data" in the current working directory
            # if the request returns a 404 (missing file) skip it without crashing
            try:
                urllib.request.urlretrieve(url, dest_path)
                unpack(dest_path)
            except urllib.error.HTTPError as error:
                if error.code == 404:
                    print(f"NOTFOUND:  {url}")
                    continue
                else:
                    raise error


        curr_date += timedelta(days=7)

# Given the .tar file located at `file`, unpack only the .csv.gz file inside it
def unpack(file):
    # Unpack the tar file, extracting only the .csv.gz file inside it by replacing the .tar extension with .gz
    try:
        tar = tarfile.open(file)
        tar.extract(f"{os.path.basename(file)[:-4]}.gz", "data")
        tar.close()
    except tarfile.ReadError as error:
        print(f"TARTRASH: {file}")

    # Remove the original tar file
    os.remove(file)

def repack_all():
    """Repack all the .csv.gz files in the data directory into Parquet files using Dask to_parquet"""
    df = dd.read_csv(
        'data/states_*.csv.gz',
        compression='gzip',
        blocksize=None,
        assume_missing=True
    )

    os.makedirs("parqdata", exist_ok=True)
    df.to_parquet('parqdata')

def process():
    states = dd.read_parquet('parqdata')
    le_birds = pd.read_csv('le_birds.csv', index_col='callsign')

    # Callsigns may have random padding, so we strip it
    states.callsign = states.callsign.str.strip()

    le_states = states[
        # Filter to only states for the birds we care about
        states.callsign.isin(le_birds.index)

        # Filter out states on the ground
        & ~states.onground

        # Filter out states with no position data
        & ~states.lat.isna()
        & ~states.lon.isna()
        & ~states.geoaltitude.isna()
    ]

    # Compute to a Pandas DF and write to a single parquet file
    le_states.compute().to_parquet('le_states.parquet', index=False, overwrite=True)

if __name__ == '__main__':
    globals()[sys.argv[1]]()