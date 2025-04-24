import pandas as pd
import logging
from datetime import datetime
from extract import fetch_billboard_data, fetch_spotify_data

# Set up logging to file
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(asctime)s - %(message)s')


def transform_data():
    '''
    Transforming data into one dataframe
    :return: Merged formated dataframe
    '''
    chart = fetch_billboard_data()
    if not chart:
        logging.error("No Billboard data to transform")
        return None

    data = []
    for song in chart:
        track_name = song.title
        artist_name = song.artist
        track = fetch_spotify_data(track_name, artist_name)
        if track:
            release_date = track['album']['release_date']
            chart_date = chart.date
            try:
                release_date_parsed = pd.to_datetime(release_date, format='mixed')
                chart_date_parsed = pd.to_datetime(chart_date)
                age_of_song = (chart_date_parsed - release_date_parsed).days
            except ValueError as e:
                logging.warning(f"Failed to parse dates for {track_name} by {artist_name}: {e}")
                age_of_song = None

            data.append({
                "CHART_DATE": chart.date,
                "RANK": song.rank,
                "TITLE": track['name'],
                "ARTIST": ', '.join([artist['name'] for artist in track['artists']]),
                "ALBUM": track['album']['name'],
                "RELEASE_DATE": release_date,
                "POPULARITY": track['popularity'],
                "SPOTIFY_ID": track['id'],
                "AGE_OF_SONG": age_of_song
            })

    # Convert to DataFrame and handle dates
    df = pd.DataFrame(data)
    df['CHART_DATE'] = pd.to_datetime(df['CHART_DATE'])
    df['RELEASE_DATE'] = pd.to_datetime(df['RELEASE_DATE'], format='mixed')

    # Handle any NaT values
    df['CHART_DATE'] = df['CHART_DATE'].fillna(pd.Timestamp('1970-01-01'))
    df['RELEASE_DATE'] = df['RELEASE_DATE'].fillna(pd.Timestamp('1970-01-01'))

    logging.info("Data transformation complete ✅")
    return df


# Example Usage
if __name__ == "__main__":
    df = transform_data()
    if df is not None:
        df.to_csv(r'D:\billboard_spotify_etl\data\master.csv', index=False)
