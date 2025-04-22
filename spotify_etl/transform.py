import pandas as pd
import logging
from datetime import datetime
from extract import fetch_billboard_data, fetch_spotify_data

# Set up logging to file
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(asctime)s - %(message)s')

def parse_date(date_str):
    """Parse date string into a datetime object, handling different formats."""
    for fmt in ('%Y-%m-%d', '%Y-%m', '%Y'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Date format for {date_str} not recognized")

def transform_data():
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
                release_date_parsed = parse_date(release_date)
                chart_date_parsed = datetime.strptime(chart_date, '%Y-%m-%d')
                age_of_song = (chart_date_parsed - release_date_parsed).days
            except ValueError as e:
                logging.warning(f"Failed to parse dates for {track_name} by {artist_name}: {e}")
                age_of_song = None

            data.append({
                "Date": chart.date,
                "Rank": song.rank,
                "Title": track['name'],
                "Artist": ', '.join([artist['name'] for artist in track['artists']]),
                "Album": track['album']['name'],
                "Release Date": release_date,
                "Popularity": track['popularity'],
                "Spotify ID": track['id'],
                "Days Between Release and Chart Date": age_of_song
            })

    # Convert to DataFrame
    df = pd.DataFrame(data)
    logging.info("Data transformation complete ✅")
    return df

# Run the transformation
if __name__ == "__main__":
    df = transform_data()
    if df is not None:
        df.to_csv(r'D:\spotify_etl\data\master_df.csv', index=False)
