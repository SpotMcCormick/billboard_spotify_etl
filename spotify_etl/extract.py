import billboard
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
import os
import logging

# Set up logging to file
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Set up Spotify authentication
client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

if not client_id or not client_secret:
    logging.error("Spotify credentials not found. Check your .env file!")
    raise ValueError("Spotify credentials not found. Check your .env file!")

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

def fetch_billboard_data(chart_name='hot-100'):
    try:
        chart = billboard.ChartData(chart_name)
        logging.info(f"Fetched Billboard chart data for {chart_name}")
        return chart
    except Exception as e:
        logging.error(f"Failed to fetch Billboard chart data: {e}")
        return None

def fetch_spotify_data(track_name, artist_name):
    query = f"{track_name} {artist_name}"
    try:
        results = sp.search(q=query, type="track", limit=1)
        if results['tracks']['items']:
            track = results['tracks']['items'][0]
            logging.info(f"Fetched Spotify data for {track_name} by {artist_name}")
            return track
        else:
            logging.warning(f"No Spotify data found for {track_name} by {artist_name}")
            return None
    except Exception as e:
        logging.error(f"Failed to fetch Spotify data for {track_name} by {artist_name}: {e}")
        return None
