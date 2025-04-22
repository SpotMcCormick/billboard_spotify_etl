import billboard
import pandas as pd
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import os

# Load environment variables
load_dotenv()

# Set up authentication securely
client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")



if not client_id or not client_secret:
    raise ValueError("Spotify credentials not found. Check your .env file!")

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

# Define save path
save_path = r"D:\spotify_etl\data"

# Function to fetch Billboard Hot 100 data and merge with Spotify metadata
def get_hot_100():
    chart = billboard.ChartData('hot-100')
    # updated_date = datetime.now().strftime("%Y-%m-%d")

    data = []
    for song in chart:
        track_name = song.title
        artist_name = song.artist
        query = f"{track_name} {artist_name}"

        # Search Spotify for the track
        results = sp.search(q=query, type="track", limit=1)
        if results['tracks']['items']:
            track = results['tracks']['items'][0]

            data.append({
                # "Updated Date": updated_date,
                "Date": chart.date,
                "Rank": song.rank,
                "Title": track['name'],
                "Artist": ', '.join([artist['name'] for artist in track['artists']]),
                "Album": track['album']['name'],
                "Release Date": track['album']['release_date'],
                "Popularity": track['popularity'],
                "Spotify ID": track['id']
            })

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Ensure directory exists before saving
    os.makedirs(save_path, exist_ok=True)
    file_path = os.path.join(save_path, "master_df.csv")
    df.to_csv(file_path, index=False)

    print(f"Master dataset saved to {file_path} ✅")

# Run the function
get_hot_100()




