import spotipy
from spotipy.oauth2 import SpotifyOAuth

CLIENT_ID = '8905232f473446d8ac9e3879435d6050'
CLIENT_SECRET = '754c21e4da6c443aba0a098f2e4dac9d'
REDIRECT_URI = 'http://localhost:8080/callback'


def get_spotify_client(scope=""):
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=scope
    ))
