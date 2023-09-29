import pandas as pd
import json

with open('/etl_airflow/data/json/playlists.json') as f:
    playlist_json = json.load(f)

df = pd.json_normalize(playlist_json['playlists'], record_path='items')

df.drop(columns=['collaborative', 'href', 'id', 'public', 'images', 'primary_color', 'snapshot_id', 'type', 'uri', 'owner.external_urls.spotify', 'owner.id', 'tracks.href', 'owner.href', 'owner.uri' ,'owner.type'], inplace=True)

df = df.rename(columns={
    'name': 'playlist_title',
    'description': 'description',
    'tracks.total': 'n_tracks',
    'owner.display_name': 'owner',
    'external_urls.spotify': 'url',
})

df.to_csv('/etl_airflow/data/csv/playlists.csv')