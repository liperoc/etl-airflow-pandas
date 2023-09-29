import requests
import json
import base64

client_id = 'spotify-client-id'
client_secret =  'client-secret'

client_creds = f'{client_id}:{client_secret}'
client_creds_b64 = base64.b64encode(client_creds.encode())

headers = {
    'Authorization': f'Basic {client_creds_b64.decode()}'

}
payload = {
    'grant_type': 'client_credentials'
}

request = requests.post('https://accounts.spotify.com/api/token', headers=headers, data=payload)

token = request.json()
acess_token = token['access_token']
    
response = requests.get('https://api.spotify.com/v1/browse/featured-playlists', headers={'Authorization': f'Bearer {acess_token}'})

with open('/etl_airflow/data/json/playlists.json', 'w') as outfile:
    json.dump(response.json(), outfile, ensure_ascii=False)

