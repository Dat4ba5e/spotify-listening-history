import json
import webbrowser
import requests
import random
import string
import urllib.parse
import base64
import datetime


# yes I know that I could have just used some Oauth2 library
# but I wanted to learn requests anyway 

# first thing to do: paste authorization code in file: authorization.json (will be generated while on first run of the program)
# authorization code: after accepting on the website in the URL after "code="
def request_authorization_code():
    file = open("authentication.json")
    credentials = json.load(file)

    # Recommended for protection against attacks such as cross-site request forgery. See RFC-6749.
    # used for "state" field
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
    print(random_string)
    url = "https://accounts.spotify.com/authorize?"

    params = {
    "response_type": "code",
    "client_id": credentials["spotify"]["client_id"],
    "scope": credentials["spotify"]["scope"],
    "redirect_uri": credentials["spotify"]["redirect_uri"],
    "state": random_string
    }

    webbrowser.open("https://accounts.spotify.com/authorize?" + urllib.parse.urlencode(params))
    
    # implement function to automatically filter out the "state"

    r = requests.get(url, params=params)
    print(r)
    print(type(r))

    template = {"authorization_key": ""}
    with open("authorization.json", "w") as outfile:
        outfile.write(json.dumps(template, indent=4))


def get_client_id_secret_b64():
    pass


def request_token():
    authentication_file = open("authentication.json")
    authorization_file = open("authorization.json")
    authentication_data = json.load(authentication_file)
    authorization_data = json.load(authorization_file)

    client_id = authentication_data["spotify"]["client_id"]
    client_secret = authentication_data["spotify"]["client_secret"]
    mix = f"{client_id}:{client_secret}"
    print(mix)
    mix_bytes = mix.encode("ascii")
    mix_b64 = base64.urlsafe_b64encode(mix_bytes).decode("ascii")[:-1]
    print(mix_b64)

    headers = {
        "Authorization": f"Basic {mix_b64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    print("................")
    print(authorization_data["authorization_key"])
    print("................")

    param = {
        "grant_type": "authorization_code",
        "code": authorization_data["authorization_key"],
        "redirect_uri": authentication_data["spotify"]["redirect_uri"]
    }

    url = "https://accounts.spotify.com/api/token?"
    r = requests.post(url=url, headers=headers, params=param)
    response = r.json()

    print("LEDERHOSN")
    print(response)
    response.update({"Renewal_at": datetime.datetime.now().timestamp() + response["expires_in"]-10} )
    with open("access_token.json", "w") as outfile:
        outfile.write(json.dumps(response, indent=4))
    #return response["access_token"]


def refresh_token():
    file = open("access_token.json")
    data = json.load(file)
    file_auth = open("authentication.json")
    data2 = json.load(file_auth)

    client_id = data2["spotify"]["client_id"]
    client_secret = data2["spotify"]["client_secret"]
    mix = f"{client_id}:{client_secret}"
    print(mix)
    mix_bytes = mix.encode("ascii")
    mix_b64 = base64.urlsafe_b64encode(mix_bytes).decode("ascii")[:-1]
    
    refresh_token = data["refresh_token"]
    #print(refresh_token)
    client_id = data2["spotify"]["client_id"]
    url = "https://accounts.spotify.com/api/token?"
    param = {
        "grant_type": 'refresh_token',
        "refresh_token": refresh_token,
        #"client_id": client_id
    }

    headers = { "Content-Type": "application/x-www-form-urlencoded", "Authorization": f"Basic {mix_b64}" }

    r = requests.post(url=url, params=param, headers=headers)
    response = r.json()
    print(response)

    if "refresh_token" not in response:
        response.update({ "refresh_token": refresh_token })

    response.update({"Renewal_at": datetime.datetime.now().timestamp() + response["expires_in"]-10} )
    with open("access_token.json", "w") as outfile:
        outfile.write(json.dumps(response, indent=4))
    

def test_api():
    TOKEN = request_token()
    headers = {
        #"Accept": "application/json",
        #"Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    #print("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp))
    print("")
    print("Testing API")
    r = requests.get(
        "https://api.spotify.com/v1/artists/4Z8W4fKeB5YxbusRsdQVPb",
        headers=headers)
    response = r.json()
    print(response)