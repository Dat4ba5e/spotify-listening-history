import os
import time

from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime
import pandas as pd


from authorization_and_token import refresh_token, request_token, request_authorization_code
from data_handling import request_data


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


def get_client_id_secret_b64():
    pass


def start(state):
    if state == 2:
        time.sleep(600)
    elif state == 1: 
        time.sleep(3600)
    request_data()


if __name__ == "__main__":
    if os.path.exists("authorization.json"):
        start(0)
        #test_api()
    else:
        print("You have to generate an Authorization code first: ")
        print("https://developer.spotify.com/documentation/web-api/tutorials/code-flow")
        request_authorization_code()

    # write token+requested time to json file -> only request new token if expired
