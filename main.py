import os
import time

from authorization_and_token import request_authorization_code
from data_handling import request_data


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
