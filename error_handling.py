import os
import json
import datetime

from main import start


def error_handling(error, source):
    if os.path.exists("error_log.json") and os.stat("error_log.json").st_size != 0:
        # check if the error_log.json file exist and if it is not empty
        file = open("error_log.json")
        data = json.load(file)
    else:
        # initialize the Dict that will become the JSON file if it doesn't exist yet
        data = { "spotify": [], "youtube": []}

    counter = 0
    for err in data[source]:
        if err["error"]["status"] == error["error"]["status"]:
            err["error"]["time"].append(int(datetime.datetime.now().timestamp()))
            counter += 1

    #print(counter)
    if counter == 0:
        error["error"].update({"time": [int(datetime.datetime.now().timestamp())]})
        data[source].append(error)

    with open("error_log.json", "w") as outfile:
        outfile.write(json.dumps(data, indent=4))

    if error["error"]["status"] == 401:
        print("Requesting new Token")
        with open("token_log.txt", "a") as outfile:
            outfile.write("Requesting new Token at time {time}".format(time=datetime.datetime.now().timestamp()))
        start(0)
    else:
        print("Encountered an error, trying again in 10 Minutes")
        start(2)