import json


def write_to_json(data):
    # Write method for streamlined JSON storage (like error messages)
    print(data)
    #for track in raw_data["items"]:
    #    print(track["track"]["name"])
    with open("alternative_data.json", "w") as outfile:
        outfile.write(json.dumps(data, indent=4))
    pass