import os, json, datetime, pytz

def check_dir(path):
    return os.path.exists(path) and os.path.isdir(path)


def normalize_raw_data():
    # Keep the RAW data from the requests in it's original form but delete duplicates
    raw_data_directory = "raw_data"
    clean_data_directory = "raw_data_cleaned"
    filenames = []
    if not check_dir(raw_data_directory):
        print("no raw data directory exists... aborting")
        return
    if os.path.exists(raw_data_directory): 
        if os.path.isdir(raw_data_directory):
            for filename in os.listdir(raw_data_directory):
                if ".json" in filename:
                    #filenames.append(os.path.join(raw_data_directory, filename))
                    filenames.append(filename)
    if not check_dir(clean_data_directory):
        os.mkdir(clean_data_directory)
    #print(filenames)

    # STILL NOT GOOOOOD :(

    # so sometime it skips the evaluation which is weird (example of files with same data: 1 0 1 1)

    print("come on") 
    print(filenames[0])
    filenames = filenames[::-1]
    i = 0
    print(filenames[0])
    all_timestamps = []
    while len(filenames) > 1:
        newer_data = json.load(open(os.path.join(raw_data_directory, filenames[0])))
        older_data = json.load(open(os.path.join(raw_data_directory, filenames[1])))
        reduced_data = []
        print(f"Comparing {filenames[0]} and {filenames[1]}")
        if "error" not in older_data and "error" not in newer_data:
            for data in newer_data["items"]:
                # this if-statement filters out ALL tracks with the same timestamp
                if data not in older_data["items"] and data["played_at"] not in all_timestamps:

                # this if-statement only filters out entries that are EXACTLY the same
                # this statement will keep the cases when a song with the same timestamp as previously is returned 
                # (less than X songs have been played since the last API request) if for example the popularity 
                # has changed in the meantime
                #if data not in older_data["items"]:
                    reduced_data.append(data)
                    all_timestamps.append(data["played_at"])

            newer_data["items"] = reduced_data
            with open(os.path.join("raw_data_cleaned", filenames[1]), "w") as outfile:
                outfile.write(json.dumps(older_data, indent=4))
                
            with open(os.path.join("raw_data_cleaned", filenames[0]), "w") as outfile:
                outfile.write(json.dumps(newer_data, indent=4))
            i += 1
        else:
            print("Error :O")
        filenames.pop(0)
    pass


def get_raw_data_names(raw_data_directory):
    filenames = []
    if os.path.exists(raw_data_directory): 
        if os.path.isdir(raw_data_directory):
            for filename in os.listdir(raw_data_directory):
                if ".json" in filename:
                    filenames.append(os.path.join(raw_data_directory, filename))
    return filenames


def validate_pls_I_wanna_sleep(source_dir, name):
    raw_list = get_raw_data_names(source_dir)
    clean_dict = {"song": []}
    full_dict = {"song": []}

    for item in raw_list:
        print(f"Evaluating Item {item}")
        data = json.load(open(item))
        for entry in data["items"]:
            clean_dict["song"].append( { item: { "name" : entry["track"]["name"], "timestamp": entry["played_at"]} } )
            full_dict["song"].append(entry)
    
    with open(name + ".json", "w") as outfile:
        outfile.write(json.dumps(clean_dict, indent=4))

    with open(name + "_all.json", "w") as outfile:
        outfile.write(json.dumps(full_dict, indent=4))
    return name + ".json"



def compare_json():
    pass


# 2023-11-10T23:45:17.947Z

def convert_timestamp(date_string):
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    date_object = datetime.datetime.strptime(date_string, date_format)
    return int(date_object.replace(tzinfo=pytz.UTC).timestamp() * 1000)


def validate_validation_plsssss(file):
    print("checking for duplicates")
    no_duplicates = True
    data = json.load(open(file))
    timestamps = []
    for item in data["song"]:
        timestamp = item[list(item.keys())[0]]["timestamp"]
        if timestamp in timestamps:
            no_duplicates = False
            print("penis")
            print("ALSO: TIMESTAMP IS ALREADY IN LIST")
            print(f"Timestamp: {timestamp}")
        timestamps.append(timestamp)

    if no_duplicates:
        print("no duplicates found")    


time = convert_timestamp("2023-11-10T23:45:17.947Z")
print(time)
normalize_raw_data()
unclean = validate_pls_I_wanna_sleep("raw_data","uncleaned_dict.json")
clean = validate_pls_I_wanna_sleep("raw_data_cleaned","cleaned_dict.json")

validate_validation_plsssss(clean)