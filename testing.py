import os, json, datetime, pytz


def normalize_raw_data():
    # Keep the RAW data from the requests in it's original form but delete duplicates
    raw_data_directory = "raw_data"
    filenames = []
    if os.path.exists(raw_data_directory): 
        if os.path.isdir(raw_data_directory):
            for filename in os.listdir(raw_data_directory):
                if ".json" in filename:
                    #filenames.append(os.path.join(raw_data_directory, filename))
                    filenames.append(filename)
                        
    #print(filenames)

    # STILL NOT GOOOOOD :(

    # so sometime it skips the evaluation which is weird (example of files with same data: 1 0 1 1)

    print("come on") 
    print(filenames[0])
    filenames = filenames[::-1]
    i = 0
    print(filenames[0])
    while len(filenames) > 1:
        newer_data = json.load(open(os.path.join(raw_data_directory, filenames[0])))
        older_data = json.load(open(os.path.join(raw_data_directory, filenames[1])))
        print(f"Comparing {filenames[0]} and {filenames[1]}")
        try:
            if "error" not in older_data and "error" not in newer_data:
                #print("not error :)")
                for data in newer_data["items"]:
                    if data in older_data["items"]:
                        #print("inside owo")
                        index = newer_data["items"].index(data)
                        newer_data["items"].pop(index)

                with open(os.path.join("raw_data_cleaned", filenames[1]), "w") as outfile:
                    outfile.write(json.dumps(older_data, indent=4))
                
                with open(os.path.join("raw_data_cleaned", filenames[0]), "w") as outfile:
                    outfile.write(json.dumps(newer_data, indent=4))
                i += 1
            else:
                print("Error :O")
        except:
            print("Fehler :C")
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

    for item in raw_list:
        print(f"Evaluating Item {item}")
        data = json.load(open(item))
        for entry in data["items"]:
            clean_dict["song"].append( { item: { "name" : entry["track"]["name"], "timestamp": entry["played_at"] } } )
    
    with open(name, "w") as outfile:
        outfile.write(json.dumps(clean_dict, indent=4))
    #print(clean_dict)



def compare_json():
    pass


# 2023-11-10T23:45:17.947Z

def convert_timestamp(date_string):
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    date_object = datetime.datetime.strptime(date_string, date_format)
    return int(date_object.replace(tzinfo=pytz.UTC).timestamp() * 1000)



time = convert_timestamp("2023-11-10T23:45:17.947Z")
print(time)
#normalize_raw_data()
#validate_pls_I_wanna_sleep("raw_data","uncleaned_dict.json")
#validate_pls_I_wanna_sleep("raw_data_cleaned","cleaned_dict.json")