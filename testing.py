import os, json


def normalize_raw_data():
    # Keep the RAW data from the requests in it's original form but delete duplicates
    raw_data_directory = "raw_data_copy"
    filenames = []
    if os.path.exists(raw_data_directory): 
        if os.path.isdir(raw_data_directory):
            for filename in os.listdir(raw_data_directory):
                if ".json" in filename:
                    filenames.append(os.path.join(raw_data_directory, filename))
                        
    #print(filenames)

    # STILL NOT GOOOOOD :(

    print("come on") 
    filenames = filenames[::-1]
    i = 0
    while len(filenames) > 1:
        newer_data = json.load(open(filenames[0]))
        older_data = json.load(open(filenames[1]))
        try:
            if "error" not in older_data and "error" not in newer_data:
                print("not error :)")
                cleared_data = []
                for data in newer_data["items"]:
                    if data in older_data["items"]:
                        print("inside owo")
                        index = newer_data["items"].index(data)
                        newer_data["items"].pop(index)

                with open(filenames[1], "w") as outfile:
                    outfile.write(json.dumps(older_data, indent=4))
                
                with open(filenames[0], "w") as outfile:
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
        data = json.load(open(item))
        for entry in data["items"]:
            clean_dict["song"].append( { "name" : entry["track"]["name"], "timestamp": entry["played_at"] })
    
    with open(name, "w") as outfile:
        outfile.write(json.dumps(clean_dict, indent=4))
    
    print(clean_dict)



def compare_json():
    pass


#normalize_raw_data()
validate_pls_I_wanna_sleep("raw_data","uncleaned_dict")
validate_pls_I_wanna_sleep("raw_data_copy","cleaned_dict")