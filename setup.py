

# Importing libraries
import os
import platform
import json

### ADDING DIRECTORIES 

# Directories to add
dirs = ["./blocks/", "./logs/", "./result"] 

# Loop through the directory list
for each in dirs:
    
    # Create a new directory if it does not exist
    isExist = os.path.exists(each)
    if not isExist:
        os.makedirs(each)
        print("directory created : " + each)
        
### CREATING CHECKPOINT JSON

# Creating dictionary of initial values
cp = {
    "dat_file":0,
    "iter": 0,
    "block_num":0
}

# Writing initial values to checkpoint.json file
tmp = json.dumps(cp, indent=4)
if not os.path.exists("./checkpoint.json"):
    with open("checkpoint.json", "w+") as outfile:
        outfile.write(tmp)
        
### CREATING EMAIL CONFIG FILE

# Creating dictionary of initial values
ec = {
    "e_addr": "<insert_email_here>",
    "e_pass": "<insert_pass_here>"
}

# Writing initial values to email_conf.json file
tmp = json.dumps(ec, indent=4)
if not os.path.exists("./email_conf.json"):
    with open("email_conf.json", "w+") as outfile:
        outfile.write(tmp)

### CREATING NEO4J DB CONFIG FILE

# Creating dictionary of initial values
# Default values for user and pass are "neo4j"
n4jc = {
    "host": "<server host name or IP here>",
    "port": 7474,
    "usr": "neo4j",
    "pass": "neo4j"
}

# Writing initial values to neo4j_db_conf.json file
tmp = json.dumps(n4jc, indent=4)
if not os.path.exists("neo4j_db_conf.json"):
    with open("neo4j_db_conf.json", "w+") as outfile:
        outfile.write(tmp)

        
### CREATING VIRTUAL ENVIRONMENT

# Determining OS type 
os_type = platform.system()

# Creating and setting up virtual environment
if os_type == "Darwin" or os_type == "Linux":
    if not os.path.exists("./venv/"):
        print("creating virtual environment")
        os.system("pip install virtualenv")
        os.system("virtualenv venv")
        os.system("sudo apt-get update && sudo apt-get upgrade")
    print("activating virtual environment and installing necessary libraries")
    os.system(". venv/bin/activate && pip install -r requirements_lin_mac.txt")
elif os_type == "Windows":
    if not os.path.exists("venv"):
        print("creating virtual environment")
        os.system("python -m venv venv")
    print("activating virtual environment")
    os.system("call venv/Scripts/activate && pip install -r requirements_win.txt")
else:
    print("current system OS not recongnized... please setup manually")
    
"setup complete!"

