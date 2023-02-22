import json
import os
import datetime
import psycopg2

def db_insert (dat_file_num, dat_file_name, blk_hash, unix_ts, ts, num_tx):

    q = f"""
        INSERT INTO index_tab (dat_file_num, dat_file_name, blk_hash, unix_ts, ts, num_tx)
        VALUES({dat_file_num},'{dat_file_name}','{blk_hash}', {unix_ts}, '{ts}', {num_tx});
    """

    return q

input_dir = "./result/"
fn_list = os.listdir(input_dir)

f = open("./db_conf.json", "r")
db_conf = json.load(f)
f.close()

conn = psycopg2.connect(
    database=db_conf["database"],
    user=db_conf["user"], 
    password=db_conf["password"], 
    host=db_conf["host"], 
    port= db_conf["port"]
)

print(conn)

cursor = conn.cursor()

for dat in fn_list:

    print ("Inserting data into index db for dat file: " + input_dir + dat)

    blk_num_str = dat.split("_")[1].split(".")[0]
    ind = int(blk_num_str)
    blk_name = "blk" + blk_num_str

    f = open(input_dir + dat, "r")
    df_json = json.load(f)
    f.close()

    for i in range(0, len(df_json)):

        hash_ = df_json[i]["hash"]
        unix_time = df_json[i]["time"]
        time_stamp = datetime.datetime.fromtimestamp(unix_time)
        num_tx = df_json[i]["nTx"]

        cursor.execute(db_insert(ind, blk_name, hash_, unix_time, time_stamp, num_tx))

    conn.commit()
    
    print(len(df_json), "records inserted successfully")