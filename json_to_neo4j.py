

# BTC 2 NEO4J IMPORTER
# Spencer King <sking115422@gmail.com>

# Command to run
# json_to_neo4j.py

### NOTES

# Non parallelized code > 25% of DAT file > ~ 125000 node > ~150000 relationships > in ~ 2 hours

# Bitcoin Owned By An Address
# Can be calculated by summing all outputs locked to an address with an out-degree of 0. 
# In other words if an output has an out going relationship it means that is has been unlocked for use in another transaction.
# Its link to the address is no longer valid.

# Importing libraries
from neo4j import GraphDatabase
from py2neo import Graph
import numpy as np
import pandas as pd
import json
import os
import traceback
from datetime import datetime
import pytz
import time
import logging
import smtplib
import ssl
from email.message import EmailMessage


# Logging
# If logging is set to true it will record the creation of every node and relationship in a log file called "logs"
# Other good format: '%(name)s > %(process)d > %(levelname)s:     %(message)s'
logging_ = True
logger = logging.getLogger('logger')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('./logs/json_to_neo4j.log', mode='a+')
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


# Emailing
# This script can email you if there is an error while running or if it runs successfully to completion
# If you wish to use this feature please set "emailMe" below to true
emailMe = True

email_conf = open("./email_conf.json")
ec = json.load(email_conf)

port = 465 # for SSL
serv = "smtp.gmail.com"
e_addr = ec["e_addr"]
e_pass = ec["e_pass"] # This comes from an app pass in google account ~ link to article is here: https://leimao.github.io/blog/Python-Send-Gmail/
from_ = e_addr
to = e_addr


def getTimeStamp ():
    
    EST = pytz.timezone("EST")
    datetime_est = datetime.now(EST)
    ct = datetime_est.strftime('%Y:%m:%d %H:%M:%S %Z %z')
    return str(ct)


def createBlockNode (sess, blk, df_start, bn_start):
    
    hash_ = str(blk["hash"])
    
    try:
        version = str(blk["version"])
    except:
        version = "null"
        
    previousBlockHash = str(blk["previousblockhash"])
    
    try:
        merkleRoot = str(blk["merkleroot"])
    except:
        merkleRoot
    try:
        time = str(blk["time"])
    except:
        time = "null"
    try:
        difficulty = str(blk["difficulty"])
    except:
        difficulty = "null"
    try:
        nonce = str(blk["nonce"])
    except:
        nonce = "null"
        
    numTransactions = str(blk["nTx"])
    dat_file_num = str(df_start)
    blk_num = str(bn_start)
    
    ps = "CREATE (n:block {" 
    p1 = "hash: '" + hash_ + "', "
    p2 = "version: '" + version + "', "
    p3 = "previousBlockHash: '" + previousBlockHash + "', "
    p4 = "merkleRoot: '" + merkleRoot + "', "
    p5 = "time: '" + time + "', "
    p6 = "difficulty: '" + difficulty + "', "
    p7 = "nonce: '" + nonce + "', "
    p8 = "numTranactions: '" + numTransactions +"', "
    p9 = "datFileNum: '" + dat_file_num + "', "
    p10 = "blkNum: '" + blk_num + "' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + p9 + p10 + pl
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating node : block > hash_id : " + hash_)
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id


def createCoinbaseNode (sess, tx, df_start, bn_start):

    value = str(tx["vout"][0]["value"])
    dat_file_num = str(df_start)
    blk_num = str(bn_start)
        
    ps = "CREATE (n:coinbase {"
    p1 = "value: '" + value + "', "
    p2 = "datFileNum: '" + dat_file_num + "', "
    p3 = "blkNum: '" + blk_num + "' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3 + pl
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating node : coinbase > value : " + value)
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id


def createTxNode (sess, tx, df_start, bn_start):
    
    txid = str(tx["txid"])
    
    try:
        version = str(tx["version"])
    except:
        version = "null"
        
    dat_file_num = str(df_start)
    blk_num = str(bn_start)
    
    ps = "CREATE (n:tx {"
    p1 = "txid: '" + txid +"', "
    p2 = "version: '" + version +"', "
    p3 = "datFileNum: '" + dat_file_num + "', "
    p4 = "blkNum: '" + blk_num + "' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3 + p4 + pl
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating node : tx > txid : " + txid)
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id


def createOutputNode(sess, tx, ind, df_start, bn_start):
    
    vout = str(ind)
    value = str(tx["value"])
    scriptPK_hex = str(tx["scriptPubKey"]["hex"])

    try:
        scriptPK_asm = str(tx["scriptPubKey"]["asm"])
    except:
        scriptPK_asm = "null"
    try:
        type_ = str(tx["scriptPubKey"]["type"])
    except:
        type_ = "null"
    try:
        address = str(tx["scriptPubKey"]["address"])
    except:
        address = "null"
        
    dat_file_num = str(df_start)
    blk_num = str(bn_start)
    
    ps = "CREATE (n:output {"
    p1 = "vout: '" + vout + "', "
    p2 = "value: '" + value + "', "
    p3 = "scriptPK_hex: '" + scriptPK_hex + "', "
    p4 = "scriptPK_asm: '" + scriptPK_asm + "', "
    p5 = "type: '" + type_ + "', "
    p6 = "address: '" + address + "', "
    p7 = "datFileNum: '" + dat_file_num + "', "
    p8 = "blkNum: '" + blk_num + "' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3+ p4 + p5 + p6 + p7 + p8 + pl
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating node : output > value : " + value)
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id
    

def checkAddressExists (sess, addr):
    
    cmd1 = "MATCH (n:address) WHERE n.address = '" + addr + "' RETURN n"
    
    ret = sess.run(cmd1)
    
    if len(ret.data()) == 0:
        return False
    else:
        return True
        

def createAddressNode(sess, address, df_start, bn_start):
        
    exists = False
    
    exists = checkAddressExists(sess, address)
    
    dat_file_num = str(df_start)
    blk_num = str(bn_start)

    if not exists :
        
        ps = "CREATE (n:address {"
        p1 = "address: '" + address + "', "
        p2 = "datFileNum: '" + dat_file_num + "', "
        p3 = "blkNum: '" + blk_num + "' "
        pl = "}) RETURN id(n)"
        
        cmd1 = ps + p1 + p2 + p3 + pl
        
        if logging_:
            logger.debug(getTimeStamp() + " | creating node : address > address : " + address)
        
        ret = sess.run(cmd1)

        node_id = ret.data()[0]["id(n)"]
        
        return node_id
            

def createChainRel(sess, prevBlkHash, hash_):

    cmd1 = """ 
            MATCH
            (a:block),
            (b:block)
            WHERE 
            a.hash = "{0}"
            AND
            b.hash = "{1}"
            MERGE (a)-[r:chain]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(prevBlkHash, hash_)
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : chain")
    
    sess.run(cmd1)


def createRewardRel(sess, n4j_blk_id, n4j_cb_id):

    cmd1 = """ 
            MATCH
            (a:block),
            (b:coinbase)
            WHERE 
            id(a) = {0}
            AND
            id(b) = {1}
            MERGE (a)-[r:reward]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(str(n4j_blk_id), str(n4j_cb_id))
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : reward")
    
    sess.run(cmd1)
    

def createSeedsRel(sess, n4j_cb_id, n4j_tx_id):   

    cmd1 = """ 
            MATCH
            (a:coinbase),
            (b:tx)
            WHERE 
            id(a) = {0} 
            AND
            id(b) = {1}
            MERGE (a)-[r:seeds]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(str(n4j_cb_id), str(n4j_tx_id))
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : seeds")
    
    sess.run(cmd1)


def createIncludesRel(sess, n4j_tx_id, n4j_blk_id):

    cmd1 = """ 
            MATCH
            (a:tx),
            (b:block)
            WHERE 
            id(a) = {0}
            AND
            id(b) = {1}
            MERGE (a)-[r:includes]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(str(n4j_tx_id), str(n4j_blk_id))
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : includes")
            
    sess.run(cmd1)


def createOutRel(sess, n4j_tx_id, n4j_out_id):

    cmd1 = """ 
            MATCH
            (a:tx),
            (b:output)
            WHERE 
            id(a) = {0}
            AND
            id(b) = {1}
            MERGE (a)-[r:out]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(str(n4j_tx_id), str(n4j_out_id))
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : out")
            
    sess.run(cmd1)


def createLockedRel(sess, scriptPK_hex, address):

    cmd1 = """ 
            MATCH
            (a:output),
            (b:address)
            WHERE 
            a.scriptPK_hex = "{0}"
            AND
            b.address = "{1}"
            MERGE (a)-[r:locked]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(scriptPK_hex, address)
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : locked")
            
    sess.run(cmd1)


def createUnlockRel(sess, vin, tx_data):
    
    # Input transaction id
    txid_in = str(vin["txid"])
    # Output number in the input transaction
    vout = str(vin["vout"])
    scriptSig_ = str(vin["scriptSig_hex"])
    
    # Current tranaction id
    txid = str(tx_data["txid"])

    cmd1 = 'MATCH (a:tx) - [:out]-> (b:output) WHERE a.txid = "{0}" AND b.vout = "{1}" '
    cmd1 = cmd1.format(txid_in, vout, txid)
    
    cmd2 = 'MATCH (c:tx) WHERE c.txid = "{0}" '
    cmd2 = cmd2.format(txid) 
        
    cmd3 = "MERGE (b) - [r:unlock {scriptSig: '" + scriptSig_ + "'}] -> (c) RETURN r"
        
    cmd4 = cmd1 + cmd2 + cmd3
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : unlock > scriptsig : " + scriptSig_)
    
    sess.run(cmd4)


def checkIndexExists(sess, nodelLabel, propName):
    
    cmd1 = "CALL db.indexes()"
    
    ret = sess.run(cmd1)
    
    index_list = ret.data()
    
    exists = False
    
    for each in index_list:
        
        labelsOrTypes = each["labelsOrTypes"]
        properties = each["properties"]
        
        if len(labelsOrTypes) > 0 and len(properties) > 0:
            if (labelsOrTypes[0] == nodelLabel and properties[0] == propName):
                exists = True
    
    return exists


def createIndex (sess, nodeLabel, propName):
    
    # CALL db.indexes()
    # DROP INDEX ON :nodeLabel(propName)
    
    cmd1 = "CREATE INDEX ON :" + nodeLabel + "(" + propName + ")"
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating index > nodetype : " + nodeLabel + " > propname : " + propName)
        
    if not checkIndexExists(sess, nodeLabel, propName):
        sess.run(cmd1)
        
        
def deleteBlockNodes (sess, datFileNum, blkNum):
    
    datFileNum = str(datFileNum)
    blkNum = str(blkNum)
    
    cmd1 =  """ 
            match (n)
            where 
            n.datFileNum = "{0}" 
            and 
            n.blkNum = "{1}"
            detach delete n
            """
            
    cmd1 = cmd1.format(datFileNum, blkNum)

    if logging_:
        logger.debug(getTimeStamp() + " | deleted block : " + blkNum + " from DAT file : " + datFileNum)
            
    sess.run(cmd1)
    
    
def sendEmail(port, serv, e_addr, e_pass, sub, from_, to, cont):
    
    port = port  
    smtp_server = serv
    sender_email = e_addr  
    receiver_email = e_addr 
    password = e_pass

    msg = EmailMessage()
    
    msg['Subject'] = sub
    msg['From'] = from_
    msg['To'] = to
    msg.set_content(cont)
    
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.send_message(msg, from_addr=sender_email, to_addrs=receiver_email)
    
    

# Creating neo4j database driver and session
# graph = Graph(uri='neo4j://localhost:7687', user="neo4j", password="password")
dbc = GraphDatabase.driver(uri = "bolt://localhost:7687", auth=("neo4j", "password"))
logger.debug("")
logger.debug("Connected to Neo4J database : " + str(dbc))
sess = dbc.session(database="neo4j")

input_dir = "./result_test/"

# Return lists of the DAT file jsons
dat_list = sorted(os.listdir(input_dir))

# Import checkpoint values 
cp = open("./checkpoint.json")
cpjs = json.load(cp)
df_start = cpjs["dat_file"]
iter_start = cpjs["iter"]
bn_start = cpjs["block_num"]

# If checkpoint.json file is not 0, 0 delete last partial imported block
if df_start != 0 or bn_start != 0:
    deleteBlockNodes(sess, df_start, bn_start)


### MAIN DRIVER CODE


# Try until some error happens
try:
    
    logger.debug("")
    logger.debug("***********************************")
    logger.debug("STARTING IMPORT TO NEO4J")
    logger.debug("***********************************")
    logger.debug("")
    
    
    # Iterating through the every DAT file twice
    # First time (0) creates the nodes and some relationships
    # Second time (1) creates the backward linked relationships
    for t in range (iter_start, 2):
        
        start2 = time.time()
        
        logger.debug("ITERATION " + str(t) + " STARTED")
        logger.debug("***********************************")
        
        # Reset bn_start each time iteration is finished
        if t != iter_start:
            bn_start = 0    
            df_start = 0
            
        iter_start = t
    
        # Iterating through each DAT file
        # testing loop below
        # for a in range(0, 1):
        for a in range(df_start, len(dat_list)):
            
            # Reset bn_start each time DAT file finished
            if a != df_start:
                bn_start = 0
                       
            
            df_start = a
            
            dat_name = dat_list[a]
            
            logger.debug("loading > " + input_dir + dat_name)
            with open(input_dir + dat_name) as bl:
                bl_json = json.load(bl)

                #Iterate through all blocks in single dat file
                # testing loops below
                # for i in range(len(bl_json)-5, len(bl_json)):    
                # for i in range(0, 10):
                for i in range(bn_start, len(bl_json)):
                    
                    bn_start = i
                    
                    start1 = time.time()
                    
                    blk = bl_json[i]
                    tx_list = blk["tx"]
                    
                    prevBlkHash = str(blk["previousblockhash"])
                    hash_ = str(blk["hash"])
                    
                    n4j_blk_id = None
                    if t == 0:
                        n4j_blk_id = createBlockNode(sess, blk, df_start, bn_start)

                    if t == 1:
                        # If block is the not the first block create chain relationship
                        if prevBlkHash != "0000000000000000000000000000000000000000000000000000000000000000":
                            createChainRel(sess, prevBlkHash, hash_)
                    
                    #Iterating through each transaction associated with the current block
                    for j in range(0, len(tx_list)):
                    # for j in range(0,1):
                        
                        tx_data = tx_list[j]
                        
                        # Creating transaction node and includes relationship
                        n4j_tx_id = None
                        if t == 0:
                            n4j_tx_id = createTxNode(sess, tx_data, df_start, bn_start)
                            createIncludesRel(sess, n4j_tx_id, n4j_blk_id)
                        
                        # Iterate through vin list in transaction
                        for each in tx_data["vin"]:
                            
                            # If it is the first transaction create coinbase node, reward relationship, and seeds relationship
                            txid_in = str(each["txid"])
                            if txid_in == "0000000000000000000000000000000000000000000000000000000000000000":
                                if t == 0:
                                    n4j_cb_id = createCoinbaseNode(sess, tx_data, df_start, bn_start)
                                    createRewardRel(sess, n4j_blk_id, n4j_cb_id)
                                    createSeedsRel(sess, n4j_cb_id, n4j_tx_id)
                                exit
                                
                            # Otherwise create the unlock relationship
                            else:
                                if t == 1:
                                    createUnlockRel(sess, each, tx_data)
                        
                        # Iterate through outputs for each transaction
                        outputs = tx_data["vout"]
                        for z in range(0, len(outputs)):
                            
                            # Creating output node 
                            n4j_out_id = None
                            if t == 0:
                                n4j_out_id = createOutputNode(sess, outputs[z], z, df_start, bn_start)
                                createOutRel(sess, n4j_tx_id, n4j_out_id)
                            
                            scriptPK_hex = str(outputs[z]["scriptPubKey"]["hex"])
                            
                            try:
                                address = str(outputs[z]["scriptPubKey"]["address"])
                            except:
                                address = "N/A"
                                
                            # Create address node if it does not exist and locked relationship to an address (might be address just created might not)    
                            if address != "N/A":
                                n4j_addr_id = None
                                if t == 0:
                                    n4j_addr_id = createAddressNode(sess, address, df_start, bn_start)
                                # if t == 1:
                                    createLockedRel(sess, scriptPK_hex, address)
                        
                    end1 = time.time()
                    
                    diff1 = end1 - start1
                    
                    if t == 0:
                        logger.debug(getTimeStamp() + " DAT file : " + str(df_start) + " > block: " + str(bn_start) + " | node import complete > execution time : " + str(diff1))
                        
                    if t == 1: 
                        logger.debug(getTimeStamp() + " DAT file : " + str(df_start) + " > block: " + str(bn_start) + " | relationships import complete > execution time : " + str(diff1))
                        
                end2 = time.time()
                diff2 = end2 - start2
                
                logger.debug("ITERATION " + str(t) + " FOR " + dat_name + " FINISHED > execution time : " + str(diff2))

    # Creating indexes
    createIndex(sess, "block", "hash")
    createIndex(sess, "tx", "txid")
    createIndex(sess, "address", "address")
    
    if emailMe:
        err_sub = "BTC 2 NEO4J IMPORT COMPLETE"
        err_msg = "The import is now complete!"
        sendEmail(port=port, serv=serv, e_addr=e_addr, e_pass=e_pass, sub=err_sub, from_=from_, to=to, cont=err_msg)
    
    logger.debug("")
    logger.debug("***********************************")
    logger.debug("FINISHED IMPORT TO NEO4J")
    logger.debug("***********************************")

    # Closing session
    sess.close()


# If error happens save progress
except:
    
    cpjs["dat_file"] = df_start
    cpjs["iter"] = iter_start
    cpjs["block_num"] = bn_start
    
    tmp = json.dumps(cpjs, indent=4)
    
    with open("checkpoint.json", "w") as outfile:
        outfile.write(tmp)
        
    fail_str = getTimeStamp() + " IMPORT EXITED EARLY > Dat file : " + str(df_start) + " > iter : " + str(iter_start) + " > blk num : " + str(bn_start)
    err = traceback.format_exc()
        
    logger.debug("")
    logger.debug(fail_str)
    logger.debug("")
    logger.debug(err)
    logger.debug("")
    
    if emailMe:
        err_sub = "BTC 2 NEO4J IMPORT ERROR"
        err_msg = fail_str + "\n\n" + err
        sendEmail(port=port, serv=serv, e_addr=e_addr, e_pass=e_pass, sub=err_sub, from_=from_, to=to, cont=err_msg)
    
    print(traceback.format_exc())
        

