# -*- coding: utf-8 -*-
#
# Blockchain parser
# Copyright (c) 2015-2021 Denis Leonov <466611@gmail.com>
#

import os
import datetime
import hashlib
import sys
import json
import gzip
from cryptotools.BTC import decode_scriptpubkey

# If this variable is set to true then the output block list (blocklist_#####.json) will be sorted chronologically
sort = True

def reverse(input):
    L = len(input)
    if (L % 2) != 0:
        return None
    else:
        Res = ''
        L = L // 2
        for i in range(L):
            T = input[i*2] + input[i*2+1]
            Res = T + Res
            T = ''
        return (Res);

def merkle_root(lst): # https://gist.github.com/anonymous/7eb080a67398f648c1709e41890f8c44
    sha256d = lambda x: hashlib.sha256(hashlib.sha256(x).digest()).digest()
    hash_pair = lambda x, y: sha256d(x[::-1] + y[::-1])[::-1]
    if len(lst) == 1: return lst[0]
    if len(lst) % 2 == 1:
        lst.append(lst[-1])
    return merkle_root([hash_pair(x,y) for x, y in zip(*[iter(lst)]*2)])

def read_bytes(file,n,byte_order = 'L'):
    data = file.read(n)
    if byte_order == 'L':
        data = data[::-1]
    data = data.hex()
    return data

def read_varint(file):
    b = file.read(1)
    bInt = int(b.hex(),16)
    c = 0
    data = ''
    if bInt < 253:
        c = 1
        data = b.hex()
    if bInt == 253: c = 3
    if bInt == 254: c = 5
    if bInt == 255: c = 9
    for j in range(1,c):
        b = file.read(1)
        b = b.hex()
        data = b + data
    return data

dirA = './blocks/' # Directory where blk*.dat files are stored
dirB = './result/' # Directory to save parsing results

fList = os.listdir(dirA)
fList = [x for x in fList if (x.endswith('.dat') and x.startswith('blk'))]
fList.sort()

for i in fList:
    nameSrc = i
    basename = nameSrc.split('.')[0]
    blockFileNum = basename[3:]
    resList = []
    blockList = []
    a = 0 # No idea what this counter does BTW ¯\_(ツ)_/¯ 
    t = dirA + nameSrc
    # resList.append('Start ' + t + ' in ' + str(datetime.datetime.now()))
    print ('Start ' + t + ' in ' + str(datetime.datetime.now()))
    f = open(t,'rb')
    tmpHex = ''
    fSize = os.path.getsize(t)
    while f.tell() != fSize:
        thisBlock = {}
        tmpHex = read_bytes(f,4)
        magicBytes = tmpHex 
        # Note that this parser reads sequentially and doesn't verify the magic bytes
        # The magic bytes should be d9b4bef9, but there is no assertion here 
        tmpHex = read_bytes(f,4)
        thisBlock['size'] = int(tmpHex, 16)
        tmpPos3 = f.tell()  # Log position of block header
        # Read and hash the block header to get the hash of the current block
        tmpHex = read_bytes(f,80,'B')
        tmpHex = bytes.fromhex(tmpHex)
        tmpHex = hashlib.new('sha256', tmpHex).digest()
        tmpHex = hashlib.new('sha256', tmpHex).digest()
        tmpHex = tmpHex[::-1]        
        tmpHex = tmpHex.hex()
        thisBlock['hash'] = tmpHex
        # Seek back to start of block header to get the hash of the previous block.
        f.seek(tmpPos3,0)
        tmpHex = read_bytes(f,4)
        thisBlock['version'] = int(tmpHex,16)
        # Oh, look, this is where the magic happens. Blockchain!
        tmpHex = read_bytes(f,32)
        thisBlock['previousblockhash'] = tmpHex
        tmpHex = read_bytes(f,32)
        thisBlock['merkleroot'] = tmpHex
        MerkleRoot = tmpHex
        tmpHex = read_bytes(f,4)
        thisBlock['time'] = int(tmpHex,16)
        tmpHex = read_bytes(f,4)
        thisBlock['difficulty'] = int(tmpHex,16)
        tmpHex = read_bytes(f,4)
        thisBlock['nonce'] = int(tmpHex,16)
        # Do this wacky variable length read to get the number of transactions
        tmpHex = read_varint(f)
        # Stuff the no of transactions into txCount and loop over the range
        txCount = int(tmpHex,16)
        thisBlock['nTx'] = int(tmpHex,16)
        thisBlock['tx'] = []
        tmpHex = ''; RawTX = ''; tx_hashes = []
        # Note: RawTX is carrying through the program taking the read
        # and appending the raw bits to itself as text.
        # However it is never written to anything.
        for k in range(txCount):
            transaction = {}
            tmpHex = read_bytes(f,4)
            transaction['version'] = int(tmpHex,16)
            RawTX = reverse(tmpHex)
            tmpHex = ''
            Witness = False # Set witness to False as default. Not what I would do but... 
            b = f.read(1)
            tmpB = b.hex()
            bInt = int(b.hex(),16)
            if bInt == 0:
                tmpB = ''
                f.seek(1,1)
                c = 0
                c = f.read(1)
                bInt = int(c.hex(),16)
                tmpB = c.hex()
                Witness = True
            c = 0
            if bInt < 253:
                c = 1
                tmpHex = hex(bInt)[2:].zfill(2)
                tmpB = ''
            if bInt == 253: c = 3
            if bInt == 254: c = 5
            if bInt == 255: c = 9
            for j in range(1,c):
                b = f.read(1)
                b = b.hex()
                tmpHex = b + tmpHex
            inCount = int(tmpHex,16) # Get number of inputs and loop over them
            tmpHex = tmpHex + tmpB
            RawTX = RawTX + reverse(tmpHex)
            transaction['vin'] = []  # Instantiate Input transaction list
            for m in range(inCount):
                transactionInput = {}
                tmpHex = read_bytes(f,32)
                transactionInput['txid'] = tmpHex
                RawTX = RawTX + reverse(tmpHex)
                tmpHex = read_bytes(f,4)                
                transactionInput['vout'] = int(tmpHex,16)
                RawTX = RawTX + reverse(tmpHex)
                # I don't entirely know what it's doing in this next block
                # Something about figuring out length of the script signature
                tmpHex = ''
                b = f.read(1)
                tmpB = b.hex()
                bInt = int(b.hex(),16)
                c = 0
                if bInt < 253:
                    c = 1
                    tmpHex = b.hex()
                    tmpB = ''
                if bInt == 253: c = 3
                if bInt == 254: c = 5
                if bInt == 255: c = 9
                for j in range(1,c):
                    b = f.read(1)
                    b = b.hex()
                    tmpHex = b + tmpHex
                scriptLength = int(tmpHex,16)
                tmpHex = tmpHex + tmpB
                RawTX = RawTX + reverse(tmpHex)
                tmpHex = read_bytes(f,scriptLength,'B')
                transactionInput['scriptSig_hex'] = tmpHex
                RawTX = RawTX + tmpHex
                tmpHex = read_bytes(f,4,'B')
                transactionInput['sequence'] = int(tmpHex,16)
                # Append to input list
                transaction['vin'].append(transactionInput)
                RawTX = RawTX + tmpHex
                tmpHex = ''
            b = f.read(1)
            tmpB = b.hex()
            bInt = int(b.hex(),16)
            c = 0
            if bInt < 253:
                c = 1
                tmpHex = b.hex()
                tmpB = ''
            if bInt == 253: c = 3
            if bInt == 254: c = 5
            if bInt == 255: c = 9
            for j in range(1,c):
                b = f.read(1)
                b = b.hex()
                tmpHex = b + tmpHex
            # Find number of outputs and loop over that
            outputCount = int(tmpHex,16)
            transaction['vout'] = []
            tmpHex = tmpHex + tmpB
            RawTX = RawTX + reverse(tmpHex)
            for m in range(outputCount):
                transactionOutput = {}
                tmpHex = read_bytes(f,8)
                Value = int(tmpHex,16)/100000000
                transactionOutput['value'] = Value
                RawTX = RawTX + reverse(tmpHex)
                tmpHex = ''
                b = f.read(1)
                tmpB = b.hex()
                bInt = int(b.hex(),16)
                c = 0
                if bInt < 253:
                    c = 1
                    tmpHex = b.hex()
                    tmpB = ''
                if bInt == 253: c = 3
                if bInt == 254: c = 5
                if bInt == 255: c = 9
                for j in range(1,c):
                    b = f.read(1)
                    b = b.hex()
                    tmpHex = b + tmpHex
                scriptLength = int(tmpHex,16)
                tmpHex = tmpHex + tmpB
                RawTX = RawTX + reverse(tmpHex)
                tmpHex = read_bytes(f,scriptLength,'B')
                # get scriptPubKey hex value
                scriptPubKey = tmpHex
                if scriptPubKey[:2] == '6a':
                    outputDict = {
                    'hex' : scriptPubKey,
                    'type' : 'nulldata'
                    }
                else:
                    try:
                        outputDict = decode_scriptpubkey(scriptPubKey)
                    except:
                        outputDict = {
                        'hex' : scriptPubKey,
                        'type' : 'unknown'
                        }
                transactionOutput['scriptPubKey'] = outputDict 
                transaction['vout'].append(transactionOutput)
                RawTX = RawTX + tmpHex
                tmpHex = ''
            # Now we have to handle the segregated witness data and plug it back into the inputs
            if Witness == True:
                for m in range(inCount):
                    # Create witness script list for item m on input
                    transaction['vin'][m]['txinwitness'] = []
                    tmpHex = read_varint(f)
                    WitnessLength = int(tmpHex,16)
                    for j in range(WitnessLength):
                        tmpHex = read_varint(f)
                        WitnessItemLength = int(tmpHex,16)
                        tmpHex = read_bytes(f,WitnessItemLength)
                        # This is in little Endian so it needs to be reversed
                        witnessScript = reverse(tmpHex)
                        # Append to item m on input
                        transaction['vin'][m]['txinwitness'].append(witnessScript)
                        tmpHex = ''
            Witness = False
            tmpHex = read_bytes(f,4)
            transaction['locktime'] = int(tmpHex,16)
            RawTX = RawTX + reverse(tmpHex)
            tmpHex = RawTX
            tmpHex = bytes.fromhex(tmpHex)
            tmpHex = hashlib.new('sha256', tmpHex).digest()
            tmpHex = hashlib.new('sha256', tmpHex).digest()
            tmpHex = tmpHex[::-1]
            tmpHex = tmpHex.hex()
            transaction['txid'] = tmpHex # Transaction txid
            thisBlock['tx'].append(transaction) # Append tx ID to block data
            tx_hashes.append(tmpHex)
            tmpHex = ''; RawTX = ''
        a += 1  # Still no idea what this counter does
        tx_hashes = [bytes.fromhex(h) for h in tx_hashes]
        tmpHex = merkle_root(tx_hashes).hex()
        if tmpHex != MerkleRoot:
            print ('Merkle roots does not match! >',MerkleRoot,tmpHex)
        blockList.append(thisBlock)
    f.close()
    
    # Sorting implimentation
    
    if sort == True:
        
        def getTime(elem):
            return int(elem["time"])

        blockListSorted = sorted(blockList, key=getTime, reverse=False)
        
    # Printing datetime of first and last block in each dat file    
    # print(datetime.datetime.utcfromtimestamp(blockListSorted[0]["time"]).strftime('%Y-%m-%d %H:%M:%S'))
    # print(datetime.datetime.utcfromtimestamp(blockListSorted[-1]["time"]).strftime('%Y-%m-%d %H:%M:%S'))
    
    with open(dirB + 'blocklist_' + blockFileNum + '.json','wt', encoding='ascii') as file:
        json.dump(blockList,file, indent=4)

# dirB_blk = './result/block_list/' # Directory where to save parsing results for block headers
# dirB_tx = './result/tr_list/' # Directory where to save parsing results for transactions