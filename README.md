# BTC 2 NEO4J

This package contains both a parser for blk.dat files and an import script for dumping the json into Neo4J graph database.

## Get .DAT files

I obtained the files need by starting my own validation node for Bitcoin using Bitcoin Core. The process is explained more fully in the link below:

[Setup BTC Validation Node](https://link-url-here.org)

## Basic Setup

1. Pull down this repo from GitHub
2. cd into the main directory BTC_2_NEO4J

### Auto Setup

1. *** MUST have PowerShell version 7.0 or higher in windows to run setup.py
2. Run the setup.py script to setup

### Manual Setup

1. Create a directory called "blocks"
2. Create a directory called "result"
3. Create a directory called "logs"
4. Create a json file called "checkpoint.json" with the following content:

{

    "dat_file": 0,
    "iter": 0,
    "block_num": 0

}

5. Create a json file called "email_conf.json" with the following content:

{

    "e_addr": <insert_email_address>,
    "e_pass": <insert_password>

}

6. Create and activate a virtual enviroment
7. Use pip install -r requirements.txt to install needed libraries

*** NOTE replace requirements.txt with requirements_lin_mac.txt for linux or mac users and requirements_win.txt for windows users ***

### Add .DAT Files

Add desired BTCXXXXX.DAT files into blocks folder

## Run Parser

1. Run dat_to_json.py script

## Setup Neo4J Locally

1. Create new Database in Neo4J as follows:
   1. project name: BTC Ledger Test
   2. DBMS name: BTC_NEO4J
   3. password: password

## Import to Neo4J

1. If you want the script to email you in case of any error or upon completetion edit the email_conf.json file with your email and password. Also, set variable "emailMe" in json_to_neo4j.py to "True". It will be "False" by default. If you are planning on using Gmail, read the article in the following link for more information on how to setup the authentication so it will work with Gmail.

[Setup Auth for Gmail](https://leimao.github.io/blog/Python-Send-Gmail/)

2. Run json_to_neo4j.py
3. This will import all the json information into Neo4J
4. Once finished open the browser in neo4j to interact with the BTC graph you have created

***!!!

NOTE: If the import terminates early due to an error or any other reason the progress is saved in the checkpoint.json file. Simply run the script again to begin again from the same point the script last exited. However, if you wish you import from the beginning (BLK00000.dat) reset the checkpoint.json file to the following:

{

    "dat_file": 0,
    "iter": 0,
    "block_num": 0

}

***!!!
