# -*- coding: utf-8 -*-
"""
Created on Mon May 11 10:57:19 2020

@author: szk1
"""

import os
import shutil
import pandas as pd
import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

print('[', datetime.datetime.now(), '] ', "Connecting to Blob Storage with connection string...")
connect_str = "DefaultEndpointsProtocol=https;AccountName=automatedpipelinestore2;AccountKey=[ConnectionStringForBatchAccount]"
print("[", datetime.datetime.now(), "] ","Connection Established!")

# print("Required directory structure:\ntest\n --download\n --output")
print("[", datetime.datetime.now(), "] ","Checking if directory structure exists...")
cwd = os.getcwd()
dir1 = 'test'
dir2 = 'download'
dir3 = 'output'
fullpath1 = os.path.join(cwd, dir1)
fullpath2 = os.path.join(fullpath1, dir2)
fullpath3 = os.path.join(fullpath1, dir3)

if os.path.isdir(fullpath1):
    print("[", datetime.datetime.now(), "] ","test folder exists!")
    if not os.path.isdir(fullpath2):
        print("[", datetime.datetime.now(), "] ","Creating download folder...")
        os.makedirs(fullpath2)
        print("[", datetime.datetime.now(), "] ","Done!")
    else:
        print("[", datetime.datetime.now(), "] ","download folder exists!")
    if not os.path.isdir(fullpath3):
        print("[", datetime.datetime.now(), "] ","Creating output folder...")
        os.makedirs(fullpath3)
        print("[", datetime.datetime.now(), "] ","Done!")
    else:
        print("[", datetime.datetime.now(), "] ","output folder exists!")
else:
    print("[", datetime.datetime.now(), "] ","Directory structure does not exist.")
    print("[", datetime.datetime.now(), "] ","Creating Directory Structure...")
    os.makedirs(fullpath1)
    os.makedirs(fullpath2)
    os.makedirs(fullpath3)
    print("[", datetime.datetime.now(), "] ","Directory structure created!")

print("[", datetime.datetime.now(), "] ","Creating Blob Service Client")
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
print("[", datetime.datetime.now(), "] ","Blob Service Client created successfully!")

#access fileupload container
print("[", datetime.datetime.now(), "] ","Listing files present in 'fileupload' container...")
container_client = blob_service_client.get_container_client('fileupload')

blob_list = container_client.list_blobs()
for blob in blob_list:
    fileNameDownload = blob.name
    print("[", datetime.datetime.now(), "] ","File: ", fileNameDownload, '\n')
    
localPathDownload = fullpath2
downloadFilePath = os.path.join(localPathDownload, fileNameDownload)

print("[", datetime.datetime.now(), "] ","Creating a blob client using the file name obtained previously...")
blob_client1 = blob_service_client.get_blob_client(container='fileupload', blob=fileNameDownload)
print("[", datetime.datetime.now(), "] ","Writing data from blob to download folder...")
with open(downloadFilePath, "wb") as download_file:
    download_file.write(blob_client1.download_blob().readall())
print("[", datetime.datetime.now(), "] ","Download successful!")

#INSERT PANDAS OPERATIONS BELOW
#------------------------------------------------------------------------------    

print("[", datetime.datetime.now(), "] ","Starting transformation using PANDAS...")
try:
    df = pd.read_csv(downloadFilePath)
    
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '').str.replace('(', '').str.replace(')', '')
    
    df['totalrevenue'] = df['unitssold']*df['unitprice']
    df['totalcost'] = df['unitssold']*df['unitcost']
    df['totalprofit'] = df['totalrevenue'] - df['totalcost']
    
    df.to_csv(downloadFilePath, index=False)
    print("[", datetime.datetime.now(), "] ","Transformations completed succesfully! Proceeding to output operation")
except:
    print("[", datetime.datetime.now(), "] ","The fields in the .csv files don't match with the fields for salesReport.csv.\nProceeding to output operation.")
    
#------------------------------------------------------------------------------
#END PANDAS OPERATIONS HERE. SAVE YOUR DATAFRAME AS CSV IN THIS DIRECTORY.


# Create a file in local Documents directory to upload from
localPathOutput = fullpath3
localFilenameOutput = 'processed_' + fileNameDownload
outputFilePath = os.path.join(localPathOutput, localFilenameOutput)

print("[", datetime.datetime.now(), "] ","Copying downloaded file to output folder...")
shutil.copyfile(downloadFilePath, outputFilePath)
print("[", datetime.datetime.now(), "] ","Success!")

# Create a blob client using the local file name as the name for the blob
print("[", datetime.datetime.now(), "] ","Creating a blob client using the local file name as the name for the blob...")
blob_client2 = blob_service_client.get_blob_client(container='output', blob=localFilenameOutput)
print("[", datetime.datetime.now(), "] ","Writing data from output folder to the blob storage...")
with open(outputFilePath, "rb") as data:
    blob_client2.upload_blob(data, overwrite=True)
print("[", datetime.datetime.now(), "] ","Upload successful!")

print("[", datetime.datetime.now(), "] ","Please find the output file in the output folder of the Azure blob storage account.")


    