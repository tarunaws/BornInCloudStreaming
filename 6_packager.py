"""
This program will download transcoded files and join them to
single file.
"""
#Import Modules
import os #Interact with Operating system
import shutil #Use for file copy/paste
import datetime #working with date command
import subprocess #Process to be run in background
import time #Identify time
import uuid #Generate unique id
import sys #Better control over input and output
import random # Generate random numbers
import json # Handle javascript object notation data
import yaml # Reading yaml file in python
import math # Generate random numbers
import boto3 # Python module to interact with AWS.
import threading # Enable multitasking, would be use for s3 multipart upload
import glob #The glob module finds all the pathnames matching a specified pattern according to the rules used by the Unix shell
import tarfile #The tarfile module makes it possible to read and write tar archives
from pymongo import MongoClient #Mongo client to interact with mongodb
from kubernetes import client, config, watch #kubernetes job specific modules
from kubernetes.client import models as mymodel #kubernetes job specific modules
from boto3.s3.transfer import TransferConfig #S3 upload specific module
from pathlib import Path


#Megabyte to byte conversion 1048576 bytes
MB = 1024 * 1024
s3 = boto3.resource('s3')

class TransferCallback:

    def __init__(self, target_size):
        self._target_size = target_size
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}

    def __call__(self, bytes_transferred):
        """
        The callback method that is called by the transfer manager.

        Display progress during file transfer and collect per-thread transfer
        data. This method can be called by multiple threads, so shared instance
        data is protected by a thread lock.
        """
        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size * MB
            sys.stdout.write(
                f"\r{self._total_transferred} of {target} transferred "
                f"({(self._total_transferred / target) * 100:.2f}%).")
            sys.stdout.flush()

def download_with_default_configuration(bucket_name, object_key,
                                        download_file_path, file_size_mb):
    """
    Download a file from an Amazon S3 bucket to a local folder, using the
    default configuration.
    """
    transfer_callback = TransferCallback(file_size_mb)
    s3.Bucket(bucket_name).Object(object_key).download_file(
        download_file_path,
        Callback=transfer_callback)
    return transfer_callback.thread_info

def upload_with_chunksize_and_meta(local_file_path, bucket_name, object_key,
                                   file_size_mb, metadata=None):
    """
    Upload a file from a local folder to an Amazon S3 bucket, setting a
    multipart chunk size and adding metadata to the Amazon S3 object.

    The multipart chunk size controls the size of the chunks of data that are
    sent in the request. A smaller chunk size typically results in the transfer
    manager using more threads for the upload.

    The metadata is a set of key-value pairs that are stored with the object
    in Amazon S3.
    """
    transfer_callback = TransferCallback(file_size_mb)

    config = TransferConfig(multipart_chunksize= 2 * MB)
    extra_args = {'Metadata': metadata} if metadata else None
    s3.Bucket(bucket_name).upload_file(
        local_file_path,
        object_key,
        Config=config,
        ExtraArgs=extra_args,
        Callback=transfer_callback)
    return transfer_callback.thread_info

bucket_name = "psltranscoder"
file_size_mb = 1000

#DB Initialization
db_client = MongoClient("mongodb://db.bornincloudstreaming.com:27017/") #DB server address
db = db_client["CoreDB"] #Database name CoreDB
transcodeDb = db["transcodeDb"]
"""Database table name transcodeDb.
It is related to transcoding job status.
"""
frontEndDb = db["jobDetails"]
"""Database table name jobDetails.
It is related to job submission by fronend UI.
"""
templateDb = db["templateDetails"]
"""
Database table which store profiles template.
"""
bitrateLadder = db["profileDetails"]
"""
Database table which store multiple profiles to be use for
transcoding.
"""
k8sDb = db["k8sDb"]
"""
Database table which store k8s job related metadata.
"""

myPriority = "Normal"
#Considering  2 minimum servers
ffprobeSleep = 10


# Variable Initialization
psls3Bucket = "/media" #local folder in the container
distributed = os.path.join(psls3Bucket,"intermediate","distributed") #Right hand side is nested folder in S3
splitPath = os.path.join(psls3Bucket,"intermediate","split") #Right hand side is nested folder in S3
outputDirectoryMultipart = os.path.join("intermediate","split") #Right hand side is nested folder in S3
splitPathMultipart = "intermediate/split"
output = os.path.join(psls3Bucket, "output") #Right hand side is folder in S3
outputSplit=os.path.join(psls3Bucket,"intermediate","splitCompress") #Right hand side is nested folder in S3
outputComplete = os.path.join(psls3Bucket, "output") #Right hand side is folder in S3
rejected = os.path.join(psls3Bucket,"intermediate","rejected") #Right hand side is nested folder in S3
tempFile = os.path.join(psls3Bucket, "temp", "out.txt") #Right hand side is a temp file under nested folder in S3
localPath = "/bento4/bin"

#Function:- clean my pod
def cleanPod():
    os.chdir(localPath)
    if os.path.exists(str("fragmented")):
            shutil.rmtree(str("fragmented"))
    if os.path.exists(str("package")):
            shutil.rmtree(str("package"))

#Function :- mp4Fragment
def mp4fragment(jobId,profileName,frcontentId):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"package":"in progress"}})
    results = transcodeDb.find({"contentId":frcontentId})
    for result in results:
        outputSinglePath = result["outputSinglePath"]
        fileName = result['revFileName']
        outputBasePath = result["outputBasePath"]
        deliverySegmentSize = result["deliverySegmentSize"]
        originalFileDuration = result["originalFileDuration"]
        retryCount = result["retryCount"]
    f_str = fileName
    fresult = f_str.split(".",1)[0]
    frfile = os.path.join(outputSinglePath,profileName)
    a = str(fresult) + "_" + profileName + ".mp4"
    b = str(fresult) + "_" + profileName + "_f" + ".mp4"
    toBeFragment = os.path.join(frfile,a)
    os.chdir(localPath)
    if not os.path.exists(os.path.join(localPath,"fragmented")):
        os.mkdir("fragmented")
    if not os.path.exists(os.path.join(localPath,"package")):
        os.mkdir("package")
    fragmentPath = os.path.join(localPath,"fragmented")
    packagePath = os.path.join(localPath,"package")
    os.chdir(localPath)
    download_file_path = os.path.join(fragmentPath, a)
    src=toBeFragment
    srcMultipart = src.removeprefix('/media/')
    object_key = srcMultipart
    download_with_default_configuration(bucket_name, object_key,download_file_path, file_size_mb)
#Output Bitrate calculation
    profileBitrate = profileName + " Bitrate"
    profileDuration = profileName + " Duration"
    actualProfileDuration = "Actual" + " " + profileDuration
    command = f"ffprobe -v error {download_file_path}  -show_streams -show_format -print_format json"
    metadata = {}
    metadata = subprocess.getoutput(command)
    time.sleep(ffprobeSleep)
    qc = json.loads(metadata)
    bitrateInBit = qc["format"]["bit_rate"]
    bitrateInMb_print = str(round((float(bitrateInBit)/1024)/1024,2)) + " Mbps"
    durationInSec = qc["format"]["duration"]
    if originalFileDuration == durationInSec :
        transcodeDb.update_one({
                                "contentId":frcontentId
                                },
                                {
                                    "$set":{
                                                profileBitrate:bitrateInMb_print,
                                                actualProfileDuration:durationInSec,
                                                profileDuration:"Same as Original"

                                            }
                                        }
                                )
        final = os.path.join(packagePath,b)
        command = f"/bento4/bin/mp4fragment --fragment-duration {deliverySegmentSize} {download_file_path} {final}"
        tempFile = os.path.join(psls3Bucket, "temp", "out.txt")
        with open(tempFile, 'w') as f:
            results = subprocess.Popen(command,stdout = f,stderr = f,shell=True)
        time.sleep(5)
        os.remove(download_file_path)
        return frcontentId
    elif float(originalFileDuration) > float(durationInSec) + 1 :
        transcodeDb.update_one({"contentId":frcontentId}, {"$set":{"Transcoding Status": "Waiting for resource"}})
        return "Retranscode"
    else:
        transcodeDb.update_one({
                                    "contentId":frcontentId
                                },
                                {
                                    "$set":{
                                            profileBitrate:bitrateInMb_print,
                                            actualProfileDuration:durationInSec,
                                            profileDuration:"Duration Mismatched"
                                            }
                                        }
                                )
        final = os.path.join(packagePath,b)
        command = f"/bento4/bin/mp4fragment --fragment-duration {deliverySegmentSize} {download_file_path} {final}"
        tempFile = os.path.join(psls3Bucket, "temp", "out.txt")
        with open(tempFile, 'w') as f:
            results = subprocess.Popen(command,stdout = f,stderr = f,shell=True)
        time.sleep(5)
        os.remove(download_file_path)
        return frcontentId


def psltoBeFragment(jobId,frContentId):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"package":"started"}})
    transcodeDb.update_one({"contentId":frContentId}, {"$set":{"Fragment Status":"Started"}})
    results = transcodeDb.find({"contentId":frContentId})
    for result in results:
        template = result['template']
        fileName = result['revFileName']
    results = templateDb.find({"templatename": template})
    for result in results:
        profiles = result['profiles']
    for i in profiles:
        status=mp4fragment(jobId,i,frContentId)
        if status == "Retranscode":
            break
    transcodeDb.update_one({"contentId":frContentId}, {"$set":{"Fragment Status":"Complete"}})
    return frContentId


#Function :- Packaging
def psltoBePackage(jobId,pkgContentId,hlsURL,dashURL):
    packagingStart = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":pkgContentId
                            },
                            {
                                "$set":{
                                    "Packaging Status":"Started",
                                    "Packaging Start":packagingStart
                                    }
                            })
    results = transcodeDb.find({"contentId":pkgContentId})
    for result in results:
        outputBasePath = result['outputBasePath']
        fileName = result['revFileName']
    fileToBePackage = " "
    packagePath = os.path.join(localPath,"package")
    newFiles = os.listdir(packagePath)
    for each in newFiles:
        fileToBePackage = fileToBePackage + " " + each
    os.chdir(packagePath)
    command = f"/bento4/bin/mp4dash --force --hls --use-segment-timeline {fileToBePackage}"
    tempFile = os.path.join(psls3Bucket, "temp", "out.txt")
    with open(tempFile, 'w') as f:
        results = subprocess.Popen(command,stdout = f,stderr = f,shell=True)
    results = transcodeDb.find({"contentId":pkgContentId})
    for result in results:
        originalFileDuration = result["originalFileDuration"]
    if float(originalFileDuration) <= 1800 :
        time.sleep(30)
    elif float(originalFileDuration) > 1800 and float(originalFileDuration) <= 3600:
        time.sleep(60)
    elif float(originalFileDuration) > 3600 and float(originalFileDuration) <= 7200:
        time.sleep(120)
    elif float(originalFileDuration) > 7200 and float(originalFileDuration) <= 9000:
        time.sleep(180)
    else:
        time.sleep(240)
    file1 = os.path.join(packagePath, "output")
    time_counter = 0
    while not os.path.exists(os.path.join(packagePath, "output","audio")):
         time.sleep(1)
         time_counter += 1
         if time_counter > 50:break
    time_counter = 0
    while not os.path.exists(os.path.join(packagePath, "output","video")):
         time.sleep(1)
         time_counter += 1
         if time_counter > 50:break
    for f in glob.glob("*.mp4"):
         os.remove(f)
    os.chdir(outputBasePath)
    if os.path.exists(os.path.join(outputBasePath, "streaming")):
        shutil.rmtree(os.path.join(outputBasePath, "streaming"))
    os.mkdir("streaming")
    streamingPath = os.path.join(outputBasePath, "streaming")
    os.chdir(streamingPath)
    os.mkdir("video")
    streamingPathVideo = os.path.join(streamingPath, "video")
    os.chdir(file1)
    shutil.move("audio",streamingPath)
    time.sleep(5)
    shutil.move("master.m3u8",streamingPath)
    shutil.move("stream.mpd",streamingPath)
    videoDir = os.path.join(file1, "video")
    os.chdir(videoDir)
    #Going inside video folder
    list = os.listdir(videoDir)
    for dir in list:
       #make avc directory in s3-output
       os.chdir(streamingPathVideo)
       os.mkdir(dir)
       streamingPathFormatDir = os.path.join(streamingPathVideo, dir)
       #########
       insideDir = os.path.join(videoDir,dir)
       os.chdir(insideDir)
       #Going inside avc folder
       list1 = os.listdir(insideDir)
       for profile in list1:
           #Create profile directory in S3-output
           os.chdir(streamingPathFormatDir)
           os.mkdir(profile)
           streamingPathInsideProfile = os.path.join(streamingPathFormatDir,profile)
           #######
           #Going inside profile folder
           insideProfile = os.path.join(insideDir,profile)
           os.chdir(insideProfile)
           list2 = os.listdir(insideProfile)
           for chunk in list2:
               #S3 path for multipart upload
               streamingPathVideoFilefinal =  os.path.join(streamingPathInsideProfile,chunk)
               outputBasePathMultipart = streamingPathVideoFilefinal.removeprefix('/media/')
               local_file_path = os.path.join(insideProfile,chunk)
               object_key = outputBasePathMultipart
               upload_with_chunksize_and_meta(local_file_path, bucket_name,object_key, file_size_mb)
    os.chdir(localPath)
    # for f in glob.glob("*.mp4"):
    #      os.remove(f)
    # shutil.rmtree(file1)
    fragmentPath = os.path.join(localPath,"fragmented")
    shutil.rmtree(fragmentPath)
    shutil.rmtree(packagePath)
    now = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":pkgContentId
                            },
                            {
                                "$set":{
                                    "Packaging End":now,
                                    "Packaging Status":"Done",
                                    "Completion Time":now,
                                    "Transcoding Status":"Done"
                                }
                            })
    frontEndDb.update_one(
                            {
                            "jobId":jobId
                            },
                            {
                                "$set":{
                                    "package":"completed",
                                    "status":"completed",
                                    "hlsURL" : hlsURL,
                                    "dashURL" : dashURL
                                }
                            })
    return pkgContentId

##Program Start from here
flag = "No"
myContentId = "Null"
results = transcodeDb.find({"Packaging Require": "Yes"})
for result in results:
    myContentId = result["contentId"]
    jobId = result["jobId"]
    hlsURL = result["hlsURL"]
    dashURL = result["dashURL"]
    retryCount = result["retryCount"]
    flag = "Yes"

if flag == "Yes":
    transcodeDb.update_one({"contentId":myContentId}, {"$set":{"Packaging Require":"In Process"}})
    cleanPod()
    try:
        pslmp4Fragment = psltoBeFragment(jobId,myContentId)
        pslPackaging = psltoBePackage(jobId,pslmp4Fragment,hlsURL,dashURL)
    except:
        if retryCount < 3:
            retryCount = retryCount + 1
            transcodeDb.update_one({
                                    "contentId":pslmp4Fragment
                                    },
                                    {
                                        "$set":{
                                            "Joining Status": "Not Initiated",
                                            "Packaging Require":"Re-packaging",
                                            "Remarks":"Unknown error during package ,sent to retranscode",
                                            "Error Stage":"Packaging",
                                            "retryCount":retryCount
                                        }
                                    })
            transcodeDb.update_one({
                                "contentId":pslmp4Fragment
                                    },
                                    {
                                        "$unset":{
                                            "Packaging Require": ""
                                        }
                                    })
            frontEndDb.update_one(
                                    {
                                        "jobId":jobId
                                    },
                                    {
                                        "$set":{
                                            "retryCount":retryCount,
                                            "package":"not started",
                                            "Error Stage":"Packaging",
                                            "retryCount":retryCount
                                        }
                                    })
        else:
            frontEndDb.update_one(
                                    {
                                        "jobId":jobId
                                    },
                                    {
                                        "$set":{
                                            "status":"Failure , Retry Count 3 Exceeded",
                                            "Failed Content Id": pslmp4Fragment,
                                            "failedInStage":"Packaging",
                                            "Error Stage":"Packaging"

                                        }
                                    })
            transcodeDb.update_one(
                                    {
                                        "contentId":pslmp4Fragment
                                    },
                                    {
                                        "$set":{
                                            "Remarks":"Failure , Retry Count 3 Exceeded",
                                            "Error Stage":"Packaging"
                                        }
                                    })
    else:
        #Time Calculation
        results = transcodeDb.find({"contentId": myContentId})
        for result in results:
            d1 = result["PreQC Start"]
            d2 = result["PreQC End"]
            d3 = result["Splitting Start"]
            d4 = result["Splitting End"]
            d5 = result["Compression Start"]
            d6 = result["Compression End"]
            d7 = result["Joining Start"]
            d8 = result["Joining End"]
            d9 = result["transcodingStartTime"]
            d10 = result["Completion Time"]
            d11 = result["Packaging Start"]
            d12 = result["Packaging End"]
            splitPath = result["splitPath"]
            jobId = result["jobId"]
            outputSplitPath = result["outputSplitPath"]
            # d15 = result["copyToIntermediateStartTime"]
            # d16 = result["copyToIntermediateEndTime"]

        # results = transcodeDb.find({"jobId": jobId})
        # for result in results:
        #     d13 = result["copyToTranscoderStartTime"]
        #     d14 = result["copyToTranscoderEndTime"]

        if os.path.exists(str(splitPath)):
            shutil.rmtree(str(splitPath))
        #Copy To Transcoder
        #    x1 = str(d14 - d13)
        #Copy to Intermediate
        #    x2 = str(d16 - d15)
        #Time to analyze
        x3 = str(d2 - d1)
        #Time to Split
        x4 = str(d4 - d3)
        #Time to Compress
        x5 = str(d6 - d5)
        #Time to Join
        x6 = str(d8 - d7)
        #Time to Package
        x7 = str(d12 - d11)
        #Total time taken
        x8 = str((d2 - d1) + (d4 - d3) + (d6 - d5) + (d8 - d7) + (d12 - d11))
        transcodeDb.update_one(
                                {
                                    "contentId":myContentId
                                },
                                {
                                    "$set":{
                                        # "Copy To Transcoder":x1,
                                        # "Copy to Intermediate":x2,
                                        "Time to analyze":x3,
                                        "Time to Split":x4,
                                        "Time to Compress":x5,
                                        "Time to Join":x6,
                                        "Time to Package":x7,
                                        "Total Time Taken (HH:MM:SS:FR)": x8
                                        }
                                })
        shutil.rmtree(outputSplitPath)
