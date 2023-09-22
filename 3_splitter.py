"""
Below program will downalod the input file from S3 into local folder inside pod.
Split the file then again transfer to aws s3 folder for compression.
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
from pymongo import MongoClient #Mongo client to interact with mongodb
from kubernetes import client, config, watch #kubernetes job specific modules
from boto3.s3.transfer import TransferConfig #S3 upload specific module

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

#Bucket name in AWS s3
bucket_name = "bornincloud-transcoder"
#Dummy file size , to calculate transfer speed.
file_size_mb = 1000

#DB Initialization
db_client = MongoClient("mongodb://db.bornincloudstreaming.com:27017/") #DB server address
db = db_client["CoreDB"] #Database name CoreDB
transcodeDb = db["transcodeDb"]
"""Database table name transcodeDb.
It is related to transcoding job status.
"""
frontEndDb = db["frontenddbs"]
"""Database table name frontenddbs.
It is related to job submission by fronend UI.
"""
bitrateLadder = db["bitrateLadder"]
"""
Database table which store multiple profiles to be use for
transcoding.
"""
k8sDb = db["k8sDb"]
"""
Database table which store k8s job related metadata.
"""

# Variable Initialization
s3Bucket = "/media" #local folder in the container
distributed = os.path.join(s3Bucket,"intermediate","distributed") #Right hand side is nested folder in S3
splitPath = os.path.join(s3Bucket,"intermediate","split") #Right hand side is nested folder in S3
outputDirectoryMultipart = os.path.join("intermediate","split") #Right hand side is nested folder in S3
splitPathMultipart = "intermediate/split"
output = os.path.join(s3Bucket, "output") #Right hand side is folder in S3
outputSplit=os.path.join(s3Bucket,"intermediate","splitCompress") #Right hand side is nested folder in S3
outputComplete = os.path.join(s3Bucket, "output") #Right hand side is folder in S3
rejected = os.path.join(s3Bucket,"intermediate","rejected") #Right hand side is nested folder in S3
tempFile = os.path.join(s3Bucket, "temp", "out.txt") #Right hand side is a temp file under nested folder in S3
localPathDel = "/video" #local folder in the container
baseLocalPath ="/"
gopFactor = 2 # Group of pictures
myPriority = "Urgent"  # Specific to priority que
jobId = 1 # Initial job id



#Function:- clean my pod
def cleanPod():
    """
    Sometime due to unknown error, pod does nto complete the job,
    but some garbage data remain present in the pod. So to claim
    the space , it is necessary to clean the pod.
    """
    shutil.rmtree(localPathDel)
    os.chdir(baseLocalPath)
    os.mkdir("video")
    return "/video"


#Time taken by ffprobe command to analyse any video file.
ffprobeSleep = 20


#Function :- Split
def split(jobId: "jobId associated with video file",
          splitContentId: "Content id associated with video file",
          bucketName: "S3 Bucket name",
          object_key: "Video file name to be download into pod",
          localPath: "Local folder into pod"):
    """
    This function will download the input video file into local filesystem of
    container and will split that file and upload to S3 for multipart transcoding.
    """
    results = transcodeDb.find({"contentId": splitContentId})
    for result in results:
        each = result['revFileName']
        fileName = result['fileName']
        retryCount = result['retryCount']
    outputDirectoryName = splitContentId
    dirSplit = splitPath
    os.chdir(localPath)
    if os.path.exists(str(outputDirectoryName)):
            shutil.rmtree(str(outputDirectoryName))
    os.mkdir(str(outputDirectoryName))
    localContentId = os.path.join(localPath,str(outputDirectoryName))
    os.chdir(localContentId)
    os.mkdir("split")
    output1 = os.path.join(localContentId,"split")
    outputname = os.path.join(output1,str(outputDirectoryName))
    download_file_path = os.path.join(localContentId,each)
    #Downlaoding input video into pod
    download_with_default_configuration(bucket_name, object_key,download_file_path, file_size_mb)
    time_counter = 0
    while not os.path.exists(download_file_path):
        """
        Till the time file does not downlaod completly, do ffprobeSleep
        for 1 sec.
        """
        time.sleep(1)
        time_counter += 1
        if time_counter > 200:
            """
             If download time increase by 200 sec or more than 3 minute
             then break the program and update in database.
            """
            transcodeDb.update_one({"contentId":splitContentId}, {"$set":{"Remarks":"Not able to download for splitting"}})
            break
    os.chdir(localContentId)
#Extract metadate of input video file.
    preQcStart = datetime.datetime.now()
    transcodeDb.update_one(
                        {
                        "contentId":splitContentId
                        },
                        {
                            "$set":{
                            "PreQC Status":"Started",
                            "PreQC Start":preQcStart
                            }
                        })
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"analyse":"inprocess"}})
    #Below command will show all the available video/audio stream from video file, into json format.
    command = f"ffprobe -v error {each}  -show_streams -show_format -print_format json"
    metadata = {}  #Assigning empty dictionary
    metadata = subprocess.getoutput(command) # Run the command and output the data into dictionary
    time.sleep(ffprobeSleep) # Sleep for sometime , as defined above
    qc = json.loads(metadata) # convert dictionary into json format.
    #Assign value of video metadate into variables.
    durationInSec = qc["format"]["duration"]
    """Identify the duration at which input video file should be split,
     for parallel transcoding. Split the file at multiple of GOP structure"""
    calculateSplitDuration = int(float(durationInSec))
    if calculateSplitDuration <= 300:
        splitTimeInSec = 10 * gopFactor # Example : 20 Sec
    elif calculateSplitDuration > 300 and calculateSplitDuration <= 600:
        splitTimeInSec = 15 * gopFactor # Example : 30 Sec
    elif calculateSplitDuration > 600 and calculateSplitDuration <= 1200:
        splitTimeInSec = 20 * gopFactor # Example : 40 Sec
    elif calculateSplitDuration > 1200 and calculateSplitDuration <= 1800:
        splitTimeInSec = 25 * gopFactor # Example : 50 Sec
    elif calculateSplitDuration > 1800 and calculateSplitDuration <= 2400:
        splitTimeInSec = 30 * gopFactor # Example : 60 Sec
    else:
        splitTimeInSec = 35 * gopFactor # Example : 70 Sec
    durationInMin = str(round(float(durationInSec)/60,2)) + " Minutes"
    sizeInBytes = qc["format"]["size"]
    sizeinGB = str((((float(sizeInBytes)/1024)/1024)/1024)) + " GB"
    #Conidering 1 GB copy time is 20 sec, hypothetical number
    rawCopy = math.ceil(((((float(sizeInBytes)/1024)/1024)/1024)/4)) * 20
    bitrateInBit = qc["format"]["bit_rate"]
    bitrateInMb = math.ceil((float(bitrateInBit)/1024)/1024)
    bitrateInMb_print = str(math.ceil((float(bitrateInBit)/1024)/1024)) + " Mbps"
    noOfContainerSingleProfile = int(math.ceil(float(durationInSec)/splitTimeInSec))
    #Calculate the time taken by ffmpeg command to split the video file.
    timeToSplit = noOfContainerSingleProfile * 0.1
    if timeToSplit < 10:
        timeToSplit = 10
    fr_str = qc["streams"][0]["r_frame_rate"]
    fr_result = fr_str.split("/",1)[0]
    #Below code is to fix the frame rate either at 25 or 50 frame per second.
    if int(fr_result) <= 25:
        actualFramePerSec = 25
    elif int(fr_result) == 50:
        actualFramePerSec = 50
    else:
        actualFramePerSec = 25
    gopForOutput = int(fr_result) * gopFactor  #GOP in terms of frames. Frame rate * GOP factor
#    NoOfFramePerSegment = 3 * gopFactor * int(fr_result) # Size of segment is multiple of GOP size.
#    deliverySegSizeInMilisec = 1000 * float(NoOfFramePerSegment/actualFramePerSec) #Experimental if Packaging cause issue.
    deliverySegSizeInMilisec = 3 * 1000 * gopFactor # Size of segment is multiple of GOP size.
    transcodeDb.update_one(
                            {
                            "contentId":splitContentId
                            },
                            {
                                "$set":{
                                        "format":qc["format"]["format_name"],
                                        "noOfStreams":qc["format"]["nb_streams"],
                                        "fileSize":sizeinGB,
                                        "Duration":durationInMin,
                                        "avgBitrate":bitrateInMb_print,
                                        "bitrateInMb":bitrateInMb,
                                        "videoCodec":qc["streams"][0]["codec_name"],
                                        "videoWidth":qc["streams"][0]["width"],
                                        "videoHeight":qc["streams"][0]["height"],
                                        "videoFramerate":fr_result,
                                        "audioCodec":qc["streams"][1]["codec_name"],
                                        "audioSamplerate":qc["streams"][1]["sample_rate"],
                                        "timeToRawCopy":rawCopy,
                                        "profile":qc["streams"][0]["profile"],
                                        "pixalFormat":qc["streams"][0]["pix_fmt"],
                                        "Chunks Per Profile":noOfContainerSingleProfile,
                                        "deliverySegmentSize":deliverySegSizeInMilisec,
                                        "originalFileDuration" : durationInSec
                                        }
                            })
    k8Metadata = {
        'contentId' : splitContentId,
        'numberOfChunks' : noOfContainerSingleProfile,
        'splitTimeInSec': splitTimeInSec,
        'timeToSplit' : timeToSplit,
        'GOP' : gopForOutput,
        'originalFrameRate' : int(fr_result)
    }
    k8sDb.insert_one(k8Metadata)
    widthInt = int(qc["streams"][0]["width"])
    heightInt = int(qc["streams"][0]["height"])
    avgBitrateValueInt = int(bitrateInMb)
    if avgBitrateValueInt >= 20 and heightInt >= 2160:
        preqcResult = "inputFor4k"
    elif avgBitrateValueInt >= 10 and heightInt >= 1080:
        preqcResult = "inputForFullHD"
    elif avgBitrateValueInt >= 5  and heightInt >= 720:
        preqcResult = "inputForHalfHD"
    elif avgBitrateValueInt >= 2:
        preqcResult = "inputForBelowHalfHD"
    else:
        preqcResult = "Rejected"
        shutil.move(each,rejected)
    timeUpdate = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                            "contentId":splitContentId
                            },
                            {
                            "$set":{
                                "PreQC Status":"Done",
                                "PreQC End":timeUpdate,
                                "inputType":preqcResult,
                                "Splitting Status":"Started",
                                "Splitting Start":timeUpdate
                                }
                            })
    frontEndDb.update_one(
                            {
                                "jobId":jobId
                            },
                            {
                                "$set":{
                                    "analyse":"completed",
                                    "split":"started"
                                    }
                            })
    results = transcodeDb.find({"contentId": splitContentId})
    dirSplit = splitPath
    copyTime = rawCopy
    os.chdir(dirSplit)
    if os.path.exists(str(outputDirectoryName)):
        shutil.rmtree(str(outputDirectoryName))
    os.mkdir(str(outputDirectoryName))
    outputDirectory = os.path.join(dirSplit,str(outputDirectoryName))
    localfileToBeSplit = os.path.join(localContentId,each)
    os.chdir(localContentId)
    findSplitDuration = k8sDb.find({"contentId":splitContentId})
    for value in findSplitDuration:
        splitTimeInSec = value["splitTimeInSec"]
        timeToSplit = value["timeToSplit"]
        numberOfChunks = value['numberOfChunks']
    #Convert split duration into format applicable for ffmpeg command i.e 60 sec or 1 min is '0:01:00'
    splitDuration = str(datetime.timedelta(seconds = splitTimeInSec ))
    f_str = each
    fresult = f_str.split(".",1)[0]
    extension = f_str.split(".",2)[1]
    command = f"ffmpeg -i {each} -c copy -segment_time {splitDuration} segment -f {outputname}%03d.{extension}"
    """
    -c copy : Copy all codecs as available in original i.e video , audio
    -segment_time : Split video in equal duration.
    -segment : Split in multiple segments
    -f : Output format
    """
    tempFile = os.path.join(s3Bucket, "temp", "out.txt")
    with open(tempFile, 'w') as f:
        """
        run ffmpeg command in background.
        """
        results = subprocess.Popen(command,stdout = f,stderr = f,shell=True)
    counter = 0
    while not counter >= numberOfChunks:
        """
        Veryfy if ffmpeg has generated same number of chunks , that were expected
        to be generate, or else sleep for 1 sec.
        """
         time.sleep(1)
         list = os.listdir(output1)
         counter = 0
         for chunk in list:
            counter = counter + 1
    time.sleep(2)
    for chunk in list:
        """
        Multipart upload input chunks into S3 for further compression.
        """
        src=os.path.join(output1, chunk)
        local_file_path = src
        object_key = os.path.join(outputDirectoryMultipart,str(outputDirectoryName),chunk)
        #Multipart upload function.
        upload_with_chunksize_and_meta(local_file_path, bucket_name,object_key, file_size_mb)
    time.sleep(2)
    shutil.rmtree(localContentId)
    splitEnd = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":splitContentId
                            },
                            {
                                "$set":{
                                    "Splitting End":splitEnd,
                                    "Splitting Status":"Done",
                                    "splitPath":outputDirectory
                                    }
                            })
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"split":"completed"}})
    return splitContentId


##Program Start from here, identify the file with splitting status "Not initiated"
flag = "No"
myContentId = "Null"
myInputFile = "Null"
revFileName = "Null"
results = transcodeDb.find({'$and': [{"Transcoding Status": "Waiting for resource"}, {"Splitting Status": "Not Initiated"}]})
for result in results:
    myContentId = result["contentId"]
    revFileName = result["revFileName"]
    jobId = result["jobId"]
    object_key = result["object_key"]
    bucketName = result["bucketName"]
    retryCount = result["retryCount"]
    flag = "Yes"

# Splitting of file will be retry for 3 times, if any error occur.
if flag == "Yes":
    transcodeDb.update_one({"contentId":myContentId}, {"$set":{"Splitting Status":"In-Process"}})
    try:
        localPath = cleanPod()
        time.sleep(2)
        contentIdToBeSplit = split(jobId,myContentId,bucketName,object_key,localPath)
    except:
        if retryCount < 3:
            retryCount = retryCount + 1
            transcodeDb.update_one(
                                    {
                                        "contentId":myContentId
                                    },
                                    {
                                        "$set":{
                                            "Remarks":"Unknown error during splitting ,sent to retranscode",
                                            "Splitting Status": "Not Initiated",
                                            "Error Stage":"Split",
                                            "retryCount":retryCount
                                        }
                                    })

            frontEndDb.update_one(
                                    {
                                        "jobId":jobId
                                    },
                                    {
                                        "$set":{
                                            "retryCount":retryCount,
                                            "Error Stage":"Split"
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
                                            "Failed Content Id": myContentId,
                                            "Error Stage":"Split"
                                        }
                                    })

            transcodeDb.update_one(
                                    {
                                        "contentId":myContentId
                                    },
                                    {
                                        "$set":{
                                            "Remarks":"Failure , Retry Count 3 Exceeded",
                                            "Error Stage":"Split"
                                            }
                                    })
    else:
        print("Nothing went wrong in ", myContentId)
