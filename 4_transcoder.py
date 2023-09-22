"""
Below program will launch parallel k8s job for transcoding multiple
bitrate profiles.
Before starting compression, kindly create AMI of k8s worker with belwo two requirements.
a) Create local docker repository and store ffmpeg container inside that
as "localhost:5000/transcodingapi:compress_v0"
b) Install s3fs and mount folder name "/mnt/transcodingapi" with AWS s3 bucket.
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
from kubernetes.client import models as mymodel #kubernetes job specific modules
from boto3.s3.transfer import TransferConfig #S3 upload specific module


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
localPath = "/video" #local folder in the container
localPathDel = "/video" #local folder in the container
baseLocalPath ="/"
gopFactor = 2 # Group of pictures
myPriority = "Urgent"  # Specific to priority que
hlsURL =  "https://d3rqc5053sq4hy.cloudfront.net/" #Sample HLS url
dashURL = "https://d3rqc5053sq4hy.cloudfront.net/" # Sample Dash url

# Profile id to profile name mapping
myProfile = {
    "1" : "480p_v1",
    "2" : "576p_v1",
    "3" : "720pLow_v1",
    "4" : "720pHigh_v1",
    "5" : "1080pLow_v1",
    "6" : "1080pHigh_v1",
    "7" : "480p_v2",
    "8" : "576p_v2",
    "9" : "720pLow_v2",
    "10" : "720pHigh_v2",
    "11" : "1080pLow_v2",
    "12" : "1080pHigh_v2",
    "13" : "480p_v3",
    "14" : "576p_v3",
    "15" : "720pLow_v3",
    "16" : "720pHigh_v3",
    "17" : "1080pLow_v3",
    "18" : "1080pHigh_v3",
    "19" : "480p_hevc_v1",
    "20" : "576p_hevc_v1",
    "21" : "720pLow_hevc_v1",
    "22" : "720pHigh_hevc_v1",
    "23" : "1080pLow_hevc_v1",
    "24" : "1080pHigh_hevc_v1",
    "25" : "2k_hevc_v1",
    "26" : "4k_low_v1",
    "27" : "4k_high_v1",
    "28" : "480p_hevc_v2",
    "29" : "576p_hevc_v2",
    "30" : "720pLow_hevc_v2",
    "31" : "720pHigh_hevc_v2",
    "32" : "1080pLow_hevc_v2",
    "33" : "1080pHigh_hevc_v2",
    "34" : "2k_hevc_v2",
    "35" : "4k_low_v2",
    "36" : "4k_high_v2",
    "37" : "480p_hevc_v3",
    "38" : "576p_hevc_v3",
    "39" : "720pLow_hevc_v3",
    "40" : "720pHigh_hevc_v3",
    "41" : "1080pLow_hevc_v3",
    "42" : "1080pHigh_hevc_v3",
    "43" : "2k_hevc_v3",
    "44" : "4k_low_v3",
    "45" : "4k_high_v3"
    }

#Function :- Kubernetes Job for Compress
def compress_job(command,profileId,cpu,memory):
    config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    container = client.V1Container(
        name="compress",
        image='localhost:5000/transcodingapi:compress_v0',
        command=["/bin/sh", "-c",command],
        volume_mounts=[client.V1VolumeMount(name="bornincloud-media", mount_path='/media')],
        resources = client.V1ResourceRequirements(
          requests = {
             "cpu": cpu ,
             "memory": memory
          },
          limits = {
             "cpu": "12000m",
             "memory": "12000Mi"
          }
        )
        )
    compress_affinity = mymodel.V1Affinity(
             pod_anti_affinity=mymodel.V1PodAntiAffinity(
                 required_during_scheduling_ignored_during_execution=[
                     mymodel.V1PodAffinityTerm(
                             label_selector=mymodel.V1LabelSelector(
                                 match_expressions=[
                                     mymodel.V1LabelSelectorRequirement(key="app", operator="In", values=["core"])
                            ]
                        ),
                        topology_key="kubernetes.io/hostname"
                    )
                ]
            )
        )
    volume = client.V1Volume(
      name='bornincloud-media',
      host_path=client.V1HostPathVolumeSource(path='/mnt/transcodingapi')
      )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={'name': 'compress'}),
        spec=client.V1PodSpec(restart_policy='OnFailure', containers=[container],volumes=[volume], affinity=compress_affinity))
    spec = client.V1JobSpec(template=template)
    data = str(uuid.uuid4())[:5]
    now = int(time.time())
    job_name = "compress-" + str(profileId) + "-" + str(now) + data
    job = client.V1Job(
        api_version='batch/v1',
        kind='Job',
        metadata=client.V1ObjectMeta(name=job_name),
        spec=spec)
    api_response = batch_v1.create_namespaced_job(
        body=job,
        namespace='default')

def compressWait_job(command,profileId,cpu,memory):
    config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    container = client.V1Container(
        name="compress",
        image='localhost:5000/transcodingapi:compress_v0',
        command=["/bin/sh", "-c",command],
        volume_mounts=[client.V1VolumeMount(name="bornincloud-media", mount_path='/media')],
        resources = client.V1ResourceRequirements(
          requests = {
             "cpu": cpu ,
              "memory": memory
          },
          limits = {
             "cpu": "12000m",
             "memory": "12000Mi"
          }
        )
        )
    compress_affinity = mymodel.V1Affinity(
             pod_anti_affinity=mymodel.V1PodAntiAffinity(
                 required_during_scheduling_ignored_during_execution=[
                     mymodel.V1PodAffinityTerm(
                             label_selector=mymodel.V1LabelSelector(
                                 match_expressions=[
                                     mymodel.V1LabelSelectorRequirement(key="app", operator="In", values=["core"])
                            ]
                        ),
                        topology_key="kubernetes.io/hostname"
                    )
                ]
            )
        )
    volume = client.V1Volume(
      name='bornincloud-media',
      host_path=client.V1HostPathVolumeSource(path='/mnt/transcodingapi')
      )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={'name': 'compress'}),
        spec=client.V1PodSpec(restart_policy='OnFailure', containers=[container],volumes=[volume],affinity=compress_affinity))
    spec = client.V1JobSpec(template=template)
    data = str(uuid.uuid4())[:5]
    now = int(time.time())
    job_name = "compress-" + str(profileId) + "-" + str(now) + data
    job = client.V1Job(
        api_version='batch/v1',
        kind='Job',
        metadata=client.V1ObjectMeta(name=job_name),
        spec=spec)
    api_response = batch_v1.create_namespaced_job(
        body=job,
        namespace='default')
    job_completed = False
    while not job_completed:
        api_response1 = batch_v1.read_namespaced_job_status(
            name=job_name,
            namespace="default")
        if api_response1.status.succeeded is not None or api_response1.status.failed is not None:
            job_completed = True
        time.sleep(2)



#Function :-Transcode Function
def transcode(jobId,profileId,fileName,trContentId,inputPath,profileName):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"compress":"inprocess"}})
    profileId = profileId
    inputPath = inputPath
    f_str = fileName
    fresult = f_str.split(".",1)[0]
    outputDirectoryName = fresult
  #  a = outputDirectoryName
    a = str(trContentId)
    hlsURL =  "https://d3rqc5053sq4hy.cloudfront.net/" + str(a) + "/streaming/master.m3u8"
    dashURL = "https://d3rqc5053sq4hy.cloudfront.net/" + str(a) + "/streaming/stream.mpd"
    os.chdir(outputSplit)
    if not os.path.exists(os.path.join(outputSplit,str(a))):
        os.mkdir(str(a))
    os.chdir(outputComplete)
    if not os.path.exists(os.path.join(outputComplete,str(a))):
        os.mkdir(str(a))
    os.chdir(os.path.join(outputComplete,str(a)))
    if not os.path.exists(os.path.join(outputComplete,str(a),"file_mp4")):
        os.mkdir("file_mp4")
    outputDirectory = os.path.join(outputSplit,str(a))
    joinDirectory = os.path.join(outputComplete,str(a),"file_mp4")
    outputBasePath = os.path.join(outputComplete,str(a))
    os.chdir(outputDirectory)
    b = profileName
    if not os.path.exists(os.path.join(outputDirectory,str(b))):
        os.mkdir(str(b))
    os.chdir(joinDirectory)
    if not os.path.exists(os.path.join(joinDirectory,str(b))):
        os.mkdir(str(b))
    pathProfile = os.path.join(outputDirectory,str(b))
    findSplitDuration = k8sDb.find({"contentId":trContentId})
    for value in findSplitDuration:
        splitTimeInSec = value["splitTimeInSec"]
        noOfChunks = value["numberOfChunks"]
        originalFrameRate = value["originalFrameRate"]
    os.chdir(s3Bucket)
    profile = bitrateLadder.find({"profileId":profileId})
    for option in profile:
        profileId = option["profileId"]
        Bitrate = option["Bitrate"]
        Maxrate = option["Maxrate"]
        Bufsize = option["Bufsize"]
        audioBitrate = option["audioBitrate"]
        Profile = option["Profile"]
        Level = option["Level"]
        Preset = option["Preset"]
        Resolution = option["Resolution"]
        videoCodec = option["videoCodec"]
        audioSampleRate = option["audioSampleRate"]
        audioChannel = option["audioChannel"]
        audioCodec = option["audioCodec"]
        videoWidth = option["videoWidth"]
        videoHeight = option["videoHeight"]
        videoFramerate = option["videoFramerate"]
        GOP = option["GOP"]
    if videoFramerate == "Same as original" or videoFramerate == "same as original" or videoFramerate == "Same As Original":
        calcFrameRate = originalFrameRate
    else :
        calcFrameRate = videoFramerate
    GOP = GOP * calcFrameRate
    # if int(videoHeight) < 250:
    #     calcFrameRate = calcFrameRate / 2
    #     GOP = GOP * calcFrameRate * 2
    # GOP = GOP * calcFrameRate
    bitrateNew = Bitrate.replace("k", " ", 1)
    intBitRate = int(bitrateNew)
    if intBitRate <= 500 and videoCodec == "libx264":
        cpu = "2000m"
        memory = "2000Mi"
    elif intBitRate > 500 and intBitRate <= 1800 and videoCodec == "libx264":
        cpu = "4000m"
        memory = "4000Mi"
    else:
        cpu = "6000m"
        memory = "6000Mi"
    if os.listdir(inputPath):
        newFiles = os.listdir(inputPath)
        waitJob = 1
        for each in newFiles:
            m_str = each
            result = m_str.split(".",1)[0]
            finaloutput = os.path.join(pathProfile,result)
            src=os.path.join(inputPath, each)
            # -pix_fmt yuv420p this option to handle content came as  yuv422p
            command=f"ffmpeg -y -i {src} -pix_fmt yuv420p -vcodec {videoCodec} -b:v {Bitrate} -maxrate {Maxrate} -r {calcFrameRate} -profile:v {Profile} -preset:v {Preset} -level {Level} -vf scale={videoWidth}:{videoHeight}:force_original_aspect_ratio=decrease,pad={videoWidth}:{videoHeight}:ow/2-iw/2:oh/2-ih/2 -g {GOP} -keyint_min {GOP} -sc_threshold 0 -ar {audioSampleRate} -ac {audioChannel} -ab {audioBitrate} -acodec {audioCodec} -pass 1 -f mp4 NUL && ffmpeg -y -i {src} -pix_fmt yuv420p -b:v {Bitrate} -maxrate {Maxrate} -bufsize {Bufsize} -vcodec {videoCodec} -r {calcFrameRate} -profile:v {Profile} -preset:v {Preset} -level {Level} -vf scale={videoWidth}:{videoHeight}:force_original_aspect_ratio=decrease,pad={videoWidth}:{videoHeight}:ow/2-iw/2:oh/2-ih/2 -g {GOP} -keyint_min {GOP} -sc_threshold 0 -ar {audioSampleRate} -ac {audioChannel} -ab {audioBitrate} -acodec {audioCodec} -pass 2 {finaloutput}.mp4"
            #last chunk is always small, so wait should be added in second last chunk as well. use compress wait to control no of container to be launch at same time
#           if waitJob == noOfChunks or waitJob == noOfChunks - 1 or waitJob == int(float(noOfChunks/2)):
            if waitJob == noOfChunks:
                compressWait_job(command,profileId,cpu,memory)
            else:
                compress_job(command,profileId,cpu,memory)
                waitJob = waitJob + 1
    transcodeDb.update_one(
                            {
                                "contentId":trContentId
                            },
                            {
                                "$set":{
                                "outputSplitPath":outputDirectory,
                                "outputSinglePath":joinDirectory,
                                "outputBasePath":outputBasePath,
                                "hlsURL" : hlsURL,
                                "dashURL" : dashURL
                            }
                        })
    transcodeVerifier = os.listdir(pathProfile)
    return transcodeVerifier

#psl Test :
def psltoBeTranscode(jobId,pslTcontentId,retryCount):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"compress":"started"}})
    transStart = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":pslTcontentId
                            },
                            {
                                "$set":{
                                    "Compression Status":"Started",
                                    "Compression Start":transStart
                                }
                            })
    findSplitDuration = k8sDb.find({"contentId":pslTcontentId})
    for value in findSplitDuration:
        splitTimeInSec = value["splitTimeInSec"]
        numberOfChunks = value["numberOfChunks"]
    results = transcodeDb.find({"contentId": pslTcontentId})
    for result in results:
        inputCategory = result['inputType']
        fileName = result['revFileName']
        inputPath = result['splitPath']
    if inputCategory == "inputFor4k":
        for i in range(1, 7):
            profileName = myProfile.get(str(i))
            try:
                transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            except:
                transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            else:
                transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            # transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            # if transcodeVerifier == numberOfChunks:
            #     transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            # else:
            #     transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            #     if transcodeVerifier == numberOfChunks:
            #         transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            #     else:
            #         transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            #         if transcodeVerifier == numberOfChunks:
            #             transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            #         else:
            #             break
    elif inputCategory == "inputForFullHD":
        for i in range(1, 7):
            profileName = myProfile.get(str(i))
            try:
                transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            except:
                transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            else:
                transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            # transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            # if transcodeVerifier == numberOfChunks:
            #     transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            # else:
            #     transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            #     if transcodeVerifier == numberOfChunks:
            #         transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            #     else:
            #         transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            #         if transcodeVerifier == numberOfChunks:
            #             transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            #         else:
            #             break
    elif inputCategory == "inputForHalfHD":
        for i in range(1, 7):
            profileName = myProfile.get(str(i))
            try:
                transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            except:
                transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            else:
                transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            # transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            # if transcodeVerifier == numberOfChunks:
            #     transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            # else:
            #     transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            #     if transcodeVerifier == numberOfChunks:
            #         transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            #     else:
            #         transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            #         if transcodeVerifier == numberOfChunks:
            #             transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            #         else:
            #             break
    elif inputCategory == "inputForBelowHalfHD":
        for i in range(1, 7):
            profileName = myProfile.get(str(i))
            try:
                transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            except:
                transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            else:
                transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            # transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            # if transcodeVerifier == numberOfChunks:
            #     transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            # else:
            #     transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            #     if transcodeVerifier == numberOfChunks:
            #         transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            #     else:
            #         transcodeVerifier = transcode(jobId,i,fileName,pslTcontentId,inputPath,profileName)
            #         if transcodeVerifier == numberOfChunks:
            #             transcodeDb.update_one({"contentId":pslTcontentId}, {"$set":{profileName:"Compress complete"}})
            #         else:
            #             break
    else:
        print("File Rejected")
    transEnd = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":pslTcontentId
                            },
                            {
                                "$set":
                                    {
                                    "Compression Status":"Done",
                                    "Compression End":transEnd
                                }
                            })
    time.sleep(1)
#    shutil.rmtree(inputPath)
    return pslTcontentId


##Program Start from here
flag = "No"
myContentId = "Null"
results = transcodeDb.find({'$and': [{"Transcoding Status": "Waiting for resource"}, {"Splitting Status":"Done"}]})
for result in results:
    myContentId = result["contentId"]
    jobId = result["jobId"]
    retryCount = result["retryCount"]
    flag = "Yes"

if flag == "Yes" :
    transcodingStart = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":myContentId
                            },
                            {
                                "$set":{
                                    "Transcoding Status":"In-Process",
                                    "transcodingStartTime":transcodingStart
                                }
                            })
    #contentIdToBeTranscode = toBeTranscode(contentIdToBeSplit)
    try:
        pslcontentIdToBeTranscode = psltoBeTranscode(jobId,myContentId,retryCount)
    except:
        if retryCount < 3:
            retryCount = retryCount + 1
            transcodeDb.update_one(
                                    {
                                        "contentId":myContentId
                                    },
                                    {
                                        "$set":{
                                            "Transcoding Status": "Waiting for resource",
                                            "Remarks":"Unknown error during compression ,sent to retranscode",
                                            "Error Stage":"Compress",
                                            "retryCount":retryCount
                                        }
                                    })
            frontEndDb.update_one(
                                    {
                                        "jobId":jobId
                                    },
                                    {
                                        "$set":{
                                            "compress":"not started",
                                            "retryCount":retryCount,
                                            "Error Stage":"Compress"
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
                                            "Error Stage":"Compress of complete file"

                                        }
                                    })
            transcodeDb.update_one(
                                    {
                                        "contentId":myContentId
                                    },
                                    {
                                        "$set":{
                                            "Remarks":"Failure , Retry Count 3 Exceeded",
                                            "Error Stage":"Compress of complete file"
                                        }
                                    })
    else:
        transcodeDb.update_one({
                                "contentId":pslcontentIdToBeTranscode
                               },
                               {
                                    "$set":{
                                            "Joining Status":"Not Initiated"
                                        }
                               })
        frontEndDb.update_one({
                                "jobId":jobId
                              },
                              {
                                "$set":{
                                    "compress":"completed"
                                    }
                                })
