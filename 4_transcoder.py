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
localPath = "/video" #local folder in the container
localPathDel = "/video" #local folder in the container
baseLocalPath ="/"
gopFactor = 2 # Group of pictures
myPriority = "Urgent"  # Specific to priority que
hlsURL =  "https://d2unj9jgf6tt0e.cloudfront.net/" #Sample HLS url
dashURL = "https://d2unj9jgf6tt0e.cloudfront.net/" # Sample Dash url


vcodec = {
    "libx264H.264/AVC/MPEG-4AVC/MPEG-4part10(codech264)" : "libx264",
    "libaomAV1(codecav1)" : "libaom-av1",
    "librav1eAV1(codecav1)" : "librav1e",
    "FLV/SorensonSpark/SorensonH.263(FlashVideo)(codecflv1)" : "flv",
    "H.261" : "h261",
    "H.263/H.263-1996" : "h263",
    "H.263+/H.263-1998/H.263version2" : "h263p",
    "libx264H.264/AVC/MPEG-4AVC/MPEG-4part10RGB(codech264)" : "libx264rgb",
    "VideoToolboxH.264Encoder(codech264)" : "h264_videotoolbox",
    "libx265H.265/HEVC(codechevc)" : "libx265",
    "VideoToolboxH.265Encoder(codechevc)" : "hevc_videotoolbox",
    "MPEG-1video" : "mpeg1video",
    "MPEG-2video" : "libx264",
    "MPEG-4part2" : "mpeg4",
    "libxvidcoreMPEG-4part2(codecmpeg4)" : "libxvid",
    "libvpxVP8(codecvp8)" : "libvpx",
    "libvpxVP9(codecvp9)" : "libvpx-vp9",
    "WindowsMediaVideo7" : "wmv1",
    "WindowsMediaVideo8" : "wmv2"
    }

acodec = {
    "AAC (Advanced Audio Coding)" : "aac",
    "ATSC A/52A (AC-3)" : "ac3",
    "ATSC A/52A (AC-3) (codec ac3)" : "ac3_fixed",
    "ADPCM Microsoft" : "adpcm_ms",
    "ADPCM Shockwave Flash" : "adpcm_swf",
    "ADPCM Yamaha" : "adpcm_yamaha",
    "ALAC (Apple Lossless Audio Codec)" : "alac",
    "alac (AudioToolbox) (codec alac)" : "alac_at",
    "OpenCORE AMR-NB (Adaptive Multi-Rate Narrow-Band) (codec amr_nb)" : "libopencore_amrnb",
    "ATSC A/52 E-AC-3" : "eac3",
    "FLAC (Free Lossless Audio Codec)" : "flac",
    "MP2 (MPEG audio layer 2)" : "mp2",
    "MP2 fixed point (MPEG audio layer 2) (codec mp2)" : "mp2fixed",
    "PCM A-law / G.711 A-law" : "pcm_alaw",
    "pcm_alaw (AudioToolbox) (codec pcm_alaw)" : "pcm_alaw_at",
    "PCM signed 16|20|24-bit big-endian for Blu-ray media" : "pcm_bluray",
    "PCM signed 16|20|24-bit big-endian for DVD media" : "pcm_dvd",
    "PCM mu-law / G.711 mu-law" : "pcm_mulaw",
    "pcm_mulaw (AudioToolbox) (codec pcm_mulaw)" : "pcm_mulaw_at",
    "PCM D-Cinema audio signed 24-bit" : "pcm_s24daud",
    "RealAudio 1.0 (14.4K) (codec ra_144)" : "real_144",
    "TrueHD" : "truehd",
    "TTA (True Audio)" : "tta",
    "Vorbis" : "vorbis",
    "libvorbis (codec vorbis)" : "libvorbis",
    "WavPack" : "wavpack",
    "Windows Media Audio 1" : "wmav1",
    "Windows Media Audio 2" : "wmav2"
    }

#Function :- Kubernetes Job for Compress
def compress_job(command,profileName,cpu,memory,trContentId):
    config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    container = client.V1Container(
        name="compress",
        image='localhost:5000/compress',
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
                                     mymodel.V1LabelSelectorRequirement(key="app", operator="In", values=["core1"])
                            ]
                        ),
                        topology_key="kubernetes.io/hostname"
                    )
                ]
            )
        )
    volume = client.V1Volume(
      name='bornincloud-media',
      host_path=client.V1HostPathVolumeSource(path='/mnt/s3bucket')
      )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={'name': 'compress'}),
        spec=client.V1PodSpec(restart_policy='OnFailure', containers=[container],volumes=[volume], affinity=compress_affinity))
    spec = client.V1JobSpec(template=template)
    data = str(uuid.uuid4())[:5]
    now = int(time.time())
    job_name = "compress-" + str(profileName) + "-" + str(now) + data
    job = client.V1Job(
        api_version='batch/v1',
        kind='Job',
        metadata=client.V1ObjectMeta(name=job_name),
        spec=spec)
    api_response = batch_v1.create_namespaced_job(
        body=job,
        namespace='default')

def compressWait_job(command,profileName,cpu,memory,trContentId):
    config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    container = client.V1Container(
        name="compress",
        image='localhost:5000/compress',
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
                                     mymodel.V1LabelSelectorRequirement(key="app", operator="In", values=["core1"])
                            ]
                        ),
                        topology_key="kubernetes.io/hostname"
                    )
                ]
            )
        )
    volume = client.V1Volume(
      name='bornincloud-media',
      host_path=client.V1HostPathVolumeSource(path='/mnt/s3bucket')
      )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={'name': 'compress'}),
        spec=client.V1PodSpec(restart_policy='OnFailure', containers=[container],volumes=[volume],affinity=compress_affinity))
    spec = client.V1JobSpec(template=template)
    data = str(uuid.uuid4())[:5]
    now = int(time.time())
    job_name = "compress-" + str(profileName) + "-" + str(now) + data
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
def transcode(jobId,profileName,fileName,trContentId,inputPath):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"compress":"in progress"}})
    profileName = profileName
    inputPath = inputPath
    f_str = fileName
    fresult = f_str.split(".",1)[0]
    outputDirectoryName = fresult
    a = str(trContentId)
    hlsURL =  "https://d2unj9jgf6tt0e.cloudfront.net/" + str(a) + "/streaming/master.m3u8"
    dashURL = "https://d2unj9jgf6tt0e.cloudfront.net/" + str(a) + "/streaming/stream.mpd"
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
    os.chdir(psls3Bucket)
    trprofile = bitrateLadder.find({"profileName":profileName})
    for option in trprofile:
        profileName = option["profileName"]
        Bitrate = option["bitrate"]
        Maxrate = option["maxrate"]
        Bufsize = option["bufsize"]
        audioBitrate = option["audiobitrate"]
        Profile = option["profile"]
        Level = float(option["level"])
        Preset = option["preset"]
        Resolution = option["resolution"]
        videoCodec = vcodec.get(str(option["videocodec"]))
        audioSampleRate = int(option["audiosamplerate"])
        audioChannel = int(option["audiochannel"])
        audioCodec = acodec.get(str(option["audiocodec"]))
        x = Resolution.split("*")
        videoWidth = int(x[0])
        videoHeight = int(x[1])
        videoFramerate = float(option["videoframerate"])
        GOP = int(option["gop"])
    if videoFramerate == "Same as original" or videoFramerate == "same as original" or videoFramerate == "Same As Original":
        calcFrameRate = originalFrameRate
    else :
        calcFrameRate = videoFramerate
    GOP = int(GOP * calcFrameRate)
    transcodeDb.update_one(
                            {
                                "contentId":trContentId
                            },
                            {
                                "$set":{
                                "tr-profileName":profileName,
                                "tr-Bitrate":Bitrate,
                                "tr-Maxrate":Maxrate,
                                "tr-Bufsize":Bufsize,
                                "tr-audioBitrate":audioBitrate,
                                "tr-Profile":Profile,
                                "tr-Level":Level,
                                "tr-Preset":Preset,
                                "tr-Resolution":Resolution,
                                "tr-videoCodec":videoCodec,
                                "tr-audioSampleRate":audioSampleRate,
                                "tr-audioChannel":audioChannel,
                                "tr-audioCodec":audioCodec,
                                "tr-videoWidth":videoWidth,
                                "tr-videoHeight":videoHeight,
                                "tr-videoFramerate":videoFramerate,
                                "tr-GOP":GOP
                            }
                        })
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
    transcodeDb.update_one(
                            {
                                "contentId":trContentId
                            },
                            {
                                "$set":{
                                "tr-profileName":profileName,
                                "tr-Bitrate":Bitrate,
                                "tr-Maxrate":Maxrate,
                                "tr-Bufsize":Bufsize,
                                "tr-audioBitrate":audioBitrate,
                                "tr-Profile":Profile,
                                "tr-Level":Level,
                                "tr-Preset":Preset,
                                "tr-Resolution":Resolution,
                                "tr-videoCodec":videoCodec,
                                "tr-audioSampleRate":audioSampleRate,
                                "tr-audioChannel":audioChannel,
                                "tr-audioCodec":audioCodec,
                                "tr-videoWidth":videoWidth,
                                "tr-videoHeight":videoHeight,
                                "tr-videoFramerate":videoFramerate,
                                "tr-GOP":GOP,
                                "tr-CPU":cpu,
                                "tr-Memory":memory
                            }
                        })
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
                compressWait_job(command,profileName,cpu,memory,trContentId)
            else:
                compress_job(command,profileName,cpu,memory,trContentId)
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
        template = result['template']
    results = templateDb.find({"templatename": template})
    for result in results:
        profiles = result['profiles']
    transcodeDb.update_one(
                            {
                                "contentId":pslTcontentId
                            },
                            {
                                "$set":{
                                    "Profiles": profiles
                                }
                            })
    for i in profiles:
        profileName = i
        try:
            transcode(jobId,profileName,fileName,pslTcontentId,inputPath)
        except:
            transcode(jobId,profileName,fileName,pslTcontentId,inputPath)
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
