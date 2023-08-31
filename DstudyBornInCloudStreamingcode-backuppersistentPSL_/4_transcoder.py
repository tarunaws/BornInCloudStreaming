#Import Modules
import os,shutil,datetime,subprocess,json,pymongo,time,yaml,math,uuid
from kubernetes import client, config, watch
from kubernetes.client import models as mymodel
from pymongo import MongoClient


#DB Initialization
db_client = MongoClient("mongodb://db.bornincloudstreaming.com:27017/")
db = db_client["CoreDB"]
bitrateLadder = db["bitrateLadder"]
transcodeDb = db["transcodeDb"]
frontEndDb = db["frontenddbs"]
k8sDb = db["k8sDb"]


myPriority = "Urgent"
# Variable Initialization
s3Bucket = "/media"
distributed = os.path.join(s3Bucket,"intermediate","distributed")
splitPath = os.path.join(s3Bucket,"intermediate","split")
output = os.path.join(s3Bucket, "output")
outputSplit=os.path.join(s3Bucket,"intermediate","splitCompress")
outputComplete = os.path.join(s3Bucket, "output")
rejected = os.path.join(s3Bucket,"intermediate","rejected")
localPath = "/video"
hlsURL =  "https://d3rqc5053sq4hy.cloudfront.net/"
dashURL = "https://d3rqc5053sq4hy.cloudfront.net/"

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

#Zee5 Test :
def zee5toBeTranscode(jobId,zee5TcontentId,retryCount):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"compress":"started"}})
    transStart = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":zee5TcontentId
                            },
                            {
                                "$set":{
                                    "Compression Status":"Started",
                                    "Compression Start":transStart
                                }
                            })
    findSplitDuration = k8sDb.find({"contentId":zee5TcontentId})
    for value in findSplitDuration:
        splitTimeInSec = value["splitTimeInSec"]
        numberOfChunks = value["numberOfChunks"]
    results = transcodeDb.find({"contentId": zee5TcontentId})
    for result in results:
        inputCategory = result['inputType']
        fileName = result['revFileName']
        inputPath = result['splitPath']
    if inputCategory == "inputFor4k":
        for i in range(1, 7):
            profileName = myProfile.get(str(i))
            try:
                transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            except:
                transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            else:
                transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            # transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            # if transcodeVerifier == numberOfChunks:
            #     transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            # else:
            #     transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            #     if transcodeVerifier == numberOfChunks:
            #         transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            #     else:
            #         transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            #         if transcodeVerifier == numberOfChunks:
            #             transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            #         else:
            #             break
    elif inputCategory == "inputForFullHD":
        for i in range(1, 7):
            profileName = myProfile.get(str(i))
            try:
                transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            except:
                transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            else:
                transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            # transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            # if transcodeVerifier == numberOfChunks:
            #     transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            # else:
            #     transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            #     if transcodeVerifier == numberOfChunks:
            #         transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            #     else:
            #         transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            #         if transcodeVerifier == numberOfChunks:
            #             transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            #         else:
            #             break
    elif inputCategory == "inputForHalfHD":
        for i in range(1, 7):
            profileName = myProfile.get(str(i))
            try:
                transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            except:
                transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            else:
                transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            # transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            # if transcodeVerifier == numberOfChunks:
            #     transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            # else:
            #     transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            #     if transcodeVerifier == numberOfChunks:
            #         transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            #     else:
            #         transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            #         if transcodeVerifier == numberOfChunks:
            #             transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            #         else:
            #             break
    elif inputCategory == "inputForBelowHalfHD":
        for i in range(1, 7):
            profileName = myProfile.get(str(i))
            try:
                transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            except:
                transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            else:
                transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            # transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            # if transcodeVerifier == numberOfChunks:
            #     transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            # else:
            #     transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            #     if transcodeVerifier == numberOfChunks:
            #         transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            #     else:
            #         transcodeVerifier = transcode(jobId,i,fileName,zee5TcontentId,inputPath,profileName)
            #         if transcodeVerifier == numberOfChunks:
            #             transcodeDb.update_one({"contentId":zee5TcontentId}, {"$set":{profileName:"Compress complete"}})
            #         else:
            #             break
    else:
        print("File Rejected")
    transEnd = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":zee5TcontentId
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
    return zee5TcontentId


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
        zee5contentIdToBeTranscode = zee5toBeTranscode(jobId,myContentId,retryCount)
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
                                "contentId":zee5contentIdToBeTranscode
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
