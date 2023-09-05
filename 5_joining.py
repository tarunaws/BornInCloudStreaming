#Import Modules
import os,shutil,datetime,subprocess,json,pymongo,time,yaml,math,uuid
from kubernetes import client, config, watch
from pymongo import MongoClient
import sys
import threading

import boto3
from boto3.s3.transfer import TransferConfig

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

bucket_name = "bornincloud-transcoder"
file_size_mb = 1000

#DB Initialization k8s
# db_client = MongoClient("mongodb://svc-db:27017/")
#Centralise DB Initialization
db_client = MongoClient("mongodb://db.bornincloudstreaming.com:27017/")
db = db_client["CoreDB"]
bitrateLadder = db["bitrateLadder"]
transcodeDb = db["transcodeDb"]
frontEndDb = db["frontenddbs"]
k8sDb = db["k8sDb"]

#Core API DB Connction
coreAPI_client = MongoClient("mongodb://primarydb.bornincloudstreaming.com:27017/")
coreAPI = db_client["coreAPIStatus"]
coreAPItranscodeDb = coreAPI["coreAPItranscodeDb"]

# Variable Initialization
s3Bucket = "/media"
distributed = os.path.join(s3Bucket,"intermediate","distributed")
splitPath = os.path.join(s3Bucket,"intermediate","split")
output = os.path.join(s3Bucket, "output")
outputSplit=os.path.join(s3Bucket,"intermediate","splitCompress")
outputComplete = output
rejected = os.path.join(s3Bucket,"intermediate","rejected")
localPath = "/video"

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


#Function :- Join
def joinSplitFile(jobId,profileId,jsContentId,timetowait):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"join":"inprocess"}})
    profileName = myProfile.get(str(profileId))
    results = transcodeDb.find({"contentId":jsContentId})
    for result in results:
        outputSplitPath = result["outputSplitPath"]
        outputSinglePath = result["outputSinglePath"]
        fileName = result['revFileName']
        s = outputSinglePath
    outputSinglePathMultiPart = s.removeprefix('/media/')
    f_str = fileName
    fresult = f_str.split(".",1)[0]
    os.chdir(localPath)
    a = str(fresult) + "_" + profileName
    if os.path.exists(a):
        shutil.rmtree(a)
    os.mkdir(a)
    localContentId = os.path.join(localPath,a)
    os.chdir(localContentId)
    if os.path.exists(profileName):
        shutil.rmtree(profileName)
    os.mkdir(profileName)
    localprofileid = os.path.join(localContentId,profileName)
    os.chdir(localprofileid)
    fileDst = os.path.join(outputSinglePath,profileName)
    pathProfile = os.path.join(outputSplitPath,profileName)
    joinFiles = os.listdir(pathProfile)
    chunkfilename = "chunks.txt"
    with open(chunkfilename , 'w') as fout:
        for file in joinFiles:
            src=os.path.join(pathProfile, file)
            srcMultipart = src.removeprefix('/media/')
            download_file_path = os.path.join(localprofileid,file)
            object_key = srcMultipart
            download_with_default_configuration(bucket_name, object_key,download_file_path, file_size_mb)
#            shutil.move(src,localprofileid)
            fout.write(file + '\n')
    chunks = list()
    with open (chunkfilename) as fin:
        for line in fin:
            chunks.append(line.strip())
    splitchunkname = list()
    for name in chunks:
        x = name.replace(".mp4", "")
        splitchunkname.append(x)
#Convert string to int
    res = [eval(i) for i in splitchunkname]
    res.sort()
    with open("File.txt", 'w') as f:
        for result1 in res:
            name = str(result1) + ".mp4"
            f.write("file '%s'\n" %name)
    joinFile = os.path.join(localprofileid,"File.txt")
    final = a + ".mp4"
    finalFileName = os.path.join(outputSinglePath,profileName,final)
    finalFileNameMultipart = os.path.join(outputSinglePathMultiPart,profileName,final)
    txtfinalFileName = os.path.join(outputSinglePath,profileName,"File.txt")
    command = f"ffmpeg -y -f concat -safe 0 -i {joinFile} -c copy {final}"
    tempFile = os.path.join(s3Bucket, "temp", "out.txt")
    with open(tempFile, 'w') as f:
        results = subprocess.Popen(command,stdout = f,stderr = f,shell=True)
    # waitToJoin = k8sDb.find({"contentId":jsContentId})
    # for value in waitToJoin:
    #     sleepForJoin = value["timeToSplit"]
#Sleep parameter need to tweak.
  #  time.sleep(int(sleepForJoin * 2 ))
    time.sleep(timetowait * 2)
    joinSleepTime = timetowait * 2
    k8sDb.update_one({"contentId":jsContentId}, {"$set":{"joinSleepTime(In Min)":joinSleepTime}})
    local_file_path = os.path.join(localprofileid,final)
    object_key = finalFileNameMultipart
    upload_with_chunksize_and_meta(local_file_path, bucket_name,object_key, file_size_mb)
    os.chdir(localPath)
    shutil.rmtree(localContentId)
    transcodeDb.update_one(
                            {
                                "contentId":jsContentId
                            },
                            {
                                "$set":{
                                    a:"Joining Complete"
                                    }
                            })
    return jsContentId


def psltoBeJoin(jobId,psljContentId,timetowait):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"join":"started"}})
    joinStart = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":psljContentId
                            },
                            {
                                "$set":{
                                    "Joining Status":"Started",
                                    "Joining Start":joinStart
                                    }
                            })
    findSplitDuration = k8sDb.find({"contentId":psljContentId})
    for value in findSplitDuration:
        splitTimeInSec = value["splitTimeInSec"]
    results = transcodeDb.find({"contentId":psljContentId})
    for result in results:
        inputCategory = result['inputType']
    if inputCategory == "inputFor4k":
#two times "joinSplitFile" function is to hold the last profile for some time
        for i in range(1, 7):
            if i == 7:
                status = joinSplitFile(jobId,i,psljContentId,timetowait)
                print(status)
            joinSplitFile(jobId,i,psljContentId,timetowait)
    elif inputCategory == "inputForFullHD":
        for i in range(1, 7):
            if i == 7:
                status = joinSplitFile(jobId,i,psljContentId,timetowait)
                print(status)
            joinSplitFile(jobId,i,psljContentId,timetowait)
    elif inputCategory == "inputForHalfHD":
        for i in range(1, 7):
            if i == 7:
                status = joinSplitFile(jobId,i,psljContentId,timetowait)
                print(status)
            joinSplitFile(jobId,i,psljContentId,timetowait)
    else:
        for i in range(1, 7):
            if i == 7:
                status = joinSplitFile(jobId,i,psljContentId,timetowait)
                print(status)
            joinSplitFile(jobId,i,psljContentId,timetowait)
    now = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":psljContentId
                            },
                            {
                                "$set":{
                                    "Joining End":now,
                                    "Joining Status":"Done",
                                    "Completion Time":now
                                }
                            })
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"join":"completed"}})
    transcodeDb.update_one({"contentId":psljContentId}, {"$set":{"Packaging Require":"Yes"}})
    return psljContentId


##Program Start from here
flag = "No"
myContentId = "Null"
results = transcodeDb.find({'$and': [{"Joining Status": "Not Initiated"}, {"Compression Status": "Done"}]})
for result in results:
    myContentId = result["contentId"]
    jobId = result["jobId"]
    retryCount = result["retryCount"]
    flag = "Yes"
    t1 = result["Splitting Start"]
    t2 = result["Splitting End"]
    hhmmss = str(t2 - t1)
    [hours, minutes, seconds] = [int(float(x)) for x in hhmmss.split(':')]
    x = datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
    timetowait = int(float(x.seconds))


if flag == "Yes" :

    try:
        #contentIdtoBeJoin = toBeJoin(contentIdToBeTranscode)
        pslcontentIdtoBeJoin = psltoBeJoin(jobId,myContentId,timetowait)
    except:
        if retryCount < 3:
            retryCount = retryCount + 1
            transcodeDb.update_one(
                                    {
                                        "contentId":myContentId
                                    },
                                    {
                                        "$set":{
                                            "Remarks":"Unknown error during Joining ,sent to rejoining",
                                            "Joining Status": "Not Initiated",
                                            "Error Stage":"Joining",
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
                                            "Error Stage":"Joining",
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
                                            "Failed Content Id": myContentId,
                                            "Error Stage":"Joining"
                                        }
                                    })
            transcodeDb.update_one(
                                    {
                                        "contentId":myContentId
                                    },
                                    {
                                        "$set":{
                                            "Remarks":"Failure , Retry Count 3 Exceeded",
                                            "Error Stage":"Joining"
                                        }
                                    })
    else:
        transcodeDb.update_one({"contentId":pslcontentIdtoBeJoin}, {"$set":{"Packaging Require":"Yes"}})
