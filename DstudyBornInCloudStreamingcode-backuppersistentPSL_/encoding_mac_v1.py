import json,pymongo,os,subprocess
from pymongo import MongoClient
basepath="/Users/tarunbhardwaj/myDrive/productDevelopment/videoCMS/Transcoding"
s3Bucket=os.path.join(basepath, "s3Bucket")
#client = MongoClient("mongodb://svc-db:27017/")
client = MongoClient("mongodb://db.bornincloudstreaming.com:27017/")
db = client["CoreDB"]

bitrateLadder = db["bitrateLadder"]


#Define Variable
videoCodec1 = "libx264"
videoCodec2 = "libx265"
frameRateLow = 25
frameRateMedium = 25
frameRateHigh = 25
audioCodec = "aac"
audioChannel = 2
audioSample = 48000
GOP = 2


#Version 1 - AAC

profile1 = {
    'profileId' : 1,
    'Bitrate' : '800k',
    'Maxrate' : '1040k',
    'Bufsize' : '1600k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 2.1,
    'Preset' : 'placebo',
    'Resolution' : "854*480",
    'videoWidth' : 854,
    'videoHeight' : 480,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


profile2 = {
    'profileId' : 2,
    'Bitrate' : '1300k',
    'Maxrate' : '1690k',
    'Bufsize' : '2600k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 2.1,
    'Preset' : 'placebo',
    'Resolution' : "1024*576",
    'videoWidth' : 1024,
    'videoHeight' : 576,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile3 = {
    'profileId' : 3,
    'Bitrate' : '1800k',
    'Maxrate' : '2340k',
    'Bufsize' : '3600k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 3.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile4 = {
    'profileId' : 4,
    'Bitrate' : '2200k',
    'Maxrate' : '2860k',
    'Bufsize' : '4400k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 3.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile5 = {
    'profileId' : 5,
    'Bitrate' : '3200k',
    'Maxrate' : '4160k',
    'Bufsize' : '6400k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 4.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile6 = {
    'profileId' : 6,
    'Bitrate' : '5000k',
    'Maxrate' : '6500k',
    'Bufsize' : '10000k',
    'audioBitrate' : '128k',
    'Profile' : "high",
    'Level' : 4.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


#Version 2 - AAC

profile7 = {
    'profileId' : 7,
    'Bitrate' : '800k',
    'Maxrate' : '960k',
    'Bufsize' : '1600k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 2.1,
    'Preset' : 'placebo',
    'Resolution' : "854*480",
    'videoWidth' : 854,
    'videoHeight' : 480,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


profile8 = {
    'profileId' : 8,
    'Bitrate' : '1300k',
    'Maxrate' : '1560k',
    'Bufsize' : '2600k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 2.1,
    'Preset' : 'placebo',
    'Resolution' : "1024*576",
    'videoWidth' : 1024,
    'videoHeight' : 576,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile9 = {
    'profileId' : 9,
    'Bitrate' : '1800k',
    'Maxrate' : '2160k',
    'Bufsize' : '3600k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 3.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile10 = {
    'profileId' : 10,
    'Bitrate' : '2200k',
    'Maxrate' : '2640k',
    'Bufsize' : '4400k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 3.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile11 = {
    'profileId' : 11,
    'Bitrate' : '3200k',
    'Maxrate' : '3840k',
    'Bufsize' : '6400k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 4.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile12 = {
    'profileId' : 12,
    'Bitrate' : '5000k',
    'Maxrate' : '6000k',
    'Bufsize' : '10000k',
    'audioBitrate' : '128k',
    'Profile' : "high",
    'Level' : 4.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

#Version 3 - AAC

profile13 = {
    'profileId' : 13,
    'Bitrate' : '800k',
    'Maxrate' : '880k',
    'Bufsize' : '1600k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 2.1,
    'Preset' : 'placebo',
    'Resolution' : "854*480",
    'videoWidth' : 854,
    'videoHeight' : 480,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


profile14 = {
    'profileId' : 14,
    'Bitrate' : '1300k',
    'Maxrate' : '1430k',
    'Bufsize' : '2600k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 2.1,
    'Preset' : 'placebo',
    'Resolution' : "1024*576",
    'videoWidth' : 1024,
    'videoHeight' : 576,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile15 = {
    'profileId' : 15,
    'Bitrate' : '1800k',
    'Maxrate' : '1980k',
    'Bufsize' : '3600k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 3.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile16 = {
    'profileId' : 16,
    'Bitrate' : '2200k',
    'Maxrate' : '2420k',
    'Bufsize' : '4400k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 3.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile17 = {
    'profileId' : 17,
    'Bitrate' : '3200k',
    'Maxrate' : '3520k',
    'Bufsize' : '6400k',
    'audioBitrate' : '128k',
    'Profile' : "main",
    'Level' : 4.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile18 = {
    'profileId' : 18,
    'Bitrate' : '5000k',
    'Maxrate' : '5500k',
    'Bufsize' : '10000k',
    'audioBitrate' : '128k',
    'Profile' : "high",
    'Level' : 4.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec1,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


#Version 1 - HEVC
# 360p - 3.1
# 480p - 3.1
# 576p - 4.1
# 720p and more > 5.1

profile19 = {
    'profileId' : 19,
    'Bitrate' : '640k',
    'Maxrate' : '832k',
    'Bufsize' : '1280k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 3.1,
    'Preset' : 'placebo',
    'Resolution' : "854*480",
    'videoWidth' : 854,
    'videoHeight' : 480,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


profile20 = {
    'profileId' : 20,
    'Bitrate' : '1040k',
    'Maxrate' : '1352k',
    'Bufsize' : '2080k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 4.1,
    'Preset' : 'placebo',
    'Resolution' : "1024*576",
    'videoWidth' : 1024,
    'videoHeight' : 576,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile21 = {
    'profileId' : 21,
    'Bitrate' : '1440k',
    'Maxrate' : '1872k',
    'Bufsize' : '2880k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile22 = {
    'profileId' : 22,
    'Bitrate' : '1760k',
    'Maxrate' : '2288k',
    'Bufsize' : '3520k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile23 = {
    'profileId' : 23,
    'Bitrate' : '2560k',
    'Maxrate' : '3328k',
    'Bufsize' : '5120k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile24 = {
    'profileId' : 24,
    'Bitrate' : '4000k',
    'Maxrate' : '5200k',
    'Bufsize' : '8000k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


profile25 = {
    'profileId' : 25,
    'Bitrate' : '6400k',
    'Maxrate' : '8320k',
    'Bufsize' : '12800k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "2560*1440",
    'videoWidth' : 2560,
    'videoHeight' : 1440,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile26 = {
    'profileId' : 26,
    'Bitrate' : '8500k',
    'Maxrate' : '11050k',
    'Bufsize' : '17000k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "3840*2160",
    'videoWidth' : 3840,
    'videoHeight' : 2160,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile27 = {
    'profileId' : 27,
    'Bitrate' : '11000k',
    'Maxrate' : '14300k',
    'Bufsize' : '22000k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "3840*2160",
    'videoWidth' : 3840,
    'videoHeight' : 2160,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

#Version 2 - HEVC

profile28 = {
    'profileId' : 28,
    'Bitrate' : '640k',
    'Maxrate' : '768k',
    'Bufsize' : '1280k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 3.1,
    'Preset' : 'placebo',
    'Resolution' : "854*480",
    'videoWidth' : 854,
    'videoHeight' : 480,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


profile29 = {
    'profileId' : 29,
    'Bitrate' : '1040k',
    'Maxrate' : '1248k',
    'Bufsize' : '2080k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 4.1,
    'Preset' : 'placebo',
    'Resolution' : "1024*576",
    'videoWidth' : 1024,
    'videoHeight' : 576,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile30 = {
    'profileId' : 30,
    'Bitrate' : '1440k',
    'Maxrate' : '1728k',
    'Bufsize' : '2880k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile31 = {
    'profileId' : 31,
    'Bitrate' : '1760k',
    'Maxrate' : '2112k',
    'Bufsize' : '3520k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile32 = {
    'profileId' : 32,
    'Bitrate' : '2560k',
    'Maxrate' : '3072k',
    'Bufsize' : '5120k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile33 = {
    'profileId' : 33,
    'Bitrate' : '4000k',
    'Maxrate' : '4800k',
    'Bufsize' : '8000k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


profile34 = {
    'profileId' : 34,
    'Bitrate' : '6400k',
    'Maxrate' : '7680k',
    'Bufsize' : '12800k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "2560*1440",
    'videoWidth' : 2560,
    'videoHeight' : 1440,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile35 = {
    'profileId' : 35,
    'Bitrate' : '8500k',
    'Maxrate' : '10200k',
    'Bufsize' : '17000k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "3840*2160",
    'videoWidth' : 3840,
    'videoHeight' : 2160,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile36 = {
    'profileId' : 36,
    'Bitrate' : '11000k',
    'Maxrate' : '13200k',
    'Bufsize' : '22000k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "3840*2160",
    'videoWidth' : 3840,
    'videoHeight' : 2160,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

#Version 3 - HEVC

profile37 = {
    'profileId' : 37,
    'Bitrate' : '640k',
    'Maxrate' : '704k',
    'Bufsize' : '1280k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 3.1,
    'Preset' : 'placebo',
    'Resolution' : "854*480",
    'videoWidth' : 854,
    'videoHeight' : 480,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


profile38 = {
    'profileId' : 38,
    'Bitrate' : '1040k',
    'Maxrate' : '1144k',
    'Bufsize' : '2080k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 4.1,
    'Preset' : 'placebo',
    'Resolution' : "1024*576",
    'videoWidth' : 1024,
    'videoHeight' : 576,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile39 = {
    'profileId' : 39,
    'Bitrate' : '1440k',
    'Maxrate' : '1584k',
    'Bufsize' : '2880k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile40 = {
    'profileId' : 40,
    'Bitrate' : '1760k',
    'Maxrate' : '1936k',
    'Bufsize' : '3520k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1280*720",
    'videoWidth' : 1280,
    'videoHeight' : 720,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile41 = {
    'profileId' : 41,
    'Bitrate' : '2560k',
    'Maxrate' : '2816k',
    'Bufsize' : '5120k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile42 = {
    'profileId' : 42,
    'Bitrate' : '4000k',
    'Maxrate' : '4400k',
    'Bufsize' : '8000k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "1920*1080",
    'videoWidth' : 1920,
    'videoHeight' : 1080,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


profile43 = {
    'profileId' : 43,
    'Bitrate' : '6400k',
    'Maxrate' : '7040k',
    'Bufsize' : '12800k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "2560*1440",
    'videoWidth' : 2560,
    'videoHeight' : 1440,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile44 = {
    'profileId' : 44,
    'Bitrate' : '8500k',
    'Maxrate' : '9350k',
    'Bufsize' : '17000k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "3840*2160",
    'videoWidth' : 3840,
    'videoHeight' : 2160,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }

profile45 = {
    'profileId' : 45,
    'Bitrate' : '11000k',
    'Maxrate' : '12100k',
    'Bufsize' : '22000k',
    'audioBitrate' : '128k',
    'Profile' : "main10",
    'Level' : 5.1,
    'Preset' : 'placebo',
    'Resolution' : "3840*2160",
    'videoWidth' : 3840,
    'videoHeight' : 2160,
    'videoFramerate' :frameRateMedium,
    'videoCodec'     : videoCodec2,
    'GOP' : GOP,
    'audioSampleRate' : audioSample,
    'audioChannel' : audioChannel,
    'audioCodec' : audioCodec
    }


bitrateLadder.insert_many([
                            profile1, 
                            profile2, 
                            profile3, 
                            profile4, 
                            profile5, 
                            profile6, 
                            profile7,
                            profile8, 
                            profile9, 
                            profile10, 
                            profile11, 
                            profile12, 
                            profile13, 
                            profile14,
                            profile15, 
                            profile16, 
                            profile17, 
                            profile18, 
                            profile19, 
                            profile20, 
                            profile21,
                            profile22, 
                            profile23, 
                            profile24, 
                            profile25, 
                            profile26, 
                            profile27, 
                            profile28,
                            profile29, 
                            profile30, 
                            profile31, 
                            profile32, 
                            profile33, 
                            profile34, 
                            profile35,
                            profile36, 
                            profile37, 
                            profile38, 
                            profile39, 
                            profile40,
                            profile41, 
                            profile42, 
                            profile43, 
                            profile44, 
                            profile45
                        ])

