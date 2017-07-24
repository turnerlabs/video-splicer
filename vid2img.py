from __future__ import division
import botocore
import subprocess as sp
import os
import boto3
import datetime
import sys
import zipfile
import math

FRAMES = int(os.environ.get('FRAMESPERSEC','1'))
TMP_DIR = "/tmp/output/"
TMP_RENAME = TMP_DIR + "renamed/"

if not os.path.exists(TMP_DIR):
    os.makedirs(TMP_DIR)

if not os.path.exists(TMP_RENAME):
    os.makedirs(TMP_RENAME)

def getLength(video):
    val = sp.check_output('ffprobe -i ' + str(video)+ ' -show_entries format=duration -v quiet -of csv="p=0"',stdin=None, stderr=None, shell=True, universal_newlines=False)
    return val


def vid_to_image(srcKey,srcBucket):
    s3 = boto3.resource('s3')
    localFilename = '/tmp/{}'.format(os.path.basename(srcKey))

    # Download video to local
    try:
        s3.Bucket(srcBucket).download_file(srcKey, localFilename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
            sys.exit()
        else:
            raise

    fps = float(os.environ.get("FRAMESPERSEC"))
    millisec = 1/fps
    time = 0
    count = 1
    vidLength = float(getLength(localFilename))
    print vidLength
    imagesNo = vidLength*fps
    imagesNo = math.ceil(imagesNo)
    imagesNo = int(imagesNo)
    print imagesNo
    for i in range(0,imagesNo + 1):
        #adding 1 to nullify offset in rekognizer
        time = time + 1
        imgName = TMP_RENAME + str(time) + "_img_" + str(count) + ".jpg"
        time = time - 1
        cmd = "ffmpeg -i " + str(localFilename) + " -ss " + str(datetime.timedelta(seconds=time)) + " -vframes 1 " + imgName
        print cmd
        sp.call(cmd,shell=True)
        time = time+millisec
        count = count + 1

    tmpKey = srcKey.rsplit(".",1)
    # Removing the video extention
    destKey = tmpKey[0]
    with zipfile.ZipFile('images.zip', 'w') as myzip:
        for imgFile in sorted(os.listdir(TMP_RENAME)):
            if imgFile.find("img") != -1:
                myzip.write(TMP_RENAME+imgFile)

    s3.Bucket(srcBucket).put_object(Body=open("./images.zip", 'rb'), Key=destKey + ".zip")

if __name__ == '__main__':
    srcBucket = str(os.environ.get('BUCKET'))
    #File to be downloaded from s3
    srcKey = str(os.environ.get('FILE'))
    vid_to_image(srcKey,srcBucket)
