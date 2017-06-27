from __future__ import division
import botocore
import subprocess as sp
import os
import boto3
import datetime
import sys


#No. of frames per second
FRAMES = int(os.environ.get('FRAMESPERSEC','1'))
TMP_DIR = "/tmp/output/"

if not os.path.exists(TMP_DIR):
    os.makedirs(TMP_DIR)

def convertVideoToImage(srcKey, srcBucket):
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
    # Chop video into images(-i is the input file name, -r is the number of frames per sec, img_%04d.jpg is the o/p format)
    try:
        cmd='ffmpeg -i ' + str(localFilename)+ ' -r ' + str(FRAMES) + ' ' + TMP_DIR + 'img_%04d.jpg'
        print cmd
        sp.call(cmd,shell=True)
    except Exception as e:
        print("Error calling ffmpeg:", e)
    # Upload file to s3
    tmpKey = srcKey.rsplit(".",1)
    # Removing the video extention
    destKey = tmpKey[0]

    n = 1
    for imgFile in sorted(os.listdir(TMP_DIR)):
        if imgFile.find("img") != -1:
            with open(TMP_DIR + imgFile, "rb") as imageFile:
                f = imageFile.read()
                imgByteArr = bytearray(f)
                #timestamp of each frame in video
                timestamp = (1/FRAMES)*n
                strtimestamp = str(round(timestamp,2))
                destination = destKey+'/'+strtimestamp+'_'+str(imgFile)
                print destination
                n = n+1
                object = s3.Bucket(srcBucket).put_object(Body=imgByteArr, Key=destination)


if __name__ == '__main__':
    #bucket in which the file can be downloaded and uploaded into
    srcBucket = str(os.environ.get('BUCKET'))
    #File to be downloaded from s3
    srcKey = str(os.environ.get('FILE'))
    convertVideoToImage(srcKey,srcBucket)
