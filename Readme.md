# Video Splicer
##Chops video into images

It downloads a file from s3 and uploads it back to s3 after chopping the images locally.

Install ffmpeg into the console before running the code.
Following command installs ffmpeg:
```brew install ffmpeg --with-fdk-aac --with-ffplay --with-freetype --with-frei0r --with-libass --with-libvo-aacenc --with-libvorbis --with-libvpx --with-opencore-amr --with-openjpeg --with-opus --with-rtmpdump --with-schroedinger --with-speex --with-theora --with-tools```


The FRAMESPERSEC environment variable can be used to define the number of image shots you want per second.
The LOCALBUCKET environment variable can be used to set the bucket to upload/download the file.
The FILE environment variable can be used to specify the path of the video in s3 that has to be chopped.

Finally run the code using: `python videotoimage.py`
