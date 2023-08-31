FROM 261443307797.dkr.ecr.ap-south-1.amazonaws.com/bornincloudstreaming-dev:dev_v0
WORKDIR /code
COPY 1_copy.py .
COPY 2_distribute.py .
COPY 3_splitter.py .
COPY 4_transcoder.py .
COPY 5_joining.py .
COPY 6_packager.py .
COPY encoding_vmaf_v1.py .