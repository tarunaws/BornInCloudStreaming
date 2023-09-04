FROM tarunaws/transcodingapi:workflow_v1
WORKDIR /code
COPY 1_copy.py .
COPY 2_distribute.py .
COPY 3_splitter.py .
COPY 4_transcoder.py .
COPY 5_joining.py .
COPY 6_packager.py .
