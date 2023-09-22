FROM tarunaws/transcodingapi:workflow_v1
WORKDIR /code
COPY 1_submit.py .
COPY 2_contentid.py .
COPY 3_splitter.py .
COPY 4_transcoder.py .
COPY 5_joining.py .
COPY 6_packager.py .
COPY encoding_mac_v1.py .
