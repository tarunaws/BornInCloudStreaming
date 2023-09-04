#!/bin/bash
#Run docker in remote server
#ssh linuxtechi@192.168.10.10 bash -c "'echo $msg'"
# docker build -t tarunaws/psl:$1 .
# docker push tarunaws/psl:$1
scp 1_copy.py 2_distribute.py 3_splitter.py 4_transcoder.py 5_joining.py 6_packager.py Dockerfile root@10.53.96.239:/home/lab/dockerbuild
ssh root@10.53.96.239 docker build -t tarunaws/psl:something3 /home/lab/dockerbuild
