#Run a script "sh gitpush.sh "PSL_v1"
#This is for pushing the content to github
#!/bin/bash

mkdir -p /Users/tarunbhardwaj/myDrive/pslTranscoder/psl-backup/$1
cp * /Users/tarunbhardwaj/myDrive/pslTranscoder/psl-backup/$1

docker build -t tarunaws/psl:$1 .
docker push tarunaws/psl:$1
