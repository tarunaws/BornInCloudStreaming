#This is for pushing the content to github
#!/bin/bash
git add .
git commit -m $1
git push -u origin main
mkdir -p /d/study/BornInCloudStreaming/code-backup/persistent/PSL_$1
cp * /d/study/BornInCloudStreaming/code-backup/persistent/PSL_$1
docker build -t bornincloudstreaming-PSL:PSL_$1 .
docker push bornincloudstreaming-PSL:PSL_$1git