#This is for pushing the content to github
#!/bin/bash
#Below commands will set up git account first time.
#git config --global user.email "bhardwaj_tarun2006@yahoo.co.in"
#git config --global user.name "tarunaws"
#git config user.name
# git remote add origin https://github.com/tarunaws/BornInCloudStreaming.git
# git add .
# git commit -m $1
# git push -u origin main
git add .
git commit -m $1
git push -u origin main
mkdir -p /d/study/BornInCloudStreaming/code-backup/persistent/PSL_$1
cp * /d/study/BornInCloudStreaming/code-backup/persistent/PSL_$1
# docker build -t bornincloudstreaming-PSL:PSL_$1 .
# docker push bornincloudstreaming-PSL:PSL_$1