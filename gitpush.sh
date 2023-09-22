#Run a script "sh gitpush.sh "PSL_v1"
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
git branch -M main
#git push -u origin main
git push origin main --force
mkdir -p /d/pslTranscoder/code-backup/persistent/$1
cp * -r /d/pslTranscoder/code-backup/persistent/$1
mkdir -p /d/"OneDrive - Persistent Systems Limited"/product/$1
cp * -r /d/"OneDrive - Persistent Systems Limited"/product/$1
scp 1_submit.py 2_contentid.py 3_splitter.py 4_transcoder.py 5_joining.py 6_packager.py Dockerfile root@10.53.96.239:/home/lab/dockerbuild
ssh root@10.53.96.239 "docker build -t tarunaws/psl:$1 /home/lab/dockerbuild && docker push tarunaws/psl:$1"
