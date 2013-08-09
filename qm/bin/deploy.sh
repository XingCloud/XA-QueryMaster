#!/bin/sh
line="############################################"

# Code base
code_home=/home/hadoop/git_project_home/XA-QueryMaster
# deploy bin home
scripts_home=${code_home}/bin/
# tomcat port
tport=$1
# Artifact Id
aid=dd

# Query master production:  8181
# Query master test:  8182
if [ "8181" = ${tport} ];then
  # Tomcat home
  tomcat_home=/home/hadoop/catalina/apache-tomcat-7.0.39.8181
else
  # Tomcat home
  tomcat_home=/home/hadoop/catalina/apache-tomcat-7.0.39.8182
fi

echo "[CHECK-POINT] - Begin deploying [QUERY-MASTER]"
echo ${line}
echo "[CODE-HOME] - "${code_home}
echo "[TOMCAT-PORT] - "${tport}
echo ${line}
echo "[CHECK-POINT] - Update code from VCS"
cd ${code_home}
git pull
echo "[CHECK-POINT] - Switch to master"
git checkout master