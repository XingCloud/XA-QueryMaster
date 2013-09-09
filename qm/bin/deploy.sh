#!/bin/sh
line="############################################"
# Code base
code_home=/home/hadoop/git_project_home/XA-QueryMaster
# Java home
java_home=/usr/java/jdk1.7.0_25
# Branch
if [ "" = "$1" ]
then
  branch=i20
else
  echo "User defined branch found($1)"
  branch=$2
fi

# deploy bin home
scripts_home=${code_home}/bin/
# tomcat port
port=$1
aid=qm

if [ "8181" = ${port} ];then
  xa_env="production"
  # Tomcat home
  tomcat_home=/home/hadoop/catalina/apache-tomcat-7.0.39.8182
else
  xa_env="pre_production"
  # Tomcat home
  tomcat_home=/home/hadoop/catalina/apache-tomcat-7.0.39.8182
fi

echo "[CHECK-POINT] - Begin deploying data driller web interface."
echo ${line}
echo "[CODE-HOME] - "${code_home}
echo "[TOMCAT] - "${tomcat_home}
echo "[XA_ENV] - "${xa_env}
echo "[BRANCH] - "${branch}
echo ${line}
echo "[CHECK-POINT] - Update code from VCS"
cd ${code_home}
git pull
git checkout ${branch}

if [ $? -ne 0 ];then
  echo "Git update/checkout failed."
  exit 1
else
  echo "Update/checkout done."
fi

echo ${line}
echo "[CHECK-POINT] - Packaging."
mvn -f ${code_home}/qm/pom.xml clean package -Dxa_env=${xa_env} -DskipTests=true

echo "[CHECK-POINT] - Shutdown tomcat."
sh ${tomcat_home}/bin/shutdown.sh
for((i=1;i<=10;i++));do
  proc=`ps aux | grep ${java_home} | grep tomcat | grep -v "grep" | grep ${port} | awk '{print $2}'`
  if [ "" = "$proc" ]
  then
    break
  else
    echo "Tomcat(${proc}) is not shutdown yet. Try again 3 second later($i)."
    sleep 3
  fi
done
proc=`ps aux | grep ${java_home} | grep tomcat | grep -v "grep" | grep ${port} | awk '{print $2}'`
if [ "" != "$proc" ];then
  echo "Tomcat(${proc}) is not shutdown, and it will be killed directly."
  kill -9 $proc
fi
echo "Tomcat is shutdown."
echo "[CHECK-POINT] - Clean application - ${aid}"
rm -rf ${tomcat_home}/webapps/${aid}
rm -rf ${tomcat_home}/webapps/${aid}*.war

echo "[CHECK-POINT] - Copy application - ${aid}"
cp ${code_home}/target/${aid}.war ${tomcat_home}/webapps
echo "[CHECK-POINT] - Start web server."
sh ${tomcat_home}/bin/startup.sh
