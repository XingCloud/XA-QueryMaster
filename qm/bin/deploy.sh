#!/bin/sh
line="############################################"
# Code base
code_home=/home/hadoop/DataDrillerWebInterfaceWithDrill
# deploy bin home
scripts_home=${code_home}/bin/
# tomcat port
tport=$1
# Artifact Id
aid=dd

if [ "8081" = ${tport} ];then
  xa_env="production"
  # Tomcat home
  tomcat_home=/home/hadoop/catalina/apache-tomcat-7.0.39.8081
  redis_queue_port=10000
else
  xa_env="pre_production"
  # Tomcat home
  tomcat_home=/home/hadoop/catalina/apache-tomcat-7.0.39.8082
  redis_queue_port=10001
fi

echo "[CHECK-POINT] - Begin deploying data driller web interface."
echo ${line}
echo "[CODE-HOME] - "${code_home}
echo "[XA_ENV] - "${xa_env}
echo "[TOMCAT-PORT] - "${tport}
echo ${line}
echo "[CHECK-POINT] - Update code from VCS"
svn up ${code_home}

if [ $? -ne 0 ];then
  echo "SVN update failed."
  exit 1
else
  echo "Update done."
fi

echo ${line}
echo "[CHECK-POINT] - Clean local adhoc repository."
rm -rf /home/hadoop/.m2/repository/com/xingcloud/AdhocProcessorRPC
echo ${line}

echo "[------copy adhoc-processor jar to local repo------]"
if [ "production" = ${xa_env} ];then
  rm -rf /home/hadoop/local_repo/production/*
  mvn install:install-file -Dfile=/home/hadoop/AdhocProcessorRPC/target/AdhocProcessorRPC-4.3.jar -DgroupId=com.xingcloud -DartifactId=AdhocProcessorRPC -Dversion=4.3 -Dpackaging=jar -DlocalRepositoryPath=/home/hadoop/local_repo/production
else
  rm -rf /home/hadoop/local_repo/pre_production/*
  mvn install:install-file -Dfile=/home/hadoop/AdhocProcessorRPC/target/AdhocProcessorRPC-4.3.jar -DgroupId=com.xingcloud -DartifactId=AdhocProcessorRPC -Dversion=4.3 -Dpackaging=jar -DlocalRepositoryPath=/home/hadoop/local_repo/pre_production
fi

echo "[CHECK-POINT] - Packaging."
mvn -f ${code_home}/pom.xml clean package -Dxa_env=${xa_env} -Dtport=${tport} -Daid=${aid}

echo "[CHECK-POINT] - Shutdown tomcat."
sh ${tomcat_home}/bin/shutdown.sh
java_bin=`which java`
for((i=1;i<=10;i++));do
    proc=`ps aux | grep ${java_bin} | grep tomcat | grep -v "grep" | grep ${tport} | awk '{print $2}'`
    if [ "" = "$proc" ]
    then
        break
    else
        echo "Tomcat(${proc}) is not shutdown yet. Try again 3 second later($i)."
        sleep 3
    fi
done
proc=`ps aux | grep ${java_bin} | grep tomcat | grep -v "grep" | grep ${port} | awk '{print $2}'`
if [ "" != "$proc" ];then
    echo "Tomcat(${proc}) is not shutdown, and it will be killed directly."
    kill -9 $proc
fi
echo "Tomcat is shutdown."

echo "[CHECK-POINT] - Clean application - ${aid}"
rm -rf ${tomcat_home}/webapps/${aid}
rm -rf ${tomcat_home}/webapps/${aid}*.war

echo "[CHECK-POINT] - Clean redis tags(db=0)."
#python /home/hadoop/xa/scripts/clear_query_tag.py --host=192.168.1.61 --port=${redis_queue_port} --db=0
echo "[CHECK-POINT] - Clean redis tags(db=1)."
#python /home/hadoop/xa/scripts/clear_query_tag.py --host=192.168.1.61 --port=${redis_queue_port} --db=1
echo "[CHECK-POINT] - Clean redis tags(db=2)."
#python /home/hadoop/xa/scripts/clear_query_tag.py --host=192.168.1.61 --port=${redis_queue_port} --db=2
sleep 1

echo "[CHECK-POINT] - Copy application - ${aid}"
cp ${code_home}/target/${aid}.war ${tomcat_home}/webapps
echo "[CHECK-POINT] - Start web server."
sh ${tomcat_home}/bin/startup.sh
