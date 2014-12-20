echo "Configuring ImpalaToGo"

read -p "Please enter your access key " ACCESS_KEY
read -p "Please enter your secret key " SECRET_KEY
eval sed -e 's/ACCESS_KEY/${ACCESS_KEY}/g' -e 's/SECRET_KEY/${SECRET_KEY}/g' conf/hdfs-site.template > hdfs-site.xml
eval sed -e 's/ACCESS_KEY/${ACCESS_KEY}/g' -e 's/SECRET_KEY/${SECRET_KEY}/g' conf/hive-site.template > hive-site.xml


read -p "Is it master ? y/n " IsMaster

if [ "$IsMaster" == "n" ]; then
read -p "Please enter DNS of Master" MASTER_DNS

eval sed 's/MASTER_DNS/${MASTER_DNS}/g' conf/impala_defaults.template >impala
else
eval sed 's/MASTER_DNS/localhost/g' conf/impala_defaults.template >impala
fi

sudo cp hdfs-site.xml /etc/impala/conf
sudo cp hive-site.xml /etc/hive/conf/
sudo cp hive-site.xml /etc/alternatives/hive-conf/
sudo cp impala /etc/default/

rm hdfs-site.xml
rm hive-site.xml

