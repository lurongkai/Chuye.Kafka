d_home=~/Downloads

cd /usr/local/bin/kafka_2.10-0.9.0.0/
sh zookeeper-server-stop.sh
sh kafka-server-stop.sh

sudo rm /usr/local/bin/kafka_2.10-0.9.0.0/ -rf
sudo rm /tmp/kafka-logs/ -rf
sudo rm /tmp/zookeeper/ -rf

cd $d_home
sudo tar -xzf kafka_2.10-0.9.0.0.tgz -C .
sudo mv kafka_2.10-0.9.0.0 /usr/local/bin/

cd /usr/local/bin/kafka_2.10-0.9.0.0/
screen -S zk -d -m bin/zookeeper-server-start.sh config/zookeeper.properties
screen -S kf -d -m bin/kafka-server-start.sh config/server.properties
