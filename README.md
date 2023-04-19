# Flume部署说明
data-flow项目主要为了将Flowwxport采集到的日志压缩包进行读取、预处理（将日志中的时间格式化）、发送至kafka
## Flume下载
```shell
# 使用华为镜像,或者可以使用官方下载方式
wget -c https://repo.huaweicloud.com/apache/flume/1.10.1/apache-flume-1.10.1-bin.tar.gz

# 解压至安装路径,如/opt
mv apache-flume-1.10.1-bin.tar.gz /opt
tar -xvf apache-flume-1.10.1-bin.tar.gz
```
## Flume配置

### 自定义Source配置
```shell
# 创建自定义配置文件,source自定义,sink指向kafka
cd ${FLUME_HOME} ; mkdir job
cd job
# 创建配置文件
touch flume-fl-kafka.conf
```

flume-fl-kafka.conf文件
```editorconfig
# example.conf: A single-node Flume configuration
# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = com.wanghe.flume.source.TagSource
a1.sources.r1.filePath = /data/flowexport/53/1
a1.channels.c1.type = memory
# 通道中存储的最大事件数
a1.channels.c1.capacity = 1000
# 每次channel从source获取事件或推送给sink的最大事件数
a1.channels.c1.transactionCapacity = 10

#设置Kafka接收器
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.topic = flumeTopic
a1.sinks.k1.kafka.flumeBatchSize = 10
#最大数据大小
a1.sinks.k1.kafka.producer.max.request.size = 104857600
a1.sinks.k1.kafka.producer.buffer.memory = 104857600
# kafak集群
a1.sinks.k1.kafka.bootstrap.servers=192.168.20.153:9092,192.168.20.154:9092,192.168.20.155:9092
a1.sinks.k1.requiredAcks = 1
a1.sinks.k1.batchSize = 20
a1.sinks.k1.kafka.flumeBatchSize = 20
a1.sinks.k1.kafka.producer.acks = 1
a1.sinks.k1.kafka.producer.linger.ms = 1

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1

```
### 代码打包
```shell
# 打开data-flow项目,在pom.xml同级路径下执行
mvn packages
```
### jar包上传至flume的lib路径
```shell
scp flume-fl-kafka-source-1.0-SNAPSHOT-jar-with-dependencies.jar user@host:`${FLUME_HOME}/lib`
```
## FLume启动
```shell
nohup ${FLUME_HOME}/bin/flume-ng agent -n a1 -c conf/ -f job/flume-fl-kafka.conf &
```

# 集群部署
Kafka+Zookeeper
```shell
略
```
