1.执行 mvn clean package -DskipTests
2.将生成的jar包 flink-ftps-1.0-SNAPSHOT.jar  放入到flink对应版本的lib下即可


# FTP数据源

## 参数说明

| 参数              | 属性    | 默认值         | 说明                                                         | 必填 |
| ----------------- | ------- | -------------- | :----------------------------------------------------------- | ---- |
| host              | String | 无             | ip地址                                                       | 是   |
| port              | Int  | ftp 21 sftp 22 | 端口号                                                       | 否   |
| user          | String | ftp        | 用户名                                              | 否 |
| password          | String | ""       | 密码                                                         | 否  |
| protocol          | String | ftp            | 数据源类型：ftp 、sftp                                       | 否   |
| path              | String | 无             | 文件路径 1. 可填写xxx/xxx.txt  2. xxx/xxx1.txt,xxx/xxx2.txt   3.xxx   4.xxx/`[正则表达式]` | 是   |
| encoding          | String | UTF-8          | 文件编码类型                                                 | 否   |
| connect-pattern  | String | pasv           | ftp的连接模式 pasv被动模式 port主动模式                      | 否   |
| first-line-header | Boolean | false          | 是否跳过文件首行                                             | 否   |
| timeout| Int | 2000|ftp连接超时时间,毫秒|否|
| read-mode | String | once |once 支持多并发的一次读，stream支持多并发的持续读，streamSingle单线程的顺序读|否|
| stream-interval | Int | 2000|stream和singleStream读取文件的时间间隔|否|
| rolling-size | Long | 52428800 |ftp落地文件的滚动大小，默认没50mb滚动生成一个文件|否|
| delete-read | boolean | false |是否将已读文件删除|否|
| byte-delimiter-length | int| 1 |文件换行符的长度，\n,\r\n分别对应1，2|否|
| format | String | 无 |文件类型 csv，json|是|
| csv.* | String | 无 |csv类型解析的相关参数，[参考官方文档](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/table/formats/csv/)|否|
| json.* | String | 无 |json类型解析的相关参数，[参考官方文档](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/table/formats/json/)|是|

## 数据源说明

1. 根据文件名来匹配固定文件。
2. 根据文件名 逗号分隔来匹配多个文件。
3. 根据文件夹来递归匹配该目录下全部文件及其子目录下的全部文件。
4. 根据文件夹+正则法则，来匹配该文件夹及其子目录下的文件。
5. 针对，stream和singleStream模式，会根据stream-interval来发现新增的文件，以及有追加操作的文件。
6. 针对，stream和singleStream模式，可以设置checkpoint相关参数来记录文件读取的信息，其中包含文件的全路径名称，文件读取的字节数。
7. 针对，stream和singleStream模式，可以通过checkpoint或savepoint记录点来实现断点恢复，有效的避免了任务故障后，需要重新启动的问题，同时可以随时的暂停任务。

## 流批说明

| 值           | 说明                                                         |
| ------------ | ------------------------------------------------------------ |
| stream       | 并行的流式处理，会按照文件绝对路径的hash%并行度，将文件分配给不同的线程来进行读取collect操作 |
| streamSingle | 单线程读取文件，按照文件的修改时间来顺序读取                 |
| once         | 并行的批处理，根据文件的索引%并行度，将文件分配给不同的线程来进行collect操作，当文件全部读取完成时，该任务结束 |

## 操作说明

文中使用flink1.12_2.11版本

1. 将flink-connector-ftp_2.11-1.12.0.jar包放入到flink的lib目录下，如图

![](/Users/gxlevi/Documents/FTP数据源/image/WX20210607-150028.png)

2. 这里采用自定义的提交sql jar包来提交任务，将flink-sql-submit.jar 放入flink的examples目录下，并将要执行的sql文件同样放入该文件夹下

![](/Users/gxlevi/Documents/FTP数据源/image/WX20210607-150129.png)

3. 编写了一个简单的ftp-demo.sql来演示ftp和sftp的数据源基本功能

```
CREATE TABLE SFTP_SOURCE(
    readName string,
    cellPhone string,
    universityName string,
    city string,
    street string,
    ip string,
    pt AS PROCTIME())
WITH(
   'connector' = 'ftp',
   'host'='127.0.0.1',
   'username'='gxlevi',
   'password'='xcl317',
   'path'='/var/ftp/gxlevi/test/`user[0-9].txt`',
   'format'='csv',
   'protocol' = 'sftp',
   'read-mode'='singleStream'
);

CREATE TABLE FTP_SOURCE(
    readName string,
    cellPhone string,
    universityName string,
    city string,
    street string,
    ip string,
    pt AS PROCTIME())
WITH(
    'connector' = 'ftp',
    'host'='127.0.0.1',
    'path'='/test',
    'format'='csv',
    'read-mode'='singleStream'
);

CREATE TABLE FILE_SINK(
    readName string,
    cellPhone string,
    universityName string,
    city string,
    street string,
    ip string)
WITH(
     'connector' = 'filesystem',
     'path' = 'file:///Users/gxlevi/test',
     'format' = 'csv',
     'sink.rolling-policy.file-size' = '20MB'
);

CREATE TABLE PRINT_SINK(
    readName string,
    num bigint)
WITH (
    'connector' = 'print'
);

insert into PRINT_SINK select city,count(*) from SFTP_SOURCE group by city,tumble(pt,INTERVAL '10' SECOND);

insert into FILE_SINK select readName,cellPhone,universityName,city,street,ip from FTP_SOURCE;
```

4. 进入flink1.12.0目录下，执行一下命令

```bash
# 该命令用于执行ftp-dem.sql -w 文件指定相对路径 -f 指定文件名 -d 是detach模式，-p用于指定并行度
./bin/flink run -d -p 2 examples/flink-sql-submit.jar -w examples -f ftp-demo.sql
# 该命令用于测试ftp数据源的savepoint功能,会生成持久化数据对应的文件目录
./bin/flink stop jobId
# 该命令用于从 -s 指定的savepoint目录进行断点恢复
./bin/flink run -d -p 2 -s {savepoint path} examples/flink-sql-submit.jar -w examples -f ftp-demo.sql
```

