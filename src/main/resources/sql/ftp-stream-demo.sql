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
   'username'='root',
   'password'='123456',
   'path'='/root/test/`user[0-9].txt`',
   'format'='csv',
   'protocol' = 'sftp',
   'read-mode'='stream'
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
    'host'='127.0.0.2',
    'path'='/test',
    'format'='csv',
    'read-mode'='singleStream',
    'csv.ignore-parse-errors' = 'true',
    'csv.field-delimiter' = ','
);

CREATE TABLE PRINT_SINK(
    readName string,
    num bigint)
WITH (
    'connector' = 'print'
);

insert into PRINT_SINK select city,count(*) from SFTP_SOURCE group by city,tumble(pt,INTERVAL '10' SECOND);

insert into PRINT_SINK select city,count(*) from FTP_SOURCE group by city,HOP(pt,INTERVAL '10' SECOND ,INTERVAL '30' SECOND);

