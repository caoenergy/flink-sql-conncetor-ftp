create table file_source(
    readName string,
    cellPhone string,
    universityName string,
    city string,
    street string,
    ip string
)with(
    'connector' = 'filesystem',
    'path' = 'file:///Users/gxlevi/test',
    'format' = 'csv'
);

create table ftp_sink(
    readName string,
    cellPhone string,
    universityName string,
    city string,
    street string,
    ip string
) with(
    'connector' = 'ftp',
    'host' = '127.0.0.1',
    'path' = '/home/ftptest/sink/sink.json',
    'format' = 'json',
    'rolling-size' = '52428800'
);

create table sftp_sink(
    readName string,
    cellPhone string,
    universityName string,
    city string,
    street string,
    ip string
) with(
      'connector' = 'ftp',
      'host' = '127.0.0.1',
      'username' = 'root',
      'password' = '123456',
      'path' = '/root/sink/sink.csv',
      'format' = 'csv',
      'protocol' = 'sftp',
      'rolling-size' = '52428800'
);

insert into sftp_sink select * from file_source;
insert into ftp_sink select * from file_source;
