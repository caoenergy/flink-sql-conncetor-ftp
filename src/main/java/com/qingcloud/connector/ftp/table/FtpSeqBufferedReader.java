package com.qingcloud.connector.ftp.table;


import com.qingcloud.connector.ftp.internal.connection.FtpHandler;
import com.qingcloud.connector.ftp.internal.connection.FtpHandlerFactory;
import com.qingcloud.connector.ftp.internal.connection.IFtpHandler;
import com.qingcloud.connector.ftp.internal.options.FtpReadOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * ftp bufferReader.
 */
public class FtpSeqBufferedReader implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FtpSeqBufferedReader.class);

    private IFtpHandler ftpHandler;

    private final Iterator<String> iter;

    private BufferedReader br;

    private String fileEncoding;

    private String fileName;

    private long fromLine = 0;

    private Map<String, Long> fileReadIndex = new HashMap<>();

    private long fileIndex;

    //ftp配置信息
    private final FtpReadOptions readOptions;

    public FtpSeqBufferedReader(
            IFtpHandler ftpHandler,
            Iterator<String> iter,
            FtpReadOptions readOptions) {
        this.ftpHandler = ftpHandler;
        this.iter = iter;
        this.readOptions = readOptions;
    }

    public String readLine(boolean flag) throws IOException {
        try {
            if (br == null) {
                if (readOptions.isDeleteRead() && fileName != null) {
                    try {
                        ftpHandler.deleteAllFilesInDir(fileName, new ArrayList<>());
                    } catch (Exception e) {
                        LOG.warn("delete failed file name - " + fileName);
                    } finally {
                        fileName = null;
                    }
                }
                nextStream();
            }

            if (br != null) {
                if (flag) {
                    br.reset();
                    String line = br.readLine();
                    LOG.warn("reset from line - " + line);
                }
                String line = br.readLine();
                if (line != null) {
                    fileIndex += line.getBytes(StandardCharsets.UTF_8).length + readOptions
                            .getByteDelimiterLength();
                    fileReadIndex.put(fileName, fileIndex);
                }
                if (line != null) {
                    br.mark(line.getBytes(StandardCharsets.UTF_8).length * 2);
                } else {
                    close();
                    return readLine(false);
                }
                return line;
            } else {
                return null;
            }
        } catch (Exception e) {
            try {
                Thread.sleep(10000);
                /*TODO 异常恢复*/
                return readLine(true);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
    }

    private void nextStream() throws IOException {
        if (iter.hasNext()) {
            String file = iter.next();
            fileName = file;
            fileIndex = 0;
            InputStream in = ftpHandler.getInputStream(file);
            if (in == null) {
                throw new RuntimeException(String.format(
                        "can not get inputStream for file [%s], please check file read and write permissions",
                        file));
            }
            br = new BufferedReader(new InputStreamReader(in, fileEncoding));
            if (fileReadIndex.containsKey(file)) {
                LOG.info(String.format(
                        "file - %s skip - %s byte",
                        fileName,
                        br.skip((fileIndex = fileReadIndex.get(file)))));
            } else {
                for (int i = 0; i < fromLine; i++) {
                    String skipLine = br.readLine();
                    LOG.info("Skip line:{}", skipLine);
                }
            }
        } else {
            br = null;
        }
    }

    public void close() throws IOException {
        if (br != null) {
            br.close();
            br = null;

            if (ftpHandler instanceof FtpHandler) {
                try {
                    ((FtpHandler) ftpHandler).getFtpClient().completePendingCommand();
                } catch (Exception e) {
                    /*TODO 如果出现了超时异常，就直接获取一个新的ftpHandler*/
                    LOG.warn("FTPClient completePendingCommand has error ->", e);
                    try {
                        ftpHandler.logoutFtpServer();
                    } catch (Exception exception) {
                        LOG.warn("FTPClient logout has error ->", exception);
                    }
                    ftpHandler = FtpHandlerFactory.createFtpHandler(readOptions.getProtocol());
                    ftpHandler.loginFtpServer(readOptions);
                }
            }
        }
    }

    public void setFileEncoding(String fileEncoding) {
        this.fileEncoding = fileEncoding;
    }

    public void setFromLine(long fromLine) {
        this.fromLine = fromLine;
    }

    public Map<String, Long> getFileReadIndex() {
        return fileReadIndex;
    }

    public void setFileReadIndex(Map<String, Long> fileReadIndex) {
        this.fileReadIndex = fileReadIndex;
    }
}
