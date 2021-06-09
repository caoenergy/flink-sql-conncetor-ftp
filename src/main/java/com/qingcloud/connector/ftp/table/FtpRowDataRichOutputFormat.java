package com.qingcloud.connector.ftp.table;

import com.esotericsoftware.minlog.Log;
import com.qingcloud.connector.ftp.internal.connection.FtpHandlerFactory;
import com.qingcloud.connector.ftp.internal.connection.IFtpHandler;
import com.qingcloud.connector.ftp.internal.constants.ConstantValue;
import com.qingcloud.connector.ftp.internal.options.FtpWriteOptions;
import com.qingcloud.connector.ftp.utils.ExceptionUtil;
import org.apache.commons.net.ftp.FTP;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static com.qingcloud.connector.ftp.internal.constants.ConstantValue.*;


/**
 * ftpOutputFormat.
 */
public class FtpRowDataRichOutputFormat extends RichOutputFormat<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(FtpRowDataRichOutputFormat.class);

    private final FtpWriteOptions writeOptions;

    private final SerializationSchema<RowData> encodingFormat;

    private static final int NEWLINE = 10;

    private transient IFtpHandler ftpHandler;

    private transient BufferedWriter writer;

    private transient OutputStream os;

    private static transient String outputPath;

    private transient String fileName;

    private static final String OVERWRITE_MODE = "overwrite";

    private static final String FILE_SUFFIX = ".data";

    private transient long rollingSize = 0L;

    private transient int blockIndex = 0;

    private FtpRowDataRichOutputFormat(
            FtpWriteOptions writeOptions,
            SerializationSchema<RowData> encodingFormat) {
        this.writeOptions = writeOptions;
        this.encodingFormat = encodingFormat;
        try {
            IFtpHandler ftpHandler = FtpHandlerFactory.createFtpHandler(writeOptions.getProtocol());
            ftpHandler.loginFtpServer(writeOptions);
            String path = writeOptions.getPath();
            path = path.replaceAll(REPLACE_FILE_SPLIT, SINGLE_SLASH_SYMBOL);
            String outputPath = null;
            if (checkPath(path, FOLDER_REGEX)) {
                outputPath = path;
            } else if (checkPath(path, FILE_REGEX)) {
                outputPath = path.substring(
                        0,
                        path.lastIndexOf(SINGLE_SLASH_SYMBOL) + 1);
            }
            checkOutputDir(ftpHandler, outputPath, writeOptions.getWriteMode());
            ftpHandler.logoutFtpServer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        openSource();
        initPath();
    }

    private void openSource() {
        try {
            ftpHandler = FtpHandlerFactory.createFtpHandler(writeOptions.getProtocol());
            ftpHandler.loginFtpServer(writeOptions);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void initPath() {
        String path = writeOptions.getPath();
        path = path.replaceAll(REPLACE_FILE_SPLIT, SINGLE_SLASH_SYMBOL);
        if (checkPath(path, FOLDER_REGEX)) {
            fileName = UUID.randomUUID() + FILE_SUFFIX;
            outputPath = path;
        } else if (checkPath(path, FILE_REGEX)) {
            fileName = path.substring(path.lastIndexOf(SINGLE_SLASH_SYMBOL) + 1);
            outputPath = path.substring(0, path.lastIndexOf(SINGLE_SLASH_SYMBOL) + 1);
        } else {
            throw new IllegalArgumentException("file path does not matches");
        }
    }

    private boolean checkPath(String path, String regEx) {
        path = path.replace("\\", SINGLE_SLASH_SYMBOL);
        return path.matches(regEx);
    }

    private void checkOutputDir(IFtpHandler ftpHandler, String outputPath, String writeMode) {
        try {
            if (ftpHandler.isDirExist(outputPath)) {
                ftpHandler.mkDirRecursive(outputPath);
            } else {
                if (OVERWRITE_MODE.equalsIgnoreCase(writeMode)
                        && !SINGLE_SLASH_SYMBOL.equals(outputPath)) {
                    ftpHandler.deleteAllFilesInDir(outputPath, null);
                    ftpHandler.mkDirRecursive(outputPath);
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void writeRecord(RowData record) throws IOException {
        if (writer == null) {
            nextBlock(blockIndex);
            LOG.info("new writer - " + writer.hashCode());
        }
        writeSingleRecordToFile(record);
    }

    private void writeSingleRecordToFile(RowData record) throws IOException {
        if (writer == null) {
            nextBlock(blockIndex);
        }
        String line = rowData2String(record);
        if ((rollingSize += line.getBytes(StandardCharsets.UTF_8).length + 1)
                > writeOptions.getRollingSize()) {
            rollingSize = 0;
            if (writer != null) {
                writer.flush();
                writer.close();
                writer = null;
            }
            if (os != null) {
                os.close();
                os = null;
            }
            if (writer == null) {
                this.ftpHandler.completePendingCommand();
                nextBlock(++blockIndex);
                this.writer.write(line);
                this.writer.write(NEWLINE);
            }
        } else {
            this.writer.write(line);
            this.writer.write(NEWLINE);
        }
        LOG.info("write line - " + line);
    }

    private void nextBlock(int index) {
        if (writer != null) {
            return;
        }
        os = ftpHandler.getOutputStream(
                outputPath + SINGLE_SLASH_SYMBOL +
                        fileName.substring(0, fileName.lastIndexOf(ConstantValue.POINT_SYMBOL))
                        + ConstantValue.SINGLE_LINE + "task-" +
                        getRuntimeContext().getIndexOfThisSubtask() + "-" + index
                        + ConstantValue.POINT_SYMBOL +
                        fileName.substring(fileName.lastIndexOf(ConstantValue.POINT_SYMBOL) + 1));
        try {
            writer = new BufferedWriter(new OutputStreamWriter(os, writeOptions.getEncoding()));
        } catch (UnsupportedEncodingException e) {
            Log.error(ExceptionUtil.getErrorMessage(e));
        }
    }

    private String rowData2String(
            RowData rowData) throws UnsupportedEncodingException {
        byte[] bytes = encodingFormat.serialize(rowData);
        String msg = new String(bytes, FTP.DEFAULT_CONTROL_ENCODING);
        return msg;
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            LOG.info("close writer - " + writer.hashCode());
            writer.flush();
            writer.close();
            writer = null;
            os.close();
            os = null;
            try {
                this.ftpHandler.completePendingCommand();
            } catch (Exception e) {
                throw new IOException(ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder of {@link FtpRowDataRichOutputFormat}.
     */
    public static class Builder {
        private FtpWriteOptions ftpWriteOptions;
        private SerializationSchema<RowData> encodingFormat;

        public Builder setFtpWriteOptions(FtpWriteOptions ftpWriteOptions) {
            this.ftpWriteOptions = ftpWriteOptions;
            return this;
        }

        public Builder setEncodingFormat(SerializationSchema<RowData> encodingFormat) {
            this.encodingFormat = encodingFormat;
            return this;
        }

        public FtpRowDataRichOutputFormat build() {
            return new FtpRowDataRichOutputFormat(ftpWriteOptions, encodingFormat);
        }
    }
}
