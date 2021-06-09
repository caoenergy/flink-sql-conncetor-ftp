package com.qingcloud.connector.ftp.internal.options;


import com.qingcloud.connector.ftp.FtpConnectionOptions;
import com.qingcloud.connector.ftp.internal.constants.FtpConfigConstants;
import com.qingcloud.connector.ftp.utils.FileUtil;

import javax.annotation.Nullable;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * options for ftp reader.
 */
public class FtpReadOptions extends FtpConnectionOptions {
    private static final long serialVersionUID = 1L;

    private final String protocol;
    private final String path;
    private final String encoding;
    private final boolean isFirstLineHeader;
    private final String readMode;
    private final Integer streamInterval;
    private final boolean deleteRead;
    private final int byteDelimiterLength;

    public FtpReadOptions(
            String host,
            Integer port,
            @Nullable String username,
            @Nullable String password,
            int timeout,
            String connectPattern,
            String privateKeyPath,
            String protocol,
            String path,
            String encoding,
            boolean isFirstLineHeader,
            String readMode,
            Integer streamInterval,
            boolean deleteRead,
            int byteDelimiterLength) {
        super(host, port, username, password, timeout, connectPattern, privateKeyPath);
        this.protocol = protocol;
        this.path = path;
        this.encoding = encoding;
        this.isFirstLineHeader = isFirstLineHeader;
        this.readMode = readMode;
        this.streamInterval = streamInterval;
        this.deleteRead = deleteRead;
        this.byteDelimiterLength = byteDelimiterLength;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getPath() {
        return path;
    }

    public String getEncoding() {
        return encoding;
    }

    public String getConnectPattern() {
        return connectPattern;
    }

    public boolean isFirstLineHeader() {
        return isFirstLineHeader;
    }

    public int getTimeout() {
        return timeout;
    }

    public String getReadMode() {
        return readMode;
    }

    public Integer getStreamInterval() {
        return streamInterval;
    }

    public boolean isDeleteRead() {
        return deleteRead;
    }

    public int getByteDelimiterLength() {
        return byteDelimiterLength;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder of {@link FtpReadOptions}.
     */
    public static class Builder {
        private String host;
        private Integer port;
        private String username;
        private String password;
        private String privateKeyPath;
        private String protocol;
        private String path;
        private String encoding;
        private String connectPattern;
        private boolean isFirstLineHeader;
        private int timeout;
        private String readMode;
        private Integer streamInterval;
        private boolean deleteRead;
        private int byteDelimiterLength;

        /**
         * required, host name.
         *
         * @param host
         * @return
         */
        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * optional port.
         *
         * @param port
         * @return
         */
        public Builder setPort(Integer port) {
            this.port = port;
            return this;
        }

        /**
         * optional username.
         *
         * @param username
         * @return
         */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /**
         * optional password.
         *
         * @param password
         * @return
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * optional privateKeyPath.
         *
         * @param privateKeyPath
         * @return
         */
        public Builder setPrivateKeyPath(String privateKeyPath) {
            this.privateKeyPath = privateKeyPath;
            return this;
        }

        /**
         * optional protocol.
         *
         * @param protocol
         * @return
         */
        public Builder setProtocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        /**
         * optional path.
         *
         * @param path
         * @return
         */
        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        /**
         * optional encoding.
         *
         * @param encoding
         * @return
         */
        public Builder setEncoding(String encoding) {
            this.encoding = Optional.of(encoding).orElse("UTF-8");
            return this;
        }

        /**
         * optional connectPattern.
         *
         * @param connectPattern
         * @return
         */
        public Builder setConnectPattern(String connectPattern) {
            this.connectPattern = Optional
                    .of(connectPattern)
                    .orElse(FtpConfigConstants.STANDARD_FTP_PROTOCOL);
            return this;
        }

        /**
         * optional isFirstLineHeader.
         *
         * @param isFirstLineHeader
         * @return
         */
        public Builder setIsFirstLineHeader(Boolean isFirstLineHeader) {
            this.isFirstLineHeader = Optional.of(isFirstLineHeader).orElse(false);
            return this;
        }

        /**
         * optional timeout.
         *
         * @param timeout
         * @return
         */
        public Builder setTimeout(Integer timeout) {
            this.timeout = Optional.of(timeout).orElse(FtpConfigConstants.DEFAULT_TIMEOUT);
            return this;
        }

        /**
         * read mode for file.
         *
         * @param readMode
         * @return
         */
        public Builder setReadMode(String readMode) {
            this.readMode = readMode;
            return this;
        }

        /**
         * read interval for stream mode.
         *
         * @param streamInterval
         * @return
         */
        public Builder setStreamInterval(Integer streamInterval) {
            this.streamInterval = streamInterval;
            return this;
        }

        /**
         * delete read finished file.
         *
         * @param deleteRead
         * @return
         */
        public Builder setDeleteRead(boolean deleteRead) {
            this.deleteRead = deleteRead;
            return this;
        }

        public Builder setByteDelimiter(int byteDelimiterLength) {
            this.byteDelimiterLength = byteDelimiterLength;
            return this;
        }

        public FtpReadOptions build() {
            checkNotNull(host, "no host supplied.");
            checkNotNull(username, "no username supplied.");
            checkNotNull(password, "no password supplied.");
            checkNotNull(protocol, "no protocol supplied.");
            checkNotNull(connectPattern, "no connectPattern supplied.");
            return new FtpReadOptions(
                    host,
                    FileUtil.validatePort(port, protocol),
                    username,
                    password,
                    timeout,
                    connectPattern,
                    privateKeyPath,
                    protocol,
                    path,
                    encoding,
                    isFirstLineHeader,
                    readMode,
                    streamInterval,
                    deleteRead,
                    byteDelimiterLength);
        }
    }

}
