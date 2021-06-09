package com.qingcloud.connector.ftp.internal.options;

import com.qingcloud.connector.ftp.FtpConnectionOptions;
import com.qingcloud.connector.ftp.internal.constants.FtpConfigConstants;
import com.qingcloud.connector.ftp.utils.FileUtil;

import javax.annotation.Nullable;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * option for ftp writer.
 */
public class FtpWriteOptions extends FtpConnectionOptions {
	private static final long serialVersionUID = 1L;

	private final String protocol;
	private final String path;
	private final String encoding;
	private final String writeMode;
	private final long rollingSize;

	public FtpWriteOptions(
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
		String writeMode,
		long rollingSize) {
		super(host, port, username, password, timeout, connectPattern, privateKeyPath);
		this.protocol = protocol;
		this.path = path;
		this.encoding = encoding;
		this.writeMode = writeMode;
		this.rollingSize = rollingSize;
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

	public String getWriteMode() {
		return writeMode;
	}

	public int getTimeout() {
		return timeout;
	}

	public long getRollingSize() {
		return rollingSize;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder of {@link FtpWriteOptions}.
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
		private String writeMode;
		private int timeout;
		private long rollingSize;

		/**
		 * required, host name.
		 *
		 * @param host
		 *
		 * @return
		 */
		public Builder setHost(String host) {
			this.host = host;
			return this;
		}

		/**
		 * optional port name.
		 *
		 * @param port
		 *
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
		 *
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
		 *
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
		 *
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
		 *
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
		 *
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
		 *
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
		 *
		 * @return
		 */
		public Builder setConnectPattern(String connectPattern) {
			this.connectPattern = Optional
				.of(connectPattern)
				.orElse(FtpConfigConstants.STANDARD_FTP_PROTOCOL);
			return this;
		}

		/**
		 * optional writeMode.
		 *
		 * @param writeMode
		 *
		 * @return
		 */
		public Builder setWriteMode(String writeMode) {
			this.writeMode = writeMode;
			return this;
		}

		/**
		 * optional timeout.
		 *
		 * @param timeout
		 *
		 * @return
		 */
		public Builder setTimeout(Integer timeout) {
			this.timeout = Optional.of(timeout).orElse(FtpConfigConstants.DEFAULT_TIMEOUT);
			return this;
		}

		/**
		 * rolling size for each file.
		 *
		 * @param rollingSize
		 *
		 * @return
		 */
		public Builder setRollingSize(long rollingSize) {
			this.rollingSize = rollingSize;
			return this;
		}

		public FtpWriteOptions build() {
			checkNotNull(host);
			checkNotNull(username);
			checkNotNull(password);
			checkNotNull(protocol);
			checkNotNull(path);
			checkNotNull(writeMode);
			return new FtpWriteOptions(
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
				writeMode,
				rollingSize);
		}
	}

}
