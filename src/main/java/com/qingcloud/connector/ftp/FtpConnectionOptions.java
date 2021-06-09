package com.qingcloud.connector.ftp;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;

/**
 * Options for the ftp scan.
 */
@PublicEvolving
public class FtpConnectionOptions implements Serializable {
	private static final long serialVersionUID = 1L;

	protected String host;
	protected Integer port;
	@Nullable
	protected String username;
	@Nullable
	protected String password;
	protected int timeout;
	protected String connectPattern;
	protected String privateKeyPath;

	protected FtpConnectionOptions(
		String host,
		Integer port,
		@Nullable String username,
		@Nullable String password,
		int timeout,
		String connectPattern,
		String privateKeyPath) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.timeout = timeout;
		this.connectPattern = connectPattern;
		this.privateKeyPath = privateKeyPath;
	}

	public String getHost() {
		return host;
	}

	public Integer getPort() {
		return port;
	}

	@Nullable
	public Optional<String> getUsername() {
		return Optional.ofNullable(username);
	}

	@Nullable
	public Optional<String> getPassword() {
		return Optional.ofNullable(password);
	}

	public int getTimeout() {
		return timeout;
	}

	public String getConnectPattern() {
		return connectPattern;
	}

	public String getPrivateKeyPath() {
		return privateKeyPath;
	}
}
