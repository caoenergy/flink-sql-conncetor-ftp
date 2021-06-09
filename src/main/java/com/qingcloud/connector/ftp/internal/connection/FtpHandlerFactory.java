package com.qingcloud.connector.ftp.internal.connection;

import org.apache.commons.lang3.StringUtils;

/**
 * factory for ftp handler.
 */
public class FtpHandlerFactory {
	public static IFtpHandler createFtpHandler(String protocolStr) {
		IFtpHandler ftpHandler;
		Protocol protocol = Protocol.getByName(protocolStr);
		if (Protocol.SFTP.equals(protocol)) {
			ftpHandler = new SftpHandler();
		} else {
			ftpHandler = new FtpHandler();
		}
		return ftpHandler;
	}

	enum Protocol {
		FTP, SFTP;

		public static Protocol getByName(String name) {
			if (StringUtils.isEmpty(name)) {
				return SFTP;
			} else {
				return Protocol.valueOf(name.toUpperCase());
			}
		}
	}
}
