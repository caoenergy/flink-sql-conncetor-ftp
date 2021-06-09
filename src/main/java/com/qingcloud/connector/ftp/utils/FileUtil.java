package com.qingcloud.connector.ftp.utils;


import com.qingcloud.connector.ftp.internal.connection.IFtpHandler;
import com.qingcloud.connector.ftp.internal.constants.EProtocol;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import static com.qingcloud.connector.ftp.internal.constants.ConstantValue.*;
import static com.qingcloud.connector.ftp.internal.constants.FtpConfigConstants.DEFAULT_FTP_PORT;
import static com.qingcloud.connector.ftp.internal.constants.FtpConfigConstants.DEFAULT_SFTP_PORT;


/**
 * file utils.
 */
public class FileUtil {

	/**
	 * parse line with delimiter.
	 *
	 * @param msg
	 * @param columnDelimiter
	 * @param parsingTypes
	 *
	 * @return
	 */
	public static Row parseLine(
		String msg,
		String columnDelimiter,
		List<LogicalType> parsingTypes) {
		msg = msg.startsWith(CSV_HEADER) || msg.endsWith(CSV_HEADER)
			? msg.replace(CSV_HEADER, EMPTY_REPLACE) : msg;
		msg = msg.endsWith(FILE_NEW_LINE) ? msg.replace(FILE_NEW_LINE, EMPTY_REPLACE) : msg;
		msg = msg.contains(FILE_NEW_TAB) ? msg.replace(FILE_NEW_TAB, EMPTY_REPLACE) : msg;
		final String[] columns = msg.split(Pattern.quote(columnDelimiter));
		final Row row = new Row(RowKind.INSERT, parsingTypes.size());
		for (int i = 0; i < parsingTypes.size(); i++) {
			row.setField(i, parse(parsingTypes.get(i).getTypeRoot(), columns[i]));
		}
		return row;
	}

	private static Object parse(LogicalTypeRoot root, String value) {
		switch (root) {
			case INTEGER:
				return Integer.parseInt(value);
			case VARCHAR:
				return value;
			case BIGINT:
				return Long.parseLong(value);
			default:
				throw new IllegalArgumentException("message format failed");
		}
	}

	/**
	 * add file for each subtasks.
	 *
	 * @param path
	 * @param ftpHandler
	 * @param files
	 *
	 * @throws IOException
	 */
	public static void addFiles(
		String path,
		IFtpHandler ftpHandler,
		List<String> files) throws IOException {
		if (path != null && path.length() > 0) {
			if (!path.contains(FILE_REGEX_SPLIT)) {
				path = path
					.replace(FILE_NEW_LINE, EMPTY_REPLACE)
					.replace(FILE_NEW_TAB, EMPTY_REPLACE);
				String[] pathArray = path.split(FILES_SPLIT);
				for (String p : pathArray) {
					files.addAll(ftpHandler.getFiles(p.trim()));
				}
			} else {
				path = path.replace(FILE_NEW_LINE, "").replace(FILE_NEW_TAB, EMPTY_REPLACE);
				String dirPath = path.substring(0, path.lastIndexOf(SINGLE_SLASH_SYMBOL) + 1);
				String fileReg = path.substring(path.lastIndexOf(SINGLE_SLASH_SYMBOL) + 1)
					.replaceAll(FILE_REGEX_SPLIT, "");
				List<String> handlerFiles = ftpHandler.getFiles(dirPath);
				handlerFiles.forEach(file -> {
					if (file.lastIndexOf(FILE_REGEX_SPLIT) != -1 && file
						.substring(
							file.lastIndexOf(SINGLE_SLASH_SYMBOL) + 1,
							file.lastIndexOf(FILE_REGEX_SPLIT))
						.matches(fileReg)) {
						files.add(file);
					} else if (file
						.substring(file.lastIndexOf(SINGLE_SLASH_SYMBOL) + 1)
						.matches(fileReg)) {
						files.add(file);
					}
				});
			}
		}
	}

	/**
	 * validate and init port.
	 *
	 * @param port
	 * @param protocol
	 *
	 * @return
	 */
	public static int validatePort(Integer port, String protocol) {
		if (protocol == null) {
			throw new RuntimeException("protocol not supplied.");
		}
		int tmpPort = DEFAULT_FTP_PORT;
		if (port == null && EProtocol.SFTP.name().equalsIgnoreCase(protocol)) {
			tmpPort = DEFAULT_SFTP_PORT;
		} else if (port == null && EProtocol.FTP.name().equalsIgnoreCase(protocol)) {
			tmpPort = DEFAULT_FTP_PORT;
		}
		return tmpPort;
	}
}
