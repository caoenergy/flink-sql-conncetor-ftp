package com.qingcloud.connector.ftp.internal.constants;

/**
 * constant value for file.
 */
public class ConstantValue {
	public static final String POINT_SYMBOL = ".";
	public static final String TWO_POINT_SYMBOL = "..";
	public static final String SINGLE_SLASH_SYMBOL = "/";
	public static final String SYSTEM_PROPERTIES_KEY_FILE_ENCODING = "file.encoding";
	public static final String SINGLE_LINE = "_";
	public static final String FILE_REGEX_SPLIT = "`";
	public static final String FILE_NEW_LINE = "\n";
	public static final String FILE_NEW_TAB = "\r";
	public static final String FILES_SPLIT = ",";
	public static final String EMPTY_REPLACE = "";
	public static final String CSV_HEADER = "\uFEFF";
	public static final String REPLACE_FILE_SPLIT = "\\\\";
	public static final String FOLDER_REGEX = "([\\s\\w+:]*?/?(/.+/)?)((\\w+))$";
	public static final String FILE_REGEX = "([\\s\\w+:]*?/?(/.+/)?)((\\w+)\\.(\\w+))$";
}
