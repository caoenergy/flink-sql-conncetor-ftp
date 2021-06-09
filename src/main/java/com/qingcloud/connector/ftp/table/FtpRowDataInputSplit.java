package com.qingcloud.connector.ftp.table;

import org.apache.flink.core.io.InputSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * ftpInputSplit.
 */
public class FtpRowDataInputSplit implements InputSplit {
	private static final long serialVersionUID = 1L;

	private final List<String> paths = new ArrayList<>();

	@Override
	public int getSplitNumber() {
		return 0;
	}

	public List<String> getPaths() {
		return paths;
	}

}
