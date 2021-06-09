package com.qingcloud.connector.ftp.table;

import org.apache.flink.core.io.InputSplit;

/**
 * errorInputSplit.
 */
public class ErrorInputSplit implements InputSplit {
	private static final long serialVersionUID = 1L;

	int splitNumber;

	String errorMessage;

	public ErrorInputSplit(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	@Override
	public int getSplitNumber() {
		return splitNumber;
	}
}
