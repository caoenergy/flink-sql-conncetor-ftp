package com.qingcloud.connector.ftp.internal.constants;

import java.io.Serializable;

/**
 * file msg for sftp.
 */
public class FileMsg implements Serializable {
	private static final long serialVersionUID = 1L;

	private String filePath;
	private long fileSize;
	private int mTime;

	public FileMsg() {
	}

	public FileMsg(String filePath) {
		this.filePath = filePath;
	}

	public FileMsg(String filePath, long fileSize, int mTime) {
		this.filePath = filePath;
		this.fileSize = fileSize;
		this.mTime = mTime;
	}

	public FileMsg(String filePath, int mTime) {
		this.filePath = filePath;
		this.mTime = mTime;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public int getmTime() {
		return mTime;
	}

	public void setmTime(int mTime) {
		this.mTime = mTime;
	}
}
