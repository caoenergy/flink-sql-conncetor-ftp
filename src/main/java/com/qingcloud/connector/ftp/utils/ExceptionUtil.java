
package com.qingcloud.connector.ftp.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * ExceptionUtil for ftp.
 */
public class ExceptionUtil {
	private static final Logger LOG = LoggerFactory.getLogger(ExceptionUtil.class);

	public static String getErrorMessage(Throwable e) {
		if (null == e) {
			return null;
		}

		try (StringWriter stringWriter = new StringWriter(); PrintWriter writer = new PrintWriter(
			stringWriter)) {
			e.printStackTrace(writer);
			writer.flush();
			stringWriter.flush();
			StringBuffer buffer = stringWriter.getBuffer();
			return buffer.toString();
		} catch (Throwable ee) {
			LOG.error("", ee);
		}
		return null;
	}
}
