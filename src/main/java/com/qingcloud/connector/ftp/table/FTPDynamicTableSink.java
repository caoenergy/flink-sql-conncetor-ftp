package com.qingcloud.connector.ftp.table;

import com.qingcloud.connector.ftp.internal.options.FtpWriteOptions;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.data.RowData;

/**
 * tableSink for ftp.
 */
@Internal
public class FTPDynamicTableSink implements DynamicTableSink {
	private final FtpWriteOptions ftpWriteOptions;
	private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
	private final TableSchema schema;

	public FTPDynamicTableSink(
		FtpWriteOptions ftpWriteOptions,
		EncodingFormat<SerializationSchema<RowData>> encodingFormat,
		TableSchema schema) {
		this.ftpWriteOptions = ftpWriteOptions;
		this.encodingFormat = encodingFormat;
		this.schema = schema;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return encodingFormat.getChangelogMode();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		FtpRowDataRichOutputFormat.Builder builder = FtpRowDataRichOutputFormat.builder()
			.setFtpWriteOptions(ftpWriteOptions)
			.setEncodingFormat(encodingFormat.createRuntimeEncoder(
				context,
				schema.toPhysicalRowDataType()));
		return OutputFormatProvider.of(builder.build());
	}

	@Override
	public DynamicTableSink copy() {
		return new FTPDynamicTableSink(ftpWriteOptions, encodingFormat, schema);
	}

	@Override
	public String asSummaryString() {
		return "FTP : " + ftpWriteOptions.getProtocol();
	}
}
