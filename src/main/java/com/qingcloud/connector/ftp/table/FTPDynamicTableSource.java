package com.qingcloud.connector.ftp.table;

import com.qingcloud.connector.ftp.internal.constants.ReadMode;
import com.qingcloud.connector.ftp.internal.options.FtpReadOptions;
import com.qingcloud.connector.ftp.table.stream.FTPParallelSourceFunction;
import com.qingcloud.connector.ftp.table.stream.FTPSourceFunction;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;

import java.util.Objects;

/**
 * table source for ftp.
 */
@Internal
public class FTPDynamicTableSource implements ScanTableSource {
    private final FtpReadOptions ftpReadOptions;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final TableSchema schema;

    public FTPDynamicTableSource(
            FtpReadOptions ftpReadOptions,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            TableSchema schema) {
        this.ftpReadOptions = ftpReadOptions;
        this.decodingFormat = decodingFormat;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        if (ReadMode.ONCE.name().equalsIgnoreCase(ftpReadOptions.getReadMode())) {
            FtpRowDataRichInputFormat.Builder builder = FtpRowDataRichInputFormat.builder()
                    .setFtpOptions(ftpReadOptions)
                    .setRowDataTypeInfo(runtimeProviderContext.createTypeInformation(schema.toPhysicalRowDataType()))
                    .setDeserializer(decodingFormat.createRuntimeDecoder(
                            runtimeProviderContext,
                            schema.toPhysicalRowDataType()))
                    .setDataStructureConverter(runtimeProviderContext.createDataStructureConverter(
                            schema.toPhysicalRowDataType()))
                    .setParsingTypes(schema.toPhysicalRowDataType().getLogicalType().getChildren());
            return InputFormatProvider.of(builder.build());
        } else {
            DeserializationSchema<RowData> runtimeDecoder = decodingFormat.createRuntimeDecoder(
                    runtimeProviderContext,
                    schema.toPhysicalRowDataType()
            );
            if (ReadMode.SINGLESTREAM.name().equalsIgnoreCase(ftpReadOptions.getReadMode())) {
                FTPSourceFunction ftpSourceFunction = new FTPSourceFunction(
                        ftpReadOptions,
                        runtimeDecoder,
                        runtimeProviderContext.createTypeInformation(schema.toPhysicalRowDataType()));
                return SourceFunctionProvider.of(ftpSourceFunction, false);
            } else {
                FTPParallelSourceFunction ftpParallelSourceFunction = new FTPParallelSourceFunction(
                        ftpReadOptions,
                        runtimeDecoder,
                        runtimeProviderContext.createTypeInformation(schema.toPhysicalRowDataType()));
                return SourceFunctionProvider.of(ftpParallelSourceFunction, false);
            }
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new FTPDynamicTableSource(ftpReadOptions, decodingFormat, schema);
    }

    @Override
    public String asSummaryString() {
        return "FTP : " + ftpReadOptions.getProtocol();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FTPDynamicTableSource)) {
            return false;
        }
        FTPDynamicTableSource that = (FTPDynamicTableSource) o;
        return Objects.equals(ftpReadOptions, that.ftpReadOptions) &&
                Objects.equals(decodingFormat, that.decodingFormat) &&
                Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ftpReadOptions, decodingFormat, schema);
    }
}
