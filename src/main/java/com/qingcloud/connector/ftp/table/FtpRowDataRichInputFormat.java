package com.qingcloud.connector.ftp.table;

import com.qingcloud.connector.ftp.internal.connection.FtpHandlerFactory;
import com.qingcloud.connector.ftp.internal.connection.IFtpHandler;
import com.qingcloud.connector.ftp.internal.options.FtpReadOptions;
import com.qingcloud.connector.ftp.utils.ExceptionUtil;
import com.qingcloud.connector.ftp.utils.FileUtil;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * ftpInputFormat.
 */
@Internal
public class FtpRowDataRichInputFormat extends RichInputFormat<RowData, InputSplit> implements ResultTypeQueryable<RowData> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FtpRowDataRichInputFormat.class);

    private final FtpReadOptions ftpReadOptions;
    private final DeserializationSchema<RowData> deserializer;
    private final TypeInformation<RowData> rowDataTypeInfo;

    private transient IFtpHandler ftpHandler;
    private transient FtpSeqBufferedReader br;
    private transient String line;

    public FtpRowDataRichInputFormat(
            FtpReadOptions ftpReadOptions,
            DeserializationSchema<RowData> deserializer,
            TypeInformation<RowData> rowDataTypeInfo) {
        this.ftpReadOptions = ftpReadOptions;
        this.deserializer = deserializer;
        this.rowDataTypeInfo = rowDataTypeInfo;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        ftpHandler = FtpHandlerFactory.createFtpHandler(ftpReadOptions.getProtocol());
        ftpHandler.loginFtpServer(ftpReadOptions);
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        try {
            IFtpHandler ftpHandler = FtpHandlerFactory.createFtpHandler(ftpReadOptions.getProtocol());
            ftpHandler.loginFtpServer(ftpReadOptions);
            List<String> files = new ArrayList<>();
            FileUtil.addFiles(ftpReadOptions.getPath(), ftpHandler, files);
            LOG.info("FTP files = {}", files);
            int numSplits = (Math.min(files.size(), minNumSplits));
            FtpRowDataInputSplit[] ftpRowDataInputSplit = new FtpRowDataInputSplit[numSplits];
            for (int index = 0; index < numSplits; ++index) {
                ftpRowDataInputSplit[index] = new FtpRowDataInputSplit();
            }
            for (int i = 0; i < files.size(); i++) {
                ftpRowDataInputSplit[i % numSplits].getPaths().add(files.get(i));
            }
            ftpHandler.logoutFtpServer();
            return ftpRowDataInputSplit;
        } catch (Exception e) {
            LOG.warn(ExceptionUtil.getErrorMessage(e));
            return createErrorInputSplit(e);
        }
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit split) throws IOException {
        checkIfCreateSplitFailed(split);
        FtpRowDataInputSplit inputSplit = (FtpRowDataInputSplit) split;
        List<String> paths = inputSplit.getPaths();
        if (ftpReadOptions.isFirstLineHeader()) {
            br = new FtpSeqBufferedReader(
                    ftpHandler,
                    paths.iterator(),
                    ftpReadOptions);
            br.setFromLine(1);
        } else {
            br = new FtpSeqBufferedReader(
                    ftpHandler,
                    paths.iterator(),
                    ftpReadOptions);
            br.setFromLine(0);
        }
        br.setFileEncoding(ftpReadOptions.getEncoding());
    }

    @Override
    public boolean reachedEnd() throws IOException {
        line = br.readLine(false);
        return line == null;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (line == null) {
            return null;
        }
        return deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        try {
            if (br != null) {
                br.close();
            }
            if (ftpHandler != null) {
                ftpHandler.logoutFtpServer();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }

    private ErrorInputSplit[] createErrorInputSplit(Exception e) {
        ErrorInputSplit[] inputSplits = new ErrorInputSplit[1];
        ErrorInputSplit errorInputSplit = new ErrorInputSplit(ExceptionUtil.getErrorMessage(e));
        inputSplits[0] = errorInputSplit;
        return inputSplits;
    }

    private void checkIfCreateSplitFailed(InputSplit inputSplit) {
        if (inputSplit instanceof ErrorInputSplit) {
            throw new RuntimeException(((ErrorInputSplit) inputSplit).getErrorMessage());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder of {@link FtpRowDataRichInputFormat}.
     */
    public static class Builder {
        private FtpReadOptions ftpReadOptions;
        private DeserializationSchema<RowData> deserializer;
        private TypeInformation<RowData> rowDataTypeInfo;
        private DynamicTableSource.DataStructureConverter dataStructureConverter;
        private List<LogicalType> parsingTypes;

        public Builder setFtpOptions(FtpReadOptions ftpReadOptions) {
            this.ftpReadOptions = ftpReadOptions;
            return this;
        }

        public Builder setDeserializer(DeserializationSchema<RowData> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public Builder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInfo = rowDataTypeInfo;
            return this;
        }

        public Builder setDataStructureConverter(DynamicTableSource.DataStructureConverter dataStructureConverter) {
            this.dataStructureConverter = dataStructureConverter;
            return this;
        }

        public Builder setParsingTypes(List<LogicalType> parsingTypes) {
            this.parsingTypes = parsingTypes;
            return this;
        }

        public FtpRowDataRichInputFormat build() {
            return new FtpRowDataRichInputFormat(
                    ftpReadOptions,
                    deserializer,
                    rowDataTypeInfo);
        }
    }
}
