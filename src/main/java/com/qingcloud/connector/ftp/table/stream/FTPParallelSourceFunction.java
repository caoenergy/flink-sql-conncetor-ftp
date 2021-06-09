package com.qingcloud.connector.ftp.table.stream;

import com.qingcloud.connector.ftp.internal.connection.FtpHandlerFactory;
import com.qingcloud.connector.ftp.internal.connection.IFtpHandler;
import com.qingcloud.connector.ftp.internal.options.FtpReadOptions;
import com.qingcloud.connector.ftp.table.FtpSeqBufferedReader;
import com.qingcloud.connector.ftp.utils.FileUtil;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.qingcloud.connector.ftp.internal.constants.ConstantValue.FILE_REGEX_SPLIT;


/**
 * stream source function.
 */
@Internal
public class FTPParallelSourceFunction extends RichParallelSourceFunction<RowData> implements ResultTypeQueryable<RowData>, CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FTPParallelSourceFunction.class);

    private final FtpReadOptions ftpReadOptions;
    private final DeserializationSchema<RowData> deserialization;
    private final TypeInformation<RowData> rowDataTypeInfo;

    private transient IFtpHandler ftpHandler;
    private transient FtpSeqBufferedReader br;
    private volatile boolean isRunning = true;
    private transient ListState<Tuple2<String, Long>> fileReadOffset;
    private transient Map<String, Long> fileReadIndex;

    public FTPParallelSourceFunction(
            FtpReadOptions ftpReadOptions,
            DeserializationSchema<RowData> deserialization,
            TypeInformation<RowData> rowDataTypeInfo) {
        this.ftpReadOptions = ftpReadOptions;
        this.deserialization = deserialization;
        this.rowDataTypeInfo = rowDataTypeInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ftpHandler = FtpHandlerFactory.createFtpHandler(ftpReadOptions.getProtocol());
        ftpHandler.loginFtpServer(ftpReadOptions);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (isRunning) {
            fileReadOffset.clear();
            if (br != null) {
                fileReadIndex = br.getFileReadIndex();
            }
            fileReadIndex.keySet().forEach(key -> {
                Tuple2<String, Long> t = new Tuple2<>();
                t.f0 = key;
                t.f1 = fileReadIndex.get(key);
                try {
                    fileReadOffset.add(t);
                    LOG.info(String.format(
                            "store task %s checkPointed file index msg %s",
                            getRuntimeContext().getIndexOfThisSubtask(),
                            t.f0 + ":" + t.f1));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        ListStateDescriptor<Tuple2<String, Long>> stateDescriptor = new ListStateDescriptor<>(
                "file read index for list", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        }));

        fileReadOffset = ctx.getOperatorStateStore().getListState(stateDescriptor);

        fileReadIndex = new HashMap<>();

        if (ctx.isRestored()) {
            for (Tuple2<String, Long> t : fileReadOffset.get()) {
                fileReadIndex.put(t.f0, t.f1);
                LOG.info(String.format("restore checkPointed file name - %s size %s", t.f0, t.f1));
            }
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        Loop:
        while (isRunning) {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            int subtaskNums = getRuntimeContext().getNumberOfParallelSubtasks();
            List<Tuple2<String, Long>> subtaskFiles = new ArrayList<>();
            List<String> files = new ArrayList<>();
            if (!isRunning) {
                break;
            }
            FileUtil.addFiles(
                    ftpReadOptions.getPath(),
                    ftpHandler,
                    files);
            int numSplits = (Math.min(files.size(), subtaskNums));
            for (String file : files) {
                String[] fileInfo = file.split(FILE_REGEX_SPLIT);
                if (Math.abs(fileInfo[0].hashCode() % numSplits) == indexOfThisSubtask) {
                    FTPSourceFunction.createFileInfo(subtaskFiles, fileInfo, fileReadIndex);
                }
            }
            if (!isRunning) {
                break;
            }
            br = new FtpSeqBufferedReader(
                    ftpHandler,
                    subtaskFiles.stream().map(s -> s.f0).iterator(),
                    ftpReadOptions
            );
            if (fileReadIndex != null && fileReadIndex.size() > 0) {
                br.setFileReadIndex(fileReadIndex);
            }

            if (ftpReadOptions.isFirstLineHeader()) {
                br.setFromLine(1);
            } else {
                br.setFromLine(0);
            }
            br.setFileEncoding(ftpReadOptions.getEncoding());
            try {
                while (true) {
                    if (!isRunning) {
                        break Loop;
                    }
                    String line;
                    synchronized (ctx.getCheckpointLock()) {
                        if ((line = br.readLine(false)) == null) {
                            break;
                        }
                    }
                    RowData row = deserialization.deserialize(line.getBytes(StandardCharsets.UTF_8));
                    if (row != null) {
                        ctx.collect(row);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (br != null) {
                fileReadIndex = br.getFileReadIndex();
                br.close();
                br = null;
            }
            Thread.sleep(ftpReadOptions.getStreamInterval());
        }
    }

    @Override
    public void cancel() {
        try {
            isRunning = false;
            if (br != null) {
                br.close();
            }
            if (ftpHandler != null) {
                ftpHandler.logoutFtpServer();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
