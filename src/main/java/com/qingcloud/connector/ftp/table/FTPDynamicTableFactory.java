package com.qingcloud.connector.ftp.table;

import com.qingcloud.connector.ftp.internal.options.FtpReadOptions;
import com.qingcloud.connector.ftp.internal.options.FtpWriteOptions;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * dynamicTableFactory.
 */
public class FTPDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "ftp";

    public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("host")
            .stringType()
            .noDefaultValue()
            .withDescription("hostname for ftp or sftp");
    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .noDefaultValue();
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("user")
            .stringType()
            .defaultValue("ftp");
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .defaultValue("");
    public static final ConfigOption<String> PRIVATE_KEY_PATH = ConfigOptions.key("privateKeyPath")
            .stringType()
            .noDefaultValue()
            .withDescription("private key path for sftp");
    public static final ConfigOption<String> PROTOCOL = ConfigOptions.key("protocol")
            .stringType()
            .defaultValue("ftp")
            .withDescription("type of connection");
    public static final ConfigOption<String> PATH = ConfigOptions.key("path")
            .stringType()
            .noDefaultValue()
            .withDescription("path for file");
    public static final ConfigOption<String> ENCODING = ConfigOptions.key("encoding")
            .stringType()
            .defaultValue("UTF-8");
    public static final ConfigOption<String> CONNECT_PATTERN = ConfigOptions.key("connect-pattern")
            .stringType()
            .defaultValue("pasv")
            .withDescription("connection mode for ftp");
    public static final ConfigOption<String> WRITE_MODE = ConfigOptions.key("writeMode")
            .stringType()
            .defaultValue("overwrite")
            .withDescription("write mode for file");
    public static final ConfigOption<Boolean> IS_FIRST_LINE_HEADER = ConfigOptions
            .key("first-line-header")
            .booleanType()
            .defaultValue(false);
    public static final ConfigOption<Integer> TIME_OUT = ConfigOptions.key("timeout")
            .intType()
            .defaultValue(2000);
    public static final ConfigOption<String> READ_MODE = ConfigOptions
            .key("read-mode")
            .stringType()
            .defaultValue("once");
    public static final ConfigOption<Integer> STREAM_INTERVAL = ConfigOptions
            .key("stream-interval")
            .intType()
            .defaultValue(2000);
    public static final ConfigOption<Long> ROLLING_SIZE = ConfigOptions
            .key("rolling-size")
            .longType()
            .defaultValue(1024 * 1024 * 50L);
    public static final ConfigOption<Boolean> DELETE_READ = ConfigOptions
            .key("delete-read")
            .booleanType()
            .defaultValue(false);
    public static final ConfigOption<Integer> BYTE_DELIMITER_LENGTH = ConfigOptions
            .key("byte-delimiter-length")
            .intType()
            .defaultValue(1);

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(
                this, context
        );
        ReadableConfig options = helper.getOptions();
        helper.validate();
        EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);

        return new FTPDynamicTableSink(
                getFtpWriteOptions(options),
                encodingFormat,
                context.getCatalogTable().getSchema());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(
                this,
                context);
        ReadableConfig options = helper.getOptions();
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);
        helper.validate();
        validateConfigOptions(options);
        return new FTPDynamicTableSource(
                getFtpReadOptions(options),
                decodingFormat,
                context.getCatalogTable().getSchema());
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HOSTNAME);
        requiredOptions.add(PATH);
        requiredOptions.add(FactoryUtil.FORMAT);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(PORT);
        optionalOptions.add(FactoryUtil.FORMAT);
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(PROTOCOL);
        optionalOptions.add(PRIVATE_KEY_PATH);
        optionalOptions.add(ENCODING);
        optionalOptions.add(CONNECT_PATTERN);
        optionalOptions.add(WRITE_MODE);
        optionalOptions.add(IS_FIRST_LINE_HEADER);
        optionalOptions.add(TIME_OUT);
        optionalOptions.add(READ_MODE);
        optionalOptions.add(STREAM_INTERVAL);
        optionalOptions.add(ROLLING_SIZE);
        optionalOptions.add(DELETE_READ);
        optionalOptions.add(BYTE_DELIMITER_LENGTH);
        return optionalOptions;
    }

    private FtpReadOptions getFtpReadOptions(ReadableConfig config) {
        return FtpReadOptions.builder()
                .setHost(config.get(HOSTNAME))
                .setPort(config.get(PORT))
                .setUsername(config.get(USERNAME))
                .setPassword(config.get(PASSWORD))
                .setPath(config.get(PATH))
                .setProtocol(config.get(PROTOCOL))
                .setPrivateKeyPath(config.get(PRIVATE_KEY_PATH))
                .setEncoding(config.get(ENCODING))
                .setConnectPattern(config.get(CONNECT_PATTERN))
                .setIsFirstLineHeader(config.get(IS_FIRST_LINE_HEADER))
                .setTimeout(config.get(TIME_OUT))
                .setReadMode(config.get(READ_MODE))
                .setStreamInterval(config.get(STREAM_INTERVAL))
                .setDeleteRead(config.get(DELETE_READ))
                .setByteDelimiter(config.get(BYTE_DELIMITER_LENGTH))
                .build();
    }

    private FtpWriteOptions getFtpWriteOptions(ReadableConfig config) {
        return FtpWriteOptions.builder()
                .setHost(config.get(HOSTNAME))
                .setPort(config.get(PORT))
                .setUsername(config.get(USERNAME))
                .setPassword(config.get(PASSWORD))
                .setPath(config.get(PATH))
                .setProtocol(config.get(PROTOCOL))
                .setPrivateKeyPath(config.get(PRIVATE_KEY_PATH))
                .setEncoding(config.get(ENCODING))
                .setConnectPattern(config.get(CONNECT_PATTERN))
                .setWriteMode(config.get(WRITE_MODE))
                .setTimeout(config.get(TIME_OUT))
                .setRollingSize(config.get(ROLLING_SIZE))
                .build();
    }

    private void validateConfigOptions(ReadableConfig config) {
        checkAllOrNone(config, new ConfigOption[]{
                USERNAME,
                PASSWORD
        });
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = countAndReturn(config, configOptions);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n" + String.join(
                        "\n",
                        getPropertyNames(configOptions)));
    }

    private int countAndReturn(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption<?> configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        return presentCount;
    }

    private String[] getPropertyNames(ConfigOption<?>[] configOptions) {
        return Arrays
                .stream(configOptions)
                .map(ConfigOption::key)
                .toArray(String[]::new);
    }
}
