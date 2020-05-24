package com.yiwei.sinks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;


/**
 * Created by yiwei 2020/4/16
 */
public class PrintSink implements AppendStreamTableSink<Row> {

    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;


    public PrintSink() {
    }

    @Override
    public TableSink configure(String[] strings, TypeInformation<?>[] typeInformations) {
        PrintSink configuredSink = new PrintSink();
        configuredSink.fieldNames = strings;
        configuredSink.fieldTypes = typeInformations;
        return configuredSink;
    }

    @Override
    public void emitDataStream(DataStream dataStream) {

        //不用实现，后期版本会移除该方法
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return Types.ROW_NAMED(fieldNames, fieldTypes);
    }

    @Override
    public DataType getConsumedDataType() {
        final RowTypeInfo rowTypeInfo = new RowTypeInfo(getFieldTypes(), getFieldNames());
        return fromLegacyInfoToDataType(rowTypeInfo);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.print();//输出到控制台
    }
}
