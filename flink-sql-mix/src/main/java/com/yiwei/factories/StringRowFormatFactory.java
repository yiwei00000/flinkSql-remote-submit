package com.yiwei.factories;


import com.yiwei.factories.serialization.StringDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * Created by yiwei 2020/5/23
 * 支持源以单string字段的方式创建source表
 */
public class StringRowFormatFactory extends TableFormatFactoryBase<Row>
        implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {
    public StringRowFormatFactory() {
        super("string", 1, true);
    }


    @Override
    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        return new StringDeserializationSchema();
    }

    @Override
    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        return null;
    }
}
