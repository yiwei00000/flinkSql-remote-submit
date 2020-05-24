package com.yiwei.factories.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Created by yiwei 2020/5/24
 */
public class StringDeserializationSchema implements DeserializationSchema<Row> {

    @Override
    public Row deserialize(byte[] message) throws IOException {
        return Row.of(new String(message));
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }


    @Override
    public TypeInformation<Row> getProducedType() {
        String[] names = {"msg"};
        return Types.ROW_NAMED(names,Types.STRING);
    }
}
