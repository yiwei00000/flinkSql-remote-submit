package com.yiwei.factories;



import com.yiwei.sinks.PrintSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.Schema.*;

/**
 * Created by yiwei 2020/4/16
 */
public class PrintSinkFactory implements StreamTableSinkFactory<Row> {

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {


        DescriptorProperties params = new DescriptorProperties();
        params.putProperties(properties);


        TableSchema tableSchema = params.getTableSchema(SCHEMA);


        final PrintSink printSink = new PrintSink();

        return (PrintSink)printSink.configure(tableSchema.getFieldNames(),tableSchema.getFieldTypes());
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("connector.type", "console");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> list = new ArrayList<>();
        list.add("connector.type");
        // schema
        list.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        list.add(SCHEMA + ".#." + SCHEMA_NAME);
        return list;
    }
}
