package com.nic.flink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

public class Playground {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporaryTable("source",
                TableDescriptor.forConnector("datagen")
                        .schema(
                                Schema.newBuilder()
                                        .column("orderId", DataTypes.INT())
                                        .column("timestamp", DataTypes.TIMESTAMP())
                                        .build()
                        )
                        .option(DataGenConnectorOptions.ROWS_PER_SECOND, 10000L)
                        .option(ConfigOptions.key("fields.orderId.min").longType().noDefaultValue(), 1L)
                        .build()
        );
        Table source = tEnv.from("source");

        final TableResult result = source.select(
                $("orderId"),
                $("timestamp")
        ).execute();

        final JobID jobID = result.getJobClient().get().getJobID();



        env.registerJobListener();

        final CloseableIterator<Row> resultIter = result.collect();

        while (resultIter.hasNext()) {
            final Row next = resultIter.next();
            System.out.println(next);
        }
    }
}
