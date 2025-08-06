/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ExecutionException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// import com.ibm.ei.streamproc.udf.ToTimestampLtzUdf;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>
 * For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink
 * Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for
 * 'mainClass').
 */
public class SQLSubmitter {

    private static String orders_table = """
                CREATE TABLE `orders-with-currency___TABLE`
                (
                    `order_id`                     STRING,
                    `currency`                     STRING,
                    `price`                        BIGINT,
                    `order_time`                   TIMESTAMP_LTZ(3),
                    `created_time`                 TIMESTAMP_LTZ(3),
                    WATERMARK FOR `order_time` AS `order_time` - INTERVAL '0' MINUTE
                )
                WITH (
                    'properties.bootstrap.servers' = 'kafka.01:9000',
                    'connector' = 'kafka',
                    'json.ignore-parse-errors' = 'true',
                    'format' = 'json',
                    'topic' = 'orders-with-currency',
                    'properties.security.protocol' = 'PLAINTEXT',
                    'properties.isolation.level' = 'read_committed',
                    'scan.startup.mode' = 'earliest-offset',
                    'json.timestamp-format.standard' = 'ISO-8601'
                );
            """;

    private static String orders_view = """
                CREATE TEMPORARY VIEW `orders-with-currency`  AS
                SELECT * FROM `orders-with-currency___TABLE`;
            """;

    private static String currency = """
            CREATE TABLE `currency_rates_upsert`

            (
                `currency`                     STRING,
                `currency_rate`                DOUBLE,
                `update_time`                  TIMESTAMP_LTZ(3),
                WATERMARK FOR `update_time` AS `update_time` - INTERVAL '1' MINUTE,
                PRIMARY KEY(currency) NOT ENFORCED
            )
            WITH (
                'connector' = 'upsert-kafka',
                'topic' = 'currency-rates-upsert',
                'properties.bootstrap.servers' = 'kafka.01:9000',
                'key.format' = 'json',
                'value.format' = 'json',
                'key.json.ignore-parse-errors' = 'true',
                'value.json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = 'PLAINTEXT',
                'properties.isolation.level' = 'read_committed'
            )
            """;

    private static String currency_view = """
                        CREATE TEMPORARY VIEW `currency-rates-upsert` AS
            SELECT * FROM `currency_rates_upsert`;
                    """;

    private static String temporal = """
                CREATE TEMPORARY VIEW `add-currency-rates-upsert` AS
                SELECT
                    `orders-with-currency`.`order_id` AS `order_id`,
                    `orders-with-currency`.`price` AS `price`,
                    `orders-with-currency`.`currency` AS `currency`,
                    `currency-rates-upsert`.`currency_rate` AS `currency_rate`,
                    `orders-with-currency`.`order_time` AS `order_time`,
                    `currency-rates-upsert`.`update_time` AS `update_time`
                FROM `orders-with-currency`
                LEFT JOIN `currency-rates-upsert` FOR SYSTEM_TIME AS OF `orders-with-currency`.`order_time`
                ON `orders-with-currency`.`currency` = `currency-rates-upsert`.`currency`;
            """;

    private static String ordersSink = """
            CREATE TABLE orders_sink
            (
                order_id              STRING,
                currency              STRING,
                price                 BIGINT,
                order_time            TIMESTAMP_LTZ(6),
                created_time            TIMESTAMP_LTZ(9)
            )
            WITH (
                'connector' = 'print'
            )
            """;

    private static String ordersInsert = """
            INSERT INTO orders_sink SELECT * from `orders-with-currency`;
            """;

    private static String currencySink = """
                           CREATE TABLE currency_sink (
                               currency        STRING,
                               currency_rate   DOUBLE,
                               update_time           TIMESTAMP_LTZ(6)
                           )
                           WITH  (
                'connector' = 'print'
            );
                       """;

    private static String currencyInsert = """
                INSERT INTO currency_sink SELECT * from `currency-rates-upsert`;
            """;

    private static String temporalSink = """
            CREATE TABLE temporal_sink
                 (
                    order_id        STRING,
                    price           BIGINT,
                    currency        STRING,
                    currency_rate   DOUBLE,
                    order_time      TIMESTAMP_LTZ(6),
                    update_time     TIMESTAMP_LTZ(6)
                )
                WITH (
                    'properties.bootstrap.servers' = 'kafka.01:9000',
                    'connector' = 'kafka',
                    'format' = 'json',
                    'topic' = 'bids',
                    'properties.security.protocol' = 'PLAINTEXT'
                )
            """;

    private static String temporalInsert = """
                INSERT INTO temporal_sink SELECT * from `add-currency-rates-upsert`;
            """;

    private static String temporalWSSink = """
            CREATE TABLE temporal_sink_ws
                 (
                    order_id        STRING,
                    price           BIGINT,
                    currency        STRING,
                    currency_rate   DOUBLE,
                    order_time      TIMESTAMP_LTZ(6),
                    update_time     TIMESTAMP_LTZ(6)
                )
                WITH (
                'connector' = 'print'
                )
            """;

    private static String temporalInsertWS = """
                INSERT INTO temporal_sink_ws SELECT * from `add-currency-rates-upsert`;
            """;

    public static void main(String[] args) throws Exception {

        try {
            System.out.println(lookup());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String lookup() throws MalformedURLException, InterruptedException, ExecutionException {
        final StreamExecutionEnvironment remoteEnv = new RemoteStreamEnvironment("localhost", 8081, new Configuration(),
                new String[]{
                    "/Users/nic/.m2/repository/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar"
                    // "/Users/nic/.m2/repository/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar"
                },
                new URL[] { 
                });
        final TableEnvironment tEnv = StreamTableEnvironment.create(remoteEnv);
        tEnv.executeSql(orders_table);
        tEnv.executeSql(orders_view);
        tEnv.executeSql(currency);
        tEnv.executeSql(currency_view);
        tEnv.executeSql(temporal);
        tEnv.executeSql(ordersSink);
        tEnv.executeSql(temporalSink);
        tEnv.executeSql(temporalWSSink);
        tEnv.executeSql(currencySink);

        tEnv.createStatementSet()
                .addInsertSql(ordersInsert)
                .addInsertSql(currencyInsert)
                .addInsertSql(temporalInsert)
                .addInsertSql(temporalInsertWS)
                .execute();
        return "";
    }

}