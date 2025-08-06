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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class SQLSubmitter {

    private static String left = """
    CREATE TABLE `leftTable`
        (
            `version`                      STRING,
            `row_time`                   TIMESTAMP_LTZ(3),
            WATERMARK FOR `row_time` AS `row_time` - INTERVAL '1' SECOND
        )
        WITH (
            'connector' = 'datagen',
            'rows-per-second' = '10'
        );
    """;

    private static String right = """
    CREATE TABLE `rightTable`
        (
            `version`                      STRING,
            `row_time`                   TIMESTAMP_LTZ(3),
            WATERMARK FOR `row_time` AS `row_time` - INTERVAL '1' SECOND
        )
        WITH (
            'connector' = 'datagen',
            'rows-per-second' = '10'
        );
    """;

    private static String joined= """
    CREATE TEMPORARY VIEW joined as  
    SELECT 
        L.version as l_version, 
        R.version as r_version,
        COALESCE(L.window_start, R.window_start) as window_join_start,
        COALESCE(L.window_end, R.window_end) as window_join_end,
        COALESCE(L.window_time, R.window_time) as window_join_time
    FROM (
        SELECT * FROM TABLE(TUMBLE(TABLE leftTable, DESCRIPTOR(row_time), INTERVAL '5' SECONDS))
    ) L
    FULL JOIN (
        SELECT * FROM TABLE(TUMBLE(TABLE rightTable, DESCRIPTOR(row_time), INTERVAL '5' SECONDS))
    ) R
    ON L.version = R.version AND L.window_start = R.window_start AND L.window_end = R.window_end AND L.window_time = R.window_time;       
    """;

    private static String joinedSink = """
            CREATE TABLE joined_sink
        (
            `l_version`                    STRING,
            `r_version`                    STRING,
            `joined_window_start`              TIMESTAMP(9),
            `joined_window_end`                TIMESTAMP(9),
            `joined_window_time`             TIMESTAMP_LTZ(6)
        )
        WITH (
            'connector' = 'blackhole'
        )
    """;

    private static String joinedInsert = """
        INSERT INTO joined_sink SELECT * from joined;
    """;

    private static String aggregate = """
        CREATE TEMPORARY VIEW aggregate as 
        SELECT window_join_start as agg_window_start, window_join_end as agg_window_end, window_join_time as agg_window_time, COUNT(l_version) as l_version_count, COUNT(r_version) as r_version_count
        FROM joined
        GROUP BY window_join_start, window_join_end, window_join_time;
    """;

    private static String rewindowAggregate = """
        CREATE TEMPORARY VIEW aggregate as 
        SELECT window_start as agg_window_start, window_end as agg_window_end, window_time as agg_window_time, COUNT(l_version) as l_version_count, COUNT(r_version) as r_version_count FROM
        TABLE(TUMBLE(TABLE joined, DESCRIPTOR(window_join_time), INTERVAL '5' SECONDS))
        GROUP BY window_start, window_end, window_time;
    """;

    private static String aggregateSink = """
            CREATE TABLE aggregate_sink
        (
            `window_start`              TIMESTAMP(9),
            `window_end`                TIMESTAMP(9),
            `window_time`             TIMESTAMP_LTZ(6),
            `l_version_count`                    BIGINT,
            `r_version_count`                    BIGINT
        )
        WITH (
            'connector' = 'blackhole'
        )
    """;

    private static String aggreagateInsert = """
        INSERT INTO aggregate_sink SELECT * from aggregate;
    """;

	public static void main(String[] args) throws Exception {

        System.out.printf("======\n====== Problem 1 - Cannot re-window after window join=====\n======\n");
        try {                    
            System.out.println(cannotAggregateWithNewWindow());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.printf("======\n====== Problem 2 - Two Inserts causing Calcite error =====\n======\n");
        try {            
            System.out.println(twoInserts());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.printf("======\n====== Problem 3- Aggregate after window is not re-using the window =====\n======\n");
        System.out.println(aggregateCreatingChangelog());
    }

    private static String twoInserts() {
        final TableEnvironment tEnv = TableEnvironment.create(new Configuration());

        tEnv.executeSql(left);
        tEnv.executeSql(right);
        tEnv.executeSql(joined);
        tEnv.executeSql(joinedSink);
        tEnv.executeSql(aggregate);
        tEnv.executeSql(aggregateSink);

        return tEnv.createStatementSet()
            .addInsertSql(joinedInsert)
            .addInsertSql(aggreagateInsert)
        .explain(ExplainDetail.CHANGELOG_MODE);
    }

    private static String aggregateCreatingChangelog() {

        final TableEnvironment tEnv = TableEnvironment.create(new Configuration());
        tEnv.executeSql(left);
        tEnv.executeSql(right);
        tEnv.executeSql(joined);
        tEnv.executeSql(aggregate);
        return tEnv.explainSql("select * from aggregate", ExplainDetail.CHANGELOG_MODE);
    }

    private static String cannotAggregateWithNewWindow() {

        final TableEnvironment tEnv = TableEnvironment.create(new Configuration());
        tEnv.executeSql(left);
        tEnv.executeSql(right);
        tEnv.executeSql(joined);
        tEnv.executeSql(rewindowAggregate);
        return tEnv.explainSql("select * from aggregate", ExplainDetail.CHANGELOG_MODE);
    }

}