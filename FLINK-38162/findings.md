# Findings

## Flink 1.20.1

### Problem 1 - Cannot rewindow

The time attribute is not available:

```java
java.lang.RuntimeException: Error while applying rule StreamPhysicalWindowTableFunctionRule(in:LOGICAL,out:STREAM_PHYSICAL), args [rel#3221:FlinkLogicalTableFunctionScan.LOGICAL.any.None: 0.[NONE].[NONE](input#0=RelSubset#3219,invocation=TUMBLE(DESCRIPTOR($2), 5000:INTERVAL SECOND),rowType=RecordType(VARCHAR(2147483647) l_version, VARCHAR(2147483647) r_version, TIMESTAMP_LTZ(3) *ROWTIME* window_join_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time))]
        at org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:250)
        at org.apache.calcite.plan.volcano.IterativeRuleDriver.drive(IterativeRuleDriver.java:59)
        at org.apache.calcite.plan.volcano.VolcanoPlanner.findBestExp(VolcanoPlanner.java:523)
        at org.apache.calcite.tools.Programs$RuleSetProgram.run(Programs.java:318)
        at org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgram.optimize(FlinkVolcanoProgram.scala:62)
        at org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.$anonfun$optimize$1(FlinkChainedProgram.scala:59)
        at scala.collection.TraversableOnce.$anonfun$foldLeft$1(TraversableOnce.scala:156)
        at scala.collection.TraversableOnce.$anonfun$foldLeft$1$adapted(TraversableOnce.scala:156)
        at scala.collection.Iterator.foreach(Iterator.scala:937)
        at scala.collection.Iterator.foreach$(Iterator.scala:937)
        at scala.collection.AbstractIterator.foreach(Iterator.scala:1425)
        at scala.collection.IterableLike.foreach(IterableLike.scala:70)
        at scala.collection.IterableLike.foreach$(IterableLike.scala:69)
        at scala.collection.AbstractIterable.foreach(Iterable.scala:54)
        at scala.collection.TraversableOnce.foldLeft(TraversableOnce.scala:156)
        at scala.collection.TraversableOnce.foldLeft$(TraversableOnce.scala:154)
        at scala.collection.AbstractTraversable.foldLeft(Traversable.scala:104)
        at org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.optimize(FlinkChainedProgram.scala:55)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.optimizeTree(StreamCommonSubGraphBasedOptimizer.scala:196)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.optimizeSinkBlocks(StreamCommonSubGraphBasedOptimizer.scala:83)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.doOptimize(StreamCommonSubGraphBasedOptimizer.scala:118)
        at org.apache.flink.table.planner.plan.optimize.CommonSubGraphBasedOptimizer.optimize(CommonSubGraphBasedOptimizer.scala:87)
        at org.apache.flink.table.planner.delegation.PlannerBase.optimize(PlannerBase.scala:320)
        at org.apache.flink.table.planner.delegation.PlannerBase.getExplainGraphs(PlannerBase.scala:534)
        at org.apache.flink.table.planner.delegation.StreamPlanner.explain(StreamPlanner.scala:103)
        at org.apache.flink.table.planner.delegation.StreamPlanner.explain(StreamPlanner.scala:51)
        at org.apache.flink.table.api.internal.TableEnvironmentImpl.explainInternal(TableEnvironmentImpl.java:697)
        at org.apache.flink.table.api.internal.TableEnvironmentImpl.explainSql(TableEnvironmentImpl.java:677)
        at org.apache.flink.table.api.TableEnvironment.explainSql(TableEnvironment.java:992)
        at com.example.SQLSubmitter.cannotAggregateWithNewWindow(SQLSubmitter.java:185)
        at com.example.SQLSubmitter.main(SQLSubmitter.java:146)
Caused by: org.apache.flink.table.api.ValidationException: The window function requires the timecol is a time attribute type, but is TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).
        at org.apache.flink.table.planner.plan.utils.WindowUtil$.validateTimeFieldWithTimeAttribute(WindowUtil.scala:180)
        at org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalWindowTableFunctionRule.convert(StreamPhysicalWindowTableFunctionRule.scala:60)
        at org.apache.calcite.rel.convert.ConverterRule.onMatch(ConverterRule.java:172)
        at org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:223)
        ... 30 more
```

### Problem 2 - Two inserts throw Calcite error

The presence of the two inserts causes the aggregate projection to fail with a Calcite error due to an expected `TIMESTAMP_LTZ(3) *ROWTIME* agg_window_time` that was instead `TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) agg_window_time`

```java
java.lang.RuntimeException: Error while applying rule ProjectToCalcRule, args [rel#1277:LogicalProject.NONE.any.None: 0.[NONE].[NONE](input=RelSubset#1276,exprs=[$2, $3, $4, $0, $1])]
        at org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:250)
        at org.apache.calcite.plan.volcano.IterativeRuleDriver.drive(IterativeRuleDriver.java:59)
        at org.apache.calcite.plan.volcano.VolcanoPlanner.findBestExp(VolcanoPlanner.java:523)
        at org.apache.calcite.tools.Programs$RuleSetProgram.run(Programs.java:318)
        at org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgram.optimize(FlinkVolcanoProgram.scala:62)
        at org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.$anonfun$optimize$1(FlinkChainedProgram.scala:59)
        at scala.collection.TraversableOnce.$anonfun$foldLeft$1(TraversableOnce.scala:156)
        at scala.collection.TraversableOnce.$anonfun$foldLeft$1$adapted(TraversableOnce.scala:156)
        at scala.collection.Iterator.foreach(Iterator.scala:937)
        at scala.collection.Iterator.foreach$(Iterator.scala:937)
        at scala.collection.AbstractIterator.foreach(Iterator.scala:1425)
        at scala.collection.IterableLike.foreach(IterableLike.scala:70)
        at scala.collection.IterableLike.foreach$(IterableLike.scala:69)
        at scala.collection.AbstractIterable.foreach(Iterable.scala:54)
        at scala.collection.TraversableOnce.foldLeft(TraversableOnce.scala:156)
        at scala.collection.TraversableOnce.foldLeft$(TraversableOnce.scala:154)
        at scala.collection.AbstractTraversable.foldLeft(Traversable.scala:104)
        at org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.optimize(FlinkChainedProgram.scala:55)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.optimizeTree(StreamCommonSubGraphBasedOptimizer.scala:196)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.optimizeBlock(StreamCommonSubGraphBasedOptimizer.scala:143)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.$anonfun$optimizeSinkBlocks$2(StreamCommonSubGraphBasedOptimizer.scala:89)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.$anonfun$optimizeSinkBlocks$2$adapted(StreamCommonSubGraphBasedOptimizer.scala:89)
        at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:58)
        at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:51)
        at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:47)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.optimizeSinkBlocks(StreamCommonSubGraphBasedOptimizer.scala:89)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.doOptimize(StreamCommonSubGraphBasedOptimizer.scala:118)
        at org.apache.flink.table.planner.plan.optimize.CommonSubGraphBasedOptimizer.optimize(CommonSubGraphBasedOptimizer.scala:87)
        at org.apache.flink.table.planner.delegation.PlannerBase.optimize(PlannerBase.scala:320)
        at org.apache.flink.table.planner.delegation.PlannerBase.getExplainGraphs(PlannerBase.scala:534)
        at org.apache.flink.table.planner.delegation.StreamPlanner.explain(StreamPlanner.scala:103)
        at org.apache.flink.table.planner.delegation.StreamPlanner.explain(StreamPlanner.scala:51)
        at org.apache.flink.table.api.internal.TableEnvironmentImpl.explainInternal(TableEnvironmentImpl.java:697)
        at org.apache.flink.table.api.internal.StatementSetImpl.explain(StatementSetImpl.java:103)
        at org.apache.flink.table.api.Explainable.explain(Explainable.java:40)
        at com.example.SQLSubmitter.twoInserts(SQLSubmitter.java:156)
        at com.example.SQLSubmitter.main(SQLSubmitter.java:134)
Caused by: java.lang.RuntimeException: Error occurred while applying rule ProjectToCalcRule
        at org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:157)
        at org.apache.calcite.plan.RelOptRuleCall.transformTo(RelOptRuleCall.java:269)
        at org.apache.calcite.plan.RelOptRuleCall.transformTo(RelOptRuleCall.java:284)
        at org.apache.calcite.rel.rules.ProjectToCalcRule.onMatch(ProjectToCalcRule.java:72)
        at org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:223)
        ... 36 more
Caused by: java.lang.IllegalArgumentException: Type mismatch:
rel rowtype: RecordType(TIMESTAMP(3) agg_window_start, TIMESTAMP(3) agg_window_end, TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) agg_window_time, VARCHAR(2147483647) CHARACTER SET "UTF-16LE" l_version, VARCHAR(2147483647) CHARACTER SET "UTF-16LE" r_version) NOT NULL
equiv rowtype: RecordType(TIMESTAMP(3) agg_window_start, TIMESTAMP(3) agg_window_end, TIMESTAMP_LTZ(3) *ROWTIME* agg_window_time, VARCHAR(2147483647) CHARACTER SET "UTF-16LE" l_version, VARCHAR(2147483647) CHARACTER SET "UTF-16LE" r_version) NOT NULL
Difference:
agg_window_time: TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) -> TIMESTAMP_LTZ(3) *ROWTIME*

        at org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:592)
        at org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
        at org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:144)
        ... 40 more
```

### Problem 3 - Planner not re-using window

In the aggregate's physical plan, we see that both the WindowJoin and the Calc are showing `changelogMode=[I]` - but then the aggregate becomes a `GroupAggregate` with `changelogMode=[I, UA]`:

```text
== Abstract Syntax Tree ==
LogicalProject(agg_window_start=[$0], agg_window_end=[$1], agg_window_time=[$2], l_version_count=[$3], r_version_count=[$4])
+- LogicalAggregate(group=[{0, 1, 2}], l_version_count=[COUNT($3)], r_version_count=[COUNT($4)])
   +- LogicalProject(agg_window_start=[$2], agg_window_end=[$3], agg_window_time=[$4], l_version=[$0], r_version=[$1])
      +- LogicalProject(l_version=[$0], r_version=[$5], window_join_start=[COALESCE($2, $7)], window_join_end=[COALESCE($3, $8)], window_join_time=[COALESCE($4, $9)])
         +- LogicalJoin(condition=[AND(=($0, $5), =($2, $7), =($3, $8), =($4, $9))], joinType=[full])
            :- LogicalProject(version=[$0], row_time=[$1], window_start=[$2], window_end=[$3], window_time=[$4])
            :  +- LogicalTableFunctionScan(invocation=[TUMBLE(DESCRIPTOR($1), 5000:INTERVAL SECOND)], rowType=[RecordType(VARCHAR(2147483647) version, TIMESTAMP_LTZ(3) *ROWTIME* row_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time)])
            :     +- LogicalProject(version=[$0], row_time=[$1])
            :        +- LogicalWatermarkAssigner(rowtime=[row_time], watermark=[-($1, 1000:INTERVAL SECOND)])
            :           +- LogicalTableScan(table=[[default_catalog, default_database, leftTable]])
            +- LogicalProject(version=[$0], row_time=[$1], window_start=[$2], window_end=[$3], window_time=[$4])
               +- LogicalTableFunctionScan(invocation=[TUMBLE(DESCRIPTOR($1), 5000:INTERVAL SECOND)], rowType=[RecordType(VARCHAR(2147483647) version, TIMESTAMP_LTZ(3) *ROWTIME* row_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time)])
                  +- LogicalProject(version=[$0], row_time=[$1])
                     +- LogicalWatermarkAssigner(rowtime=[row_time], watermark=[-($1, 1000:INTERVAL SECOND)])
                        +- LogicalTableScan(table=[[default_catalog, default_database, rightTable]])

== Optimized Physical Plan ==
GroupAggregate(groupBy=[agg_window_start, agg_window_end, agg_window_time], select=[agg_window_start, agg_window_end, agg_window_time, COUNT(l_version) AS l_version_count, COUNT(r_version) AS r_version_count], changelogMode=[I,UA])
+- Exchange(distribution=[hash[agg_window_start, agg_window_end, agg_window_time]], changelogMode=[I])
   +- Calc(select=[COALESCE(window_start, window_start0) AS agg_window_start, COALESCE(window_end, window_end0) AS agg_window_end, COALESCE(CAST(window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)), CAST(window_time0 AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) AS agg_window_time, version AS l_version, version0 AS r_version], changelogMode=[I])
      +- WindowJoin(leftWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rightWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], joinType=[FullOuterJoin], where=[AND(=(version, version0), =(window_time, window_time0))], select=[version, window_start, window_end, window_time, version0, window_start0, window_end0, window_time0], changelogMode=[I])
         :- Exchange(distribution=[hash[version, window_time]], changelogMode=[I])
         :  +- Calc(select=[version, window_start, window_end, window_time], changelogMode=[I])
         :     +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])], changelogMode=[I])
         :        +- WatermarkAssigner(rowtime=[row_time], watermark=[-(row_time, 1000:INTERVAL SECOND)], changelogMode=[I])
         :           +- TableSourceScan(table=[[default_catalog, default_database, leftTable]], fields=[version, row_time], changelogMode=[I])
         +- Exchange(distribution=[hash[version, window_time]], changelogMode=[I])
            +- Calc(select=[version, window_start, window_end, window_time], changelogMode=[I])
               +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])], changelogMode=[I])
                  +- WatermarkAssigner(rowtime=[row_time], watermark=[-(row_time, 1000:INTERVAL SECOND)], changelogMode=[I])
                     +- TableSourceScan(table=[[default_catalog, default_database, rightTable]], fields=[version, row_time], changelogMode=[I])

== Optimized Execution Plan ==
GroupAggregate(groupBy=[agg_window_start, agg_window_end, agg_window_time], select=[agg_window_start, agg_window_end, agg_window_time, COUNT(l_version) AS l_version_count, COUNT(r_version) AS r_version_count])
+- Exchange(distribution=[hash[agg_window_start, agg_window_end, agg_window_time]])
   +- Calc(select=[COALESCE(window_start, window_start0) AS agg_window_start, COALESCE(window_end, window_end0) AS agg_window_end, COALESCE(CAST(window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)), CAST(window_time0 AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) AS agg_window_time, version AS l_version, version0 AS r_version])
      +- WindowJoin(leftWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rightWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], joinType=[FullOuterJoin], where=[((version = version0) AND (window_time = window_time0))], select=[version, window_start, window_end, window_time, version0, window_start0, window_end0, window_time0])
         :- Exchange(distribution=[hash[version, window_time]])
         :  +- Calc(select=[version, window_start, window_end, window_time])
         :     +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])])
         :        +- WatermarkAssigner(rowtime=[row_time], watermark=[(row_time - 1000:INTERVAL SECOND)])
         :           +- TableSourceScan(table=[[default_catalog, default_database, leftTable]], fields=[version, row_time])
         +- Exchange(distribution=[hash[version, window_time]])
            +- Calc(select=[version, window_start, window_end, window_time])
               +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])])
                  +- WatermarkAssigner(rowtime=[row_time], watermark=[(row_time - 1000:INTERVAL SECOND)])
                     +- TableSourceScan(table=[[default_catalog, default_database, rightTable]], fields=[version, row_time])

```


## Flink 2.1.0

### Problem 1 - Cannot rewindow

The time attribute is not available:

```java
java.lang.RuntimeException: Error while applying rule StreamPhysicalWindowTableFunctionRule(in:LOGICAL,out:STREAM_PHYSICAL), args [rel#8888:FlinkLogicalTableFunctionScan.LOGICAL.any.None: 0.[NONE].[NONE].[NONE](input#0=RelSubset#8886,invocation=TUMBLE(TABLE(#0), DESCRIPTOR(_UTF-16LE'window_join_time'), 5000:INTERVAL SECOND),rowType=RecordType(VARCHAR(2147483647) l_version, VARCHAR(2147483647) r_version, TIMESTAMP_LTZ(3) *ROWTIME* window_join_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time))]
        at org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:250)
        at org.apache.calcite.plan.volcano.IterativeRuleDriver.drive(IterativeRuleDriver.java:59)
        at org.apache.calcite.plan.volcano.VolcanoPlanner.findBestExp(VolcanoPlanner.java:523)
        at org.apache.calcite.tools.Programs$RuleSetProgram.run(Programs.java:317)
        at org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgram.optimize(FlinkVolcanoProgram.scala:62)
        at org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.$anonfun$optimize$1(FlinkChainedProgram.scala:59)
        at scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:196)
        at scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:194)
        at scala.collection.Iterator.foreach(Iterator.scala:943)
        at scala.collection.Iterator.foreach$(Iterator.scala:943)
        at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
        at scala.collection.IterableLike.foreach(IterableLike.scala:74)
        at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
        at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
        at scala.collection.TraversableOnce.foldLeft(TraversableOnce.scala:199)
        at scala.collection.TraversableOnce.foldLeft$(TraversableOnce.scala:192)
        at scala.collection.AbstractTraversable.foldLeft(Traversable.scala:108)
        at org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.optimize(FlinkChainedProgram.scala:55)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.optimizeTree(StreamCommonSubGraphBasedOptimizer.scala:208)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.optimizeSinkBlocks(StreamCommonSubGraphBasedOptimizer.scala:87)
        at org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer.doOptimize(StreamCommonSubGraphBasedOptimizer.scala:123)
        at org.apache.flink.table.planner.plan.optimize.CommonSubGraphBasedOptimizer.optimize(CommonSubGraphBasedOptimizer.scala:87)
        at org.apache.flink.table.planner.delegation.PlannerBase.optimize(PlannerBase.scala:395)
        at org.apache.flink.table.planner.delegation.PlannerBase.getExplainGraphs(PlannerBase.scala:637)
        at org.apache.flink.table.planner.delegation.StreamPlanner.explain(StreamPlanner.scala:100)
        at org.apache.flink.table.planner.delegation.StreamPlanner.explain(StreamPlanner.scala:47)
        at org.apache.flink.table.api.internal.TableEnvironmentImpl.explainInternal(TableEnvironmentImpl.java:835)
        at org.apache.flink.table.api.internal.TableEnvironmentImpl.explainSql(TableEnvironmentImpl.java:815)
        at org.apache.flink.table.api.TableEnvironment.explainSql(TableEnvironment.java:1364)
        at com.example.SQLSubmitter.cannotAggregateWithNewWindow(SQLSubmitter.java:185)
        at com.example.SQLSubmitter.main(SQLSubmitter.java:146)
Caused by: org.apache.flink.table.api.ValidationException: The window function requires the timecol is a time attribute type, but is TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).
        at org.apache.flink.table.planner.plan.utils.WindowUtil$.validateTimeFieldWithTimeAttribute(WindowUtil.scala:184)
        at org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalWindowTableFunctionRule.convert(StreamPhysicalWindowTableFunctionRule.scala:60)
        at org.apache.calcite.rel.convert.ConverterRule.onMatch(ConverterRule.java:172)
        at org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:223)
        ... 30 more
```

### Problem 2 - Two inserts throw Calcite error

This time Flink doesn't throw a Calcite error. However, we see the same problem as before in the plan for the aggregate where it becomes a `GroupAggregate` with `changelogMode=[I,UA]`.

```text
== Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.joined_sink], fields=[l_version, r_version, joined_window_start, joined_window_end, joined_window_time])
+- LogicalProject(l_version=[$0], r_version=[$1], joined_window_start=[CAST($2):TIMESTAMP(9)], joined_window_end=[CAST($3):TIMESTAMP(9)], joined_window_time=[CAST($4):TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)])
   +- LogicalProject(l_version=[$0], r_version=[$5], window_join_start=[COALESCE($2, $7)], window_join_end=[COALESCE($3, $8)], window_join_time=[COALESCE($4, $9)])
      +- LogicalJoin(condition=[AND(=($0, $5), =($2, $7), =($3, $8), =($4, $9))], joinType=[full])
         :- LogicalProject(version=[$0], row_time=[$1], window_start=[$2], window_end=[$3], window_time=[$4])
         :  +- LogicalTableFunctionScan(invocation=[TUMBLE(TABLE(#0), DESCRIPTOR(_UTF-16LE'row_time'), 5000:INTERVAL SECOND)], rowType=[RecordType(VARCHAR(2147483647) version, TIMESTAMP_LTZ(3) *ROWTIME* row_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time)])
         :     +- LogicalProject(version=[$0], row_time=[$1])
         :        +- LogicalWatermarkAssigner(rowtime=[row_time], watermark=[-($1, 1000:INTERVAL SECOND)])
         :           +- LogicalTableScan(table=[[default_catalog, default_database, leftTable]])
         +- LogicalProject(version=[$0], row_time=[$1], window_start=[$2], window_end=[$3], window_time=[$4])
            +- LogicalTableFunctionScan(invocation=[TUMBLE(TABLE(#0), DESCRIPTOR(_UTF-16LE'row_time'), 5000:INTERVAL SECOND)], rowType=[RecordType(VARCHAR(2147483647) version, TIMESTAMP_LTZ(3) *ROWTIME* row_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time)])
               +- LogicalProject(version=[$0], row_time=[$1])
                  +- LogicalWatermarkAssigner(rowtime=[row_time], watermark=[-($1, 1000:INTERVAL SECOND)])
                     +- LogicalTableScan(table=[[default_catalog, default_database, rightTable]])

LogicalSink(table=[default_catalog.default_database.aggregate_sink], fields=[window_start, window_end, window_time, l_version_count, r_version_count])
+- LogicalProject(window_start=[CAST($0):TIMESTAMP(9)], window_end=[CAST($1):TIMESTAMP(9)], window_time=[CAST($2):TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)], l_version_count=[CAST($3):BIGINT], r_version_count=[CAST($4):BIGINT])
   +- LogicalAggregate(group=[{0, 1, 2}], l_version_count=[COUNT($3)], r_version_count=[COUNT($4)])
      +- LogicalProject(agg_window_start=[$2], agg_window_end=[$3], agg_window_time=[$4], l_version=[$0], r_version=[$1])
         +- LogicalProject(l_version=[$0], r_version=[$5], window_join_start=[COALESCE($2, $7)], window_join_end=[COALESCE($3, $8)], window_join_time=[COALESCE($4, $9)])
            +- LogicalJoin(condition=[AND(=($0, $5), =($2, $7), =($3, $8), =($4, $9))], joinType=[full])
               :- LogicalProject(version=[$0], row_time=[$1], window_start=[$2], window_end=[$3], window_time=[$4])
               :  +- LogicalTableFunctionScan(invocation=[TUMBLE(TABLE(#0), DESCRIPTOR(_UTF-16LE'row_time'), 5000:INTERVAL SECOND)], rowType=[RecordType(VARCHAR(2147483647) version, TIMESTAMP_LTZ(3) *ROWTIME* row_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time)])
               :     +- LogicalProject(version=[$0], row_time=[$1])
               :        +- LogicalWatermarkAssigner(rowtime=[row_time], watermark=[-($1, 1000:INTERVAL SECOND)])
               :           +- LogicalTableScan(table=[[default_catalog, default_database, leftTable]])
               +- LogicalProject(version=[$0], row_time=[$1], window_start=[$2], window_end=[$3], window_time=[$4])
                  +- LogicalTableFunctionScan(invocation=[TUMBLE(TABLE(#0), DESCRIPTOR(_UTF-16LE'row_time'), 5000:INTERVAL SECOND)], rowType=[RecordType(VARCHAR(2147483647) version, TIMESTAMP_LTZ(3) *ROWTIME* row_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time)])
                     +- LogicalProject(version=[$0], row_time=[$1])
                        +- LogicalWatermarkAssigner(rowtime=[row_time], watermark=[-($1, 1000:INTERVAL SECOND)])
                           +- LogicalTableScan(table=[[default_catalog, default_database, rightTable]])

== Optimized Physical Plan ==
Sink(table=[default_catalog.default_database.joined_sink], fields=[l_version, r_version, joined_window_start, joined_window_end, joined_window_time], changelogMode=[NONE])
+- Calc(select=[version AS l_version, version0 AS r_version, CAST(COALESCE(window_start, window_start0) AS TIMESTAMP(9)) AS joined_window_start, CAST(COALESCE(window_end, window_end0) AS TIMESTAMP(9)) AS joined_window_end, CAST(COALESCE(CAST(window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)), CAST(window_time0 AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)) AS joined_window_time], changelogMode=[I])
   +- WindowJoin(leftWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rightWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], joinType=[FullOuterJoin], where=[AND(=(version, version0), =(window_time, window_time0))], select=[version, window_start, window_end, window_time, version0, window_start0, window_end0, window_time0], changelogMode=[I])
      :- Exchange(distribution=[hash[version, window_time]], changelogMode=[I])
      :  +- Calc(select=[version, window_start, window_end, window_time], changelogMode=[I])
      :     +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])], changelogMode=[I])
      :        +- WatermarkAssigner(rowtime=[row_time], watermark=[-(row_time, 1000:INTERVAL SECOND)], changelogMode=[I])
      :           +- TableSourceScan(table=[[default_catalog, default_database, leftTable]], fields=[version, row_time], changelogMode=[I])
      +- Exchange(distribution=[hash[version, window_time]], changelogMode=[I])
         +- Calc(select=[version, window_start, window_end, window_time], changelogMode=[I])
            +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])], changelogMode=[I])
               +- WatermarkAssigner(rowtime=[row_time], watermark=[-(row_time, 1000:INTERVAL SECOND)], changelogMode=[I])
                  +- TableSourceScan(table=[[default_catalog, default_database, rightTable]], fields=[version, row_time], changelogMode=[I])

Sink(table=[default_catalog.default_database.aggregate_sink], fields=[window_start, window_end, window_time, l_version_count, r_version_count], changelogMode=[NONE])
+- Calc(select=[CAST(agg_window_start AS TIMESTAMP(9)) AS window_start, CAST(agg_window_end AS TIMESTAMP(9)) AS window_end, CAST(agg_window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)) AS window_time, CAST(l_version_count AS BIGINT) AS l_version_count, CAST(r_version_count AS BIGINT) AS r_version_count], changelogMode=[I,UA])
   +- GroupAggregate(groupBy=[agg_window_start, agg_window_end, agg_window_time], select=[agg_window_start, agg_window_end, agg_window_time, COUNT(l_version) AS l_version_count, COUNT(r_version) AS r_version_count], changelogMode=[I,UA])
      +- Exchange(distribution=[hash[agg_window_start, agg_window_end, agg_window_time]], changelogMode=[I])
         +- Calc(select=[COALESCE(window_start, window_start0) AS agg_window_start, COALESCE(window_end, window_end0) AS agg_window_end, COALESCE(CAST(window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)), CAST(window_time0 AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) AS agg_window_time, version AS l_version, version0 AS r_version], changelogMode=[I])
            +- WindowJoin(leftWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rightWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], joinType=[FullOuterJoin], where=[AND(=(version, version0), =(window_time, window_time0))], select=[version, window_start, window_end, window_time, version0, window_start0, window_end0, window_time0], changelogMode=[I])
               :- Exchange(distribution=[hash[version, window_time]], changelogMode=[I])
               :  +- Calc(select=[version, window_start, window_end, window_time], changelogMode=[I])
               :     +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])], changelogMode=[I])
               :        +- WatermarkAssigner(rowtime=[row_time], watermark=[-(row_time, 1000:INTERVAL SECOND)], changelogMode=[I])
               :           +- TableSourceScan(table=[[default_catalog, default_database, leftTable]], fields=[version, row_time], changelogMode=[I])
               +- Exchange(distribution=[hash[version, window_time]], changelogMode=[I])
                  +- Calc(select=[version, window_start, window_end, window_time], changelogMode=[I])
                     +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])], changelogMode=[I])
                        +- WatermarkAssigner(rowtime=[row_time], watermark=[-(row_time, 1000:INTERVAL SECOND)], changelogMode=[I])
                           +- TableSourceScan(table=[[default_catalog, default_database, rightTable]], fields=[version, row_time], changelogMode=[I])

== Optimized Execution Plan ==
WindowJoin(leftWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rightWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], joinType=[FullOuterJoin], where=[((version = version0) AND (window_time = window_time0))], select=[version, window_start, window_end, window_time, version0, window_start0, window_end0, window_time0])(reuse_id=[1])
:- Exchange(distribution=[hash[version, window_time]])
:  +- Calc(select=[version, window_start, window_end, window_time])
:     +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])])
:        +- WatermarkAssigner(rowtime=[row_time], watermark=[(row_time - 1000:INTERVAL SECOND)])
:           +- TableSourceScan(table=[[default_catalog, default_database, leftTable]], fields=[version, row_time])
+- Exchange(distribution=[hash[version, window_time]])
   +- Calc(select=[version, window_start, window_end, window_time])
      +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])])
         +- WatermarkAssigner(rowtime=[row_time], watermark=[(row_time - 1000:INTERVAL SECOND)])
            +- TableSourceScan(table=[[default_catalog, default_database, rightTable]], fields=[version, row_time])

Sink(table=[default_catalog.default_database.joined_sink], fields=[l_version, r_version, joined_window_start, joined_window_end, joined_window_time])
+- Calc(select=[version AS l_version, version0 AS r_version, CAST(COALESCE(window_start, window_start0) AS TIMESTAMP(9)) AS joined_window_start, CAST(COALESCE(window_end, window_end0) AS TIMESTAMP(9)) AS joined_window_end, CAST(COALESCE(CAST(window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)), CAST(window_time0 AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)) AS joined_window_time])
   +- Reused(reference_id=[1])

Sink(table=[default_catalog.default_database.aggregate_sink], fields=[window_start, window_end, window_time, l_version_count, r_version_count])
+- Calc(select=[CAST(agg_window_start AS TIMESTAMP(9)) AS window_start, CAST(agg_window_end AS TIMESTAMP(9)) AS window_end, CAST(agg_window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)) AS window_time, CAST(l_version_count AS BIGINT) AS l_version_count, CAST(r_version_count AS BIGINT) AS r_version_count])
   +- GroupAggregate(groupBy=[agg_window_start, agg_window_end, agg_window_time], select=[agg_window_start, agg_window_end, agg_window_time, COUNT(l_version) AS l_version_count, COUNT(r_version) AS r_version_count])
      +- Exchange(distribution=[hash[agg_window_start, agg_window_end, agg_window_time]])
         +- Calc(select=[COALESCE(window_start, window_start0) AS agg_window_start, COALESCE(window_end, window_end0) AS agg_window_end, COALESCE(CAST(window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)), CAST(window_time0 AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) AS agg_window_time, version AS l_version, version0 AS r_version])
            +- Reused(reference_id=[1])
```

### Problem 3 - Planner not re-using window

Same issue with the `GroupAggregate` and `changelogMode=[I, UA]`:

```text
== Abstract Syntax Tree ==
LogicalProject(agg_window_start=[$0], agg_window_end=[$1], agg_window_time=[$2], l_version_count=[$3], r_version_count=[$4])
+- LogicalAggregate(group=[{0, 1, 2}], l_version_count=[COUNT($3)], r_version_count=[COUNT($4)])
   +- LogicalProject(agg_window_start=[$2], agg_window_end=[$3], agg_window_time=[$4], l_version=[$0], r_version=[$1])
      +- LogicalProject(l_version=[$0], r_version=[$5], window_join_start=[COALESCE($2, $7)], window_join_end=[COALESCE($3, $8)], window_join_time=[COALESCE($4, $9)])
         +- LogicalJoin(condition=[AND(=($0, $5), =($2, $7), =($3, $8), =($4, $9))], joinType=[full])
            :- LogicalProject(version=[$0], row_time=[$1], window_start=[$2], window_end=[$3], window_time=[$4])
            :  +- LogicalTableFunctionScan(invocation=[TUMBLE(TABLE(#0), DESCRIPTOR(_UTF-16LE'row_time'), 5000:INTERVAL SECOND)], rowType=[RecordType(VARCHAR(2147483647) version, TIMESTAMP_LTZ(3) *ROWTIME* row_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time)])
            :     +- LogicalProject(version=[$0], row_time=[$1])
            :        +- LogicalWatermarkAssigner(rowtime=[row_time], watermark=[-($1, 1000:INTERVAL SECOND)])
            :           +- LogicalTableScan(table=[[default_catalog, default_database, leftTable]])
            +- LogicalProject(version=[$0], row_time=[$1], window_start=[$2], window_end=[$3], window_time=[$4])
               +- LogicalTableFunctionScan(invocation=[TUMBLE(TABLE(#0), DESCRIPTOR(_UTF-16LE'row_time'), 5000:INTERVAL SECOND)], rowType=[RecordType(VARCHAR(2147483647) version, TIMESTAMP_LTZ(3) *ROWTIME* row_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP_LTZ(3) *ROWTIME* window_time)])
                  +- LogicalProject(version=[$0], row_time=[$1])
                     +- LogicalWatermarkAssigner(rowtime=[row_time], watermark=[-($1, 1000:INTERVAL SECOND)])
                        +- LogicalTableScan(table=[[default_catalog, default_database, rightTable]])

== Optimized Physical Plan ==
GroupAggregate(groupBy=[agg_window_start, agg_window_end, agg_window_time], select=[agg_window_start, agg_window_end, agg_window_time, COUNT(l_version) AS l_version_count, COUNT(r_version) AS r_version_count], changelogMode=[I,UA])
+- Exchange(distribution=[hash[agg_window_start, agg_window_end, agg_window_time]], changelogMode=[I])
   +- Calc(select=[COALESCE(window_start, window_start0) AS agg_window_start, COALESCE(window_end, window_end0) AS agg_window_end, COALESCE(CAST(window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)), CAST(window_time0 AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) AS agg_window_time, version AS l_version, version0 AS r_version], changelogMode=[I])
      +- WindowJoin(leftWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rightWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], joinType=[FullOuterJoin], where=[AND(=(version, version0), =(window_time, window_time0))], select=[version, window_start, window_end, window_time, version0, window_start0, window_end0, window_time0], changelogMode=[I])
         :- Exchange(distribution=[hash[version, window_time]], changelogMode=[I])
         :  +- Calc(select=[version, window_start, window_end, window_time], changelogMode=[I])
         :     +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])], changelogMode=[I])
         :        +- WatermarkAssigner(rowtime=[row_time], watermark=[-(row_time, 1000:INTERVAL SECOND)], changelogMode=[I])
         :           +- TableSourceScan(table=[[default_catalog, default_database, leftTable]], fields=[version, row_time], changelogMode=[I])
         +- Exchange(distribution=[hash[version, window_time]], changelogMode=[I])
            +- Calc(select=[version, window_start, window_end, window_time], changelogMode=[I])
               +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])], changelogMode=[I])
                  +- WatermarkAssigner(rowtime=[row_time], watermark=[-(row_time, 1000:INTERVAL SECOND)], changelogMode=[I])
                     +- TableSourceScan(table=[[default_catalog, default_database, rightTable]], fields=[version, row_time], changelogMode=[I])

== Optimized Execution Plan ==
GroupAggregate(groupBy=[agg_window_start, agg_window_end, agg_window_time], select=[agg_window_start, agg_window_end, agg_window_time, COUNT(l_version) AS l_version_count, COUNT(r_version) AS r_version_count])
+- Exchange(distribution=[hash[agg_window_start, agg_window_end, agg_window_time]])
   +- Calc(select=[COALESCE(window_start, window_start0) AS agg_window_start, COALESCE(window_end, window_end0) AS agg_window_end, COALESCE(CAST(window_time AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)), CAST(window_time0 AS TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) AS agg_window_time, version AS l_version, version0 AS r_version])
      +- WindowJoin(leftWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], rightWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[5 s])], joinType=[FullOuterJoin], where=[((version = version0) AND (window_time = window_time0))], select=[version, window_start, window_end, window_time, version0, window_start0, window_end0, window_time0])
         :- Exchange(distribution=[hash[version, window_time]])
         :  +- Calc(select=[version, window_start, window_end, window_time])
         :     +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])])
         :        +- WatermarkAssigner(rowtime=[row_time], watermark=[(row_time - 1000:INTERVAL SECOND)])
         :           +- TableSourceScan(table=[[default_catalog, default_database, leftTable]], fields=[version, row_time])
         +- Exchange(distribution=[hash[version, window_time]])
            +- Calc(select=[version, window_start, window_end, window_time])
               +- WindowTableFunction(window=[TUMBLE(time_col=[row_time], size=[5 s])])
                  +- WatermarkAssigner(rowtime=[row_time], watermark=[(row_time - 1000:INTERVAL SECOND)])
                     +- TableSourceScan(table=[[default_catalog, default_database, rightTable]], fields=[version, row_time])
```
