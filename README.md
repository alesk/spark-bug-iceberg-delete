# Spark Bug - Iceberg Table `MERGE INTO` Statement with Null Values

This repository demonstrates a bug in Spark when using the `MERGE INTO ... WHEN MATCHED THEN DELETE` statement on an
Iceberg table. The bug occurs when deleting rows from sorted Iceberg table, and one of the remaining rows contains a null
value in some column. 

## Bug Details

- Spark Version: 3.4.1
- Iceberg Version: 1.3
- Works fine with Iceberg 1.3 and Spark 3.3.2
- Bug is caused by the `MERGE INTO ... WHEN MATCHED THEN DELETE` statement deleting rows from the Iceberg table.
- Bug is manifested as an exception thrown by the Spark job

```text
java.lang.NullPointerException: Cannot invoke "org.apache.spark.unsafe.types.UTF8String.getBaseObject()" because "input" is null
```

This is the whole stacktrace

```java
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:161)
        at org.apache.spark.scheduler.Task.run(Task.scala:139)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:554)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1529)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:557)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
        at java.base/java.lang.Thread.run(Thread.java:833)

Driver stacktrace:
        at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2785)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2721)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2720)
        at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
        at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
        at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
        at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2720)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1206)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1206)
        at scala.Option.foreach(Option.scala:407)
        at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1206)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2984)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2923)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2912)
        at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
        at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:971)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2263)
        at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2(WriteToDataSourceV2Exec.scala:408)
        at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2$(WriteToDataSourceV2Exec.scala:382)
        at org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec.writeWithV2(WriteToDataSourceV2Exec.scala:294)
        at org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec.run(WriteToDataSourceV2Exec.scala:360)
        at org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec.run$(WriteToDataSourceV2Exec.scala:359)
        at org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec.run(WriteToDataSourceV2Exec.scala:294)
        at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result$lzycompute(V2CommandExec.scala:43)
        at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result(V2CommandExec.scala:43)
        at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.executeCollect(V2CommandExec.scala:49)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:98)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:118)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:195)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:103)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:827)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:65)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:94)
        at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:512)
        at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:104)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:512)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:31)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:31)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:31)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:488)
        at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:94)
        at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:81)
        at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:79)
        at org.apache.spark.sql.Dataset.<init>(Dataset.scala:219)
        at org.apache.spark.sql.Dataset$.$anonfun$ofRows$2(Dataset.scala:99)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:827)
        at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:96)
        at org.apache.spark.sql.SparkSession.$anonfun$sql$1(SparkSession.scala:640)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:827)
        at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:630)
        at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:662)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:76)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:52)
        at java.base/java.lang.reflect.Method.invoke(Method.java:577)
        at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
        at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
        at py4j.Gateway.invoke(Gateway.java:282)
        at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
        at py4j.commands.CallCommand.execute(CallCommand.java:79)
        at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
        at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
        at java.base/java.lang.Thread.run(Thread.java:833)
Caused by: java.lang.NullPointerException
        at org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter.write(UnsafeWriter.java:110)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage3.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:760)
        at org.apache.spark.sql.execution.datasources.v2.WritingSparkTask.$anonfun$run$1(WriteToDataSourceV2Exec.scala:464)
        at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1563)
        at org.apache.spark.sql.execution.datasources.v2.WritingSparkTask.run(WriteToDataSourceV2Exec.scala:509)
        at org.apache.spark.sql.execution.datasources.v2.WritingSparkTask.run$(WriteToDataSourceV2Exec.scala:448)
        at org.apache.spark.sql.execution.datasources.v2.DataWritingSparkTask$.run(WriteToDataSourceV2Exec.scala:514)
        at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.$anonfun$writeWithV2$2(WriteToDataSourceV2Exec.scala:411)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:92)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:161)
        at org.apache.spark.scheduler.Task.run(Task.scala:139)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:554)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1529)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:557)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
        ... 1 more
```


## Bug Reproduction

1. Make sure you have python 3.7+ installed.
2. Run the provided script `run.sh` to run the Spark job with the bug.

## Bug Workaround

There is a workaround to this bug. Instead of using the `MERGE INTO ... WHEN MATCHED THEN DELETE` statement, you can use
the `DELETE FROM ... WHERE EXISTS` statement to achieve the same functionality without encountering the bug.

## Running on a Different Spark/Iceberg Version

If you want to run the code on different spark configuration, you can use the following command:

In `run.sh` change the line 7 to install the different version of pyspark, i.e. `pyspark==3.3.2`

In min_reproduce, change the line 18 installing different version of iceberg runtime, for example

```
'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0'
```

## Repository Contents

The repository contains the following files:

1. `min_reproduce.py`: Python script to reproduce the bug.
2. `run.sh`: Bash script to execute the bug reproduction.
4. `README.md`: This README file.

## Actions taken

An [issue #8126](https://github.com/apache/iceberg/issues/8126) has been submitted to the Apache Iceberg's GitHub
repository.