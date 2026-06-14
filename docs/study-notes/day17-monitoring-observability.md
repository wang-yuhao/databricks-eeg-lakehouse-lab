# Day 17: Monitoring and Observability

## Overview
Study notes on monitoring, observability, and debugging in Databricks for Professional certification.

## Key Concepts

### 1. Spark UI
- **Jobs Tab**: View all Spark jobs and their stages
- **Stages Tab**: Detailed stage information and task metrics
- **Storage Tab**: Cached RDD/DataFrame information
- **Environment Tab**: Spark configuration and properties
- **Executors Tab**: Executor metrics and resource usage
- **SQL Tab**: DataFrame operations and query plans

### 2. Cluster Metrics
- CPU utilization
- Memory usage (heap and off-heap)
- Disk I/O statistics
- Network throughput
- Garbage collection metrics
- Shuffle metrics

### 3. Databricks Metrics
- **Ganglia Metrics**: Legacy cluster monitoring
- **Metrics API**: Programmatic access to metrics
- **Driver and Executor Logs**: Application logs
- **Event Logs**: Spark event history

### 4. Query Performance Analysis
- Query execution plans (logical and physical)
- Adaptive Query Execution (AQE)
- Query optimization techniques
- Broadcast joins vs. shuffle joins
- Predicate pushdown verification

### 5. Observability Tools
- **Databricks SQL Query History**: Track query performance
- **Job Runs**: Monitor pipeline executions
- **Workflow Runs**: Track orchestrated jobs
- **Delta Table History**: Version control and time travel
- **Lineage Tracking**: Data flow visualization

## Monitoring Best Practices

### Performance Monitoring
- Monitor task duration and identify stragglers
- Check for data skew in partitions
- Analyze shuffle read/write operations
- Review GC time percentage
- Monitor executor memory pressure

### Resource Optimization
- Right-size cluster configurations
- Use autoscaling appropriately
- Monitor spot instance interruptions
- Track cluster utilization rates
- Optimize executor and driver memory

### Debugging Techniques
1. **Check Spark UI for bottlenecks**
   - Long-running tasks
   - Failed tasks and retries
   - Data skew indicators

2. **Analyze logs**
   - Driver logs for application errors
   - Executor logs for task failures
   - System logs for infrastructure issues

3. **Query optimization**
   - Review execution plans
   - Check for full table scans
   - Verify partition pruning
   - Validate predicate pushdown

## Common Performance Issues

### Data Skew
- **Symptoms**: Few tasks take much longer than others
- **Solutions**: Salting keys, repartitioning, broadcast joins

### Small Files Problem
- **Symptoms**: Many small files in storage
- **Solutions**: OPTIMIZE command, Auto Optimize, file compaction

### Shuffle Overhead
- **Symptoms**: High shuffle read/write times
- **Solutions**: Reduce shuffles, increase shuffle partitions, use broadcast

### Memory Issues
- **Symptoms**: OOM errors, high GC time
- **Solutions**: Increase executor memory, tune spark.memory.fraction

## Logging and Diagnostics

### Log Levels
```python
# Set log level
spark.sparkContext.setLogLevel("WARN")
# Options: ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF
```

### Custom Logging
```python
import logging
logger = logging.getLogger(__name__)
logger.info("Custom log message")
```

### Accessing Logs
- Cluster event logs: `/databricks/driver/logs`
- Download logs from cluster UI
- Stream logs via Databricks CLI

## Delta Lake Monitoring

### Table Metrics
```sql
DESCRIBE DETAIL my_table;
DESCRIBE HISTORY my_table;
```

### Optimize Operations
```sql
OPTIMIZE my_table;
VACUUM my_table RETAIN 168 HOURS;
```

## Practice Questions
1. How do you identify data skew using Spark UI?
2. What metrics indicate shuffle overhead?
3. How can you monitor Delta table performance over time?
4. What's the difference between logical and physical query plans?
5. How do you debug OOM errors in Spark applications?

## References
- Databricks Monitoring and Logging Guide
- Apache Spark Performance Tuning
- Delta Lake Best Practices
