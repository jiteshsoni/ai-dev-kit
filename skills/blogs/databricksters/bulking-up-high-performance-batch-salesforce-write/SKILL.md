---
name: bulking-up-high-performance-batch-salesforce-write
description: 
---

# Bulking Up: High-Performance Batch Salesforce Writes with PySpark

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/bulking-up-high-performance-batch

## Tags
ai, spark, data-engineering, performance, databricks, clustering

## Full Content

# Bulking Up: High-Performance Batch Salesforce Writes with PySpark

**Source:** https://www.databricksters.com/p/bulking-up-high-performance-batch

**Blog:** Databricks

---



Bulking Up: High-Performance Batch Salesforce Writes with PySpark

Databricksters

Subscribe

Sign in

Bulking Up: High-Performance Batch Salesforce Writes with PySpark

An example of reverse-ETL to Salesforce with parent/child object upserts. 

Neil Wilson

Dec 16, 2025

3

1

Share

The 

Python Data Source API

 allows Spark developers to easily define custom sources and sinks for their Spark jobs 

written in Python

. One of the most commonly requested custom sinks I’ve seen in the field is writing data back to Salesforce. 

I’ve written an example custom Salesforce batch writer 

here

. It is designed to support uploads via the Salesforce Bulk API v1.0 or v2.0, but this guide will focus on v2.0. I strongly recommend using 2.0 for newly introduced features discussed below.

This code is a robust example, and is not intended to be copy/pasted into production.

Background on Salesforce Bulk API 2.0

Before diving into the code, let’s discuss how the Bulk API 2.0 works and how our writer can interact with it. A “job” is the unit of work in the Salesforce Bulk API. In v1 of the Salesforce API, users had to create a job and then manually break that job into chunks of 10,000 records and track each batch individually. In v2, you simply submit your data via a job and it handles batches and retries automatically. The v1 limit for upload was 10MB per 

batch

, while the v2 limit is 150MB per 

job

.

But what if my DataFrame is larger than 150MB, do I have to manually slice the data into sub 150MB chunks and iteratively submit multiple Bulk API jobs? This is where the power of Spark shines, though it comes with a requirement. Spark will automatically parallelize the work into multiple Salesforce “jobs”, but it won’t automatically slice large partitions. You must explicitly control the partition size (using 

repartition

) to ensure you don’t send a chunk larger than 150MB.

Background on Spark writes

In Spark, when 

df.write

 is called, the driver calls the writer() method for the DataSource object in question. In our Salesforce example, calling this writer method will result in the instantiation of our SalesforceBatchWriter object.

def writer(self, schema: StructType, overwrite: bool):
 “”“Create a writer instance for the given schema.”“”
 return SalesforceBatchWriter(self.options, schema)

class SalesforceBatchWriter(DataSourceWriter):
 “”“
 DataSourceWriter implementation for Salesforce Bulk API operations.
 Handles both authentication and bulk data upload to Salesforce objects.
 “”“

 def __init__(self, options: Dict[str, str], schema: StructType):
 self.options = options
 self.schema = schema

Spark then looks at the write() method within your DataSourceWriter (recall above, our SalesforceBatchWriter inherited from the DataSourceWriter class), serializes (pickles) this write method, and creates a task for every partition in your DataFrame. It then sends these tasks to the Executors. Simply put, in Spark, each partition of data receives its own write task. This means if we have an extremely large DataFrame, so long as its partitioned and each partition is under 150MB, each partition will receive its own set of write instructions and can be submitted in parallel as multiple Salesforce Bulk API v2 jobs.

Spark to Salesforce Writer

Now that we understand a bit more about how Spark and the Salesforce Bulk API can interact, let’s dive into our custom writer implementation.

def write(self, rows: Iterator[Row]) -> SalesforceCommitMessage:
 “”“
 Write rows to Salesforce using Bulk API.

 Args:
 rows: Iterator of PySpark Row objects to write

 Returns:
 SalesforceCommitMessage with write statistics
 “”“
 # Import inside method to meet serialization requirements on executors
 from simple_salesforce import Salesforce, SalesforceAuthenticationFailed

 ctx = TaskContext.get()
 partition_id = ctx.partitionId()

 username = self.options.get(”username”)
 password = self.options.get(”password”)
 security_token = self.options.get(”security_token”)
 sobject = self.options.get(”sobject”)
 instance_url = (self.options.get(”instance_url”) or “”).strip()
 domain = (self.options.get(”domain”) or “login”).strip()
 api_version = self.options.get(”api_version”, “1”) # “1” (Bulk V1) or “2” (Bulk V2)

 if not all([username, password, security_token, sobject]):
 raise ValueError(”Missing required Salesforce options: ‘username’, ‘password’, ‘security_token’, ‘sobject’”)

 # Collect iterator to a list of dicts for bulk insert
 data_to_upload: List[Dict[str, Any]] = [row.asDict() for row in rows]

 if not data_to_upload:
 print(f”Partition {partition_id}: No rows to write.”)
 return SalesforceCommitMessage(partition_id=partition_id, records_written=0, errors=0)

 try:
 if instance_url:
 sf = Salesforce(
 username=username,
 password=password,
 security_token=security_token,
 instance_url=instance_url,
 )
 else:
 sf = Salesforce(
 username=username,
 password=password,
 security_token=security_token,
 domain=domain,
 )

The 

data_to_upload 

is an important array variable in this writer. To understand what it is doing, recall that write() is pickled and sent as a task for 

each partition

 in your DataFrame. This means that the 

rows: Iterator[Row]

 argument that data_to_upload is iterating over is the set of rows for 

one partition

. This is turning each row into a key/value dict for compatibility with the Salesforce API and storing them in a Python List. It’s extremely important to recognize that creating this List of Dict objects is 

materializing the entire partition in the memory of your executor

. This must be done, as the Salesforce API requires the full payload to be constructed before sending. If this is done on a partition that is too large, you will face Out of Memory (OOM) issues. If this is the case, logic could be added to chunk your partition, but that would mean additional Bulk API jobs and additional API calls in Salesforce.

The next step is to actually perform the bulk insert of these records and track the status of the job returned by the API.

if api_version == “2”:
 job_summaries = self._bulk2_insert(sf, sobject, data_to_upload, batch_size)

 for summary in job_summaries:
 num_processed = int(summary.get(”numberRecordsProcessed”, 0))
 num_failed = int(summary.get(”numberRecordsFailed”, 0))
 success += max(0, num_processed - num_failed)
 errors += max(0, num_failed)

 print(f”Partition {partition_id} job {summary.get(’job_id’)} summary: “
 f”processed={num_processed}, failed={num_failed}, total={summary.get(’numberRecordsTotal’)}”)

 if num_failed > 0 and summary.get(”job_id”):
 try:
 failed_csv = sf.bulk2.__getattr__(sobject).get_failed_records(summary[”job_id”])
 if isinstance(failed_csv, str):
 preview = failed_csv[:2048]
 print(f”Partition {partition_id} job {summary[’job_id’]} failed records (preview):\n”
 f”{preview}”)

 # Store the first error preview to bubble up to the driver
 if not failed_records_preview:
 failed_records_preview = preview
 except Exception as e:
 print(f”Partition {partition_id}: Could not retrieve failed records: {e}”)

job_summaries performs the insert via the _bulk2_call method, and will contain job_status information for the submitted job once it completes. _bulk2_call is an internal method that uses the 

simple-salesforce insert method

 and attempts to upload the data in the v2 syntax “insert(records=records)” or if that fails attempts the v1 syntax “insert(data=records)”. It also contains logic to allow for performing upserts instead of inserts.

def _bulk2_call(
 self,
 sf,
 sobject: str,
 operation: str,
 records: List[Dict[str, Any]],
 batch_size: int,
 upsert_key: Optional[str] = None,
 ):
 “”“Execute a Bulk API 2.0 job for the given operation and records.

 Uses the modern signature if available and falls back to legacy signature



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
