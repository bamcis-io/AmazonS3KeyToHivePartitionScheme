# BAMCIS S3 Key to Hive Partition Scheme
An AWS Serverless Application that converts S3 keys written in a value1/value2/value3/filename.txt format to key1=value1/key2=value2/key3=value3/filename.txt format used by Hive for partitions.

## Table of Contents
- [Usage](#usage)
- [Options](#options)
- [Revision History](#revision-history)

## Usage
The CloudFormation template creates a Lambda function that reads S3 objects from an event notification, copies them with a new key name and new bucket, and optionally deletes the original. **DO NOT SPECIFY THE SAME BUCKET AS THE SOURCE FOR THE DESTINATION, IT WILL CAUSE AN INFINITE LOOP WITH YOUR LAMBDA FUNCTION NOTIFICATIONS**.

## Options

The source and destination buckets can either be pre-existing or the script can create them. If the source S3 Bucket is pre-existing, you must use the SNS event type and provide the SNS topic ARN that is set to trigger the Lambda function (and set the `EventNotificationMethod` to `sns`). If you create the buckets with the template, you can choose to either use SNS to trigger the Lambda function or let S3 trigger the Lambda function directly.

The `NewKeyPattern` is the string that is provided to the `String.Format()` method. The other argument provided to `String.Format()` is the original S3 key split by the `/` character (resulting in an array of the "folder" names with the last element being the actual file name). This allows you to add in key values each partition, i.e. instead of value1/value2/file.txt, you can provide the new key pattern of `key1={0}/key2={1}/{2}` which will provide the partition naming convention Hive uses.

## Revision History

### 1.0.1
Minor bug fixes.

### 1.0.0
Initial release of the application.
