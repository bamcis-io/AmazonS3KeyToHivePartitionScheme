using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Lambda.SNSEvents;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleNotificationService;
using BAMCIS.AWSLambda.Common;
using BAMCIS.AWSLambda.Common.Events.SNS;
using Newtonsoft.Json;
using System;
using System.Net;
using System.Threading.Tasks;
using static Amazon.Lambda.SNSEvents.SNSEvent;
using static Amazon.S3.Util.S3EventNotification;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace BAMCIS.LambdaFunctions.AmazonS3KeyToHivePartitionScheme
{
    public class Entrypoint
    {
        #region Private Fields

        private static IAmazonS3 _S3Client;
        private static IAmazonSimpleNotificationService _SNSClient;
        private static string _SNS_FAILURE_TOPIC_ARN;

        #endregion

        #region Constructors

        /// <summary>
        /// Static constructor to initialize static fields
        /// </summary>
        static Entrypoint()
        {
            _S3Client = new AmazonS3Client();
            _SNSClient = new AmazonSimpleNotificationServiceClient();
            _SNS_FAILURE_TOPIC_ARN = Environment.GetEnvironmentVariable("SNS_FAILURE_TOPIC_ARN");
        }

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Entrypoint()
        {

        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Entrypoint for the Lambda function 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task ExecSNS(SNSEvent request, ILambdaContext context)
        {
            string DestinationBucket;
            if (String.IsNullOrEmpty(DestinationBucket = await GetDestinationBucket(context)))
            {
                return;
            }

            string PrefixPattern;
            if (String.IsNullOrEmpty(PrefixPattern = await GetPrefixPattern(context)))
            {
                return;
            }

            bool DeleteSource = false;
            Boolean.TryParse(Environment.GetEnvironmentVariable("DELETE_SOURCE"), out DeleteSource);

            foreach (SNSRecord Record in request.Records)
            {
                try
                {
                    string Message = Record.Sns.Message;

                    if (S3TestMessage.IsTestMessage(Message))
                    {
                        context.LogInfo($"Processing test event from SNS: {Message}");
                        return;
                    }

                    SNSS3RecordSet RecordSet = JsonConvert.DeserializeObject<SNSS3RecordSet>(Message);

                    foreach (SNSS3Record S3Record in RecordSet.Records)
                    {
                        try
                        {
                            string Key = S3Record.S3.Object.Key;
                            string Bucket = S3Record.S3.Bucket.Name;

                            CopyObjectResponse Response = await Copy(Bucket, Key, DestinationBucket, PrefixPattern, context);
                        }
                        catch (AggregateException e)
                        {
                            context.LogError(e);

                            await SendFailureSNS(e.InnerException, context);
                        }
                        catch (Exception e)
                        {
                            context.LogError(e);

                            await SendFailureSNS(e, context);
                        }
                    }
                }
                catch (AggregateException e)
                {
                    context.LogError(e);

                    await SendFailureSNS(e.InnerException, context);
                }
                catch (Exception e)
                {
                    context.LogError(e);

                    await SendFailureSNS(e, context);
                }
            }
        }

        /// <summary>
        /// Entrypoint for the Lambda function 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task ExecS3(S3Event request, ILambdaContext context)
        {
            string DestinationBucket;
            if (String.IsNullOrEmpty(DestinationBucket = await GetDestinationBucket(context)))
            {
                return;
            }

            string PrefixPattern;
            if (String.IsNullOrEmpty(PrefixPattern = await GetPrefixPattern(context)))
            {
                return;
            }

            bool DeleteSource = false;
            Boolean.TryParse(Environment.GetEnvironmentVariable("DELETE_SOURCE"), out DeleteSource);

            foreach (S3EventNotificationRecord Record in request.Records)
            {
                try
                {
                    string Key = Record.S3.Object.Key;
                    string Bucket = Record.S3.Bucket.Name;

                    CopyObjectResponse Response = await Copy(Bucket, Key, DestinationBucket, PrefixPattern, context);
                }
                catch (AggregateException e)
                {
                    context.LogError(e);

                    await SendFailureSNS(e.InnerException, context);
                }
                catch (Exception e)
                {
                    context.LogError(e);

                    await SendFailureSNS(e, context);
                }
            }
        }

        #endregion

        #region Private Methods

        private static async Task<string> GetDestinationBucket(ILambdaContext context)
        {
            string DestinationBucket = Environment.GetEnvironmentVariable("S3_BUCKET");

            if (String.IsNullOrEmpty(DestinationBucket))
            {
                string Msg = "The environment variable S3_BUCKET for the destination was not set.";
                context.LogError(Msg);

                await SendFailureSNS(Msg, context);
            }

            return DestinationBucket;
        }

        private static async Task<string> GetPrefixPattern(ILambdaContext context)
        {
            string PrefixPattern = Environment.GetEnvironmentVariable("PREFIX_PATTERN");

            if (String.IsNullOrEmpty(PrefixPattern))
            {
                string Msg = "The environment variable PREFIX_PATTERN was not set.";
                context.LogError(Msg);

                await SendFailureSNS(Msg, context);
            }

            return PrefixPattern;
        }

        private static async Task<CopyObjectResponse> Copy(string sourceBucket, string sourceKey, string destinationBucket, string prefixPattern, ILambdaContext context)
        {
            // The S3 key prefixes are separated with a forward slash
            string[] Parts = sourceKey.Split("/");
            string DestinationKey = String.Format(prefixPattern, Parts);
            string DestinationUri = $"s3://{destinationBucket}/{DestinationKey}";

            context.LogInfo($"Using destination: {DestinationUri}");

            GetObjectTaggingRequest TagRequest = new GetObjectTaggingRequest()
            {
                BucketName = sourceBucket,
                Key = sourceKey
            };

            GetObjectTaggingResponse TagResponse = await _S3Client.GetObjectTaggingAsync(TagRequest);

            CopyObjectRequest CopyRequest = new CopyObjectRequest()
            {
                DestinationBucket = destinationBucket,
                SourceBucket = sourceBucket,
                SourceKey = sourceKey,
                DestinationKey = DestinationKey,
                TagSet = TagResponse.Tagging
            };

            CopyObjectResponse Response =  await _S3Client.CopyOrMoveObjectAsync(CopyRequest, true);

            if (Response.HttpStatusCode == HttpStatusCode.OK)
            {
                context.LogInfo($"Successfully moved s3://{sourceBucket}/{sourceKey} to {DestinationUri}.");
            }
            else
            {
                context.LogError($"Unsuccessful copy of s3://{sourceBucket}/{sourceKey} to {DestinationUri} : ${(int)Response.HttpStatusCode}");
            }

            return Response;
        }

        private static async Task SendFailureSNS(string message, ILambdaContext context)
        {
            if (!String.IsNullOrEmpty(_SNS_FAILURE_TOPIC_ARN))
            {
                await _SNSClient.PublishAsync(_SNS_FAILURE_TOPIC_ARN, message, $"Lambda Execution Failure: {context.InvokedFunctionArn}");
            }
        }

        private static async Task SendFailureSNS(Exception e, ILambdaContext context)
        {
            await SendFailureSNS($"{e.Message}\n{e.StackTrace}", context);
        }

        #endregion
    }
}