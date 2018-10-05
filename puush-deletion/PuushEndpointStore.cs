using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace puush_deletion
{
    class PuushEndpointStore
    {
        private readonly AmazonS3Client client;
        private readonly int pool;
        private readonly string bucket;

        public PuushEndpointStore(int pool, string key, string secret, string bucket, string endpoint = null)
        {
            if (endpoint != null)
                client = new AmazonS3Client(new BasicAWSCredentials(key, secret), new AmazonS3Config
                {
                    ServiceURL = endpoint,
                    UseHttp = true,
                    ForcePathStyle = true
                });
            else
                client = new AmazonS3Client(new BasicAWSCredentials(key, secret), RegionEndpoint.USWest2);

            this.pool = pool;
            this.bucket = bucket;

            Console.Write($"Checking connection to endpoint {pool} ({endpoint ?? "s3"}/{bucket})..");
            client.ListObjectsAsync(bucket, "test_lookup").Wait();
            Console.WriteLine("OK!");
        }

        private static readonly object FileLock = new object();

        public Task Delete(string key)
        {
            lock (FileLock)
                File.AppendAllText($"deleted-{pool}.txt", $"single: {key}\n");

            return client.DeleteObjectAsync(bucket, key);
        }

        public Task Delete(IEnumerable<string> keys)
        {
            lock (FileLock)
                File.AppendAllText($"deleted-{pool}.txt", $"batch: {string.Join(" ", keys)}\n");

            return client.DeleteObjectsAsync(new DeleteObjectsRequest
            {
                BucketName = bucket,
                Objects = keys.Select(k => new KeyVersion { Key = k }).ToList()
            });
        }
    }
}