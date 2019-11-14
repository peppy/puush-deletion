using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using StatsdClient;

namespace puush_deletion
{
    class PuushEndpointStore
    {
        public readonly int Pool;

        private readonly AmazonS3Client client;
        private readonly string bucket;
        private readonly string endpoint;
        private readonly bool requiresDeletion;

        public PuushEndpointStore(int pool, string key, string secret, string bucket, string endpoint = null, bool requiresDeletion = true)
        {
            if (endpoint != null)
                client = new AmazonS3Client(new BasicAWSCredentials(key, secret), new AmazonS3Config
                {
                    CacheHttpClient = true,
                    HttpClientCacheSize = 32,
                    ServiceURL = endpoint,
                    UseHttp = true,
                    ForcePathStyle = true
                });
            else
                client = new AmazonS3Client(new BasicAWSCredentials(key, secret), RegionEndpoint.USWest2);

            Pool = pool;

            this.bucket = bucket;
            this.endpoint = endpoint;
            this.requiresDeletion = requiresDeletion;

            Console.Write($"Checking connection to endpoint {pool} ({endpoint ?? "s3"}/{bucket})..");
            client.ListObjectsAsync(bucket, "test_lookup").Wait();
            Console.WriteLine("OK!");
        }

        public override string ToString() => $"{Pool} ({endpoint})";

        private static readonly object file_lock = new object();

        public Task Delete(string key)
        {
            lock (file_lock)
                File.AppendAllText($"deleted-{Pool}.txt", $"single: {key}\n");

            if (!requiresDeletion) return Task.CompletedTask;

            return client.DeleteObjectAsync(bucket, key);
        }

        public Task<GetObjectResponse> Get(string key)
        {
            lock (file_lock)
                File.AppendAllText($"migrated-{Pool}.txt", $"{key}\n");
            return client.GetObjectAsync(bucket, key);
        }

        public Task Put(string key, GetObjectResponse getData)
        {
            return client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                CannedACL = S3CannedACL.PublicRead,
                Headers =
                {
                    ContentLength = getData.Headers.ContentLength,
                    ContentType = getData.Headers.ContentType,
                },
                InputStream = getData.ResponseStream
            });
        }

        public Task Delete(IEnumerable<string> keys)
        {
            int count = keys.Count();

            lock (file_lock)
                File.AppendAllText($"deleted-{Pool}.txt", $"batch: {string.Join(" ", keys)}\n");
            
            DogStatsd.Increment("deleted", tags: new[] { $"pool:{Pool}" }, value: count);

            if (!requiresDeletion) return Task.CompletedTask;

            switch (count)
            {
                case 1:
                    return Delete(keys.First());
                default:
                    return client.DeleteObjectsAsync(new DeleteObjectsRequest
                    {
                        BucketName = bucket,
                        Objects = keys.Select(k => new KeyVersion { Key = k }).ToList()
                    });
            }
        }
    }
}