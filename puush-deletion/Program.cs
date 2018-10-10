using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;

namespace puush_deletion
{
    static class Program
    {
        private static int existing;
        private static int deletions;
        private static int errors;
        private static int running;
        private static long freedBytes;
        private static int proFilesSkipped;
        private static int chunksProcessed;

        static void Main()
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
                .Build();

            ServicePointManager.DefaultConnectionLimit = 128;
            
            ThreadPool.GetMinThreads(out _, out int completion);
            ThreadPool.SetMinThreads(256, completion);
            
            Database.ConnectionString = config["database"];
            Database.ConnectionStringSlave = config["database_slave"];
            
            var partitionSize = int.Parse(config["partition_size"]);
            var workerCount = int.Parse(config["worker_count"]);

            var endpoints = new Dictionary<int, PuushEndpointStore>();

            foreach (var section in config.GetSection("endpoints").GetChildren())
                endpoints.Add(int.Parse(section["Pool"]), new PuushEndpointStore(int.Parse(section["Pool"]), section["Key"], section["Secret"], section["Bucket"], section["Endpoint"]));

            var proUsers = new List<int>();

            Console.Write("Populating pro users...");
            var results = Database.RunQuery("SELECT user_id FROM user WHERE status > 0");
            while (results.Read())
                proUsers.Add(results.GetInt32("user_id"));

            Console.WriteLine($" {proUsers.Count} users found!");

            Console.WriteLine("Fetching deletable items...");
            results = Database.RunQuerySlave(
                "SELECT `upload`.`upload_id`, `upload`.`user_id`, `upload`.`filestore`, `upload`.`filesize`, `upload`.`pool_id`, `upload`.`path` FROM `upload_stats` FORCE INDEX (delete_lookup) INNER JOIN `upload` ON `upload`.`upload_id` = `upload_stats`.`upload_id` WHERE (`upload_stats`.`last_access` < DATE_ADD(NOW(), INTERVAL -90 DAY))");

            var options = new ParallelOptions { MaxDegreeOfParallelism = workerCount };

            StartConsoleLogging();

            Parallel.ForEach(results.Cast<IDataRecord>().Partition(partitionSize), options, records =>
            {
                try
                {
                    Interlocked.Increment(ref running);

                    var chunk = records.Select(r => new PuushUpload(r)).ToList();

                    Interlocked.Add(ref proFilesSkipped, chunk.RemoveAll(i => proUsers.Contains(i.UserId)));

                    List<PuushUpload> toDelete = new List<PuushUpload>(chunk);
                    foreach (var i in chunk)
                    {
                        var stillExists = Database.RunQueryOne("SELECT upload_id FROM `upload` WHERE `upload`.`upload_id` != @id AND `upload`.`path` = @path AND `upload`.`filestore` = @filestore LIMIT 1",
                                              new MySqlParameter("id", i.UploadId),
                                              new MySqlParameter("path", i.Path),
                                              new MySqlParameter("filestore", i.Filestore)) != null;

                        if (stillExists)
                        {
                            Console.WriteLine($"Other instance found for {i.UploadId}; skipping delete.");
                            Interlocked.Increment(ref existing);
                            toDelete.Remove(i);
                        }
                    }

                    if (toDelete.Count > 0)
                    {
                        try
                        {
                            Task.WaitAll(
                                toDelete.GroupBy(i => i.Filestore)
                                    .Select(e => endpoints[e.Key].Delete(e.Select(i => $"files/{i.Path}"))).ToArray()
                            );
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.ToString());
                            Interlocked.Increment(ref errors);
                            return;
                        }

                        Interlocked.Add(ref deletions, toDelete.Count);
                        Interlocked.Add(ref freedBytes, toDelete.Sum(i => i.Filesize));
                    }

                    foreach (var item in toDelete)
                    {
                        Database.RunNonQuery($"DELETE FROM `upload` WHERE `upload_id` = {item.UploadId}");
                        Database.RunNonQuery($"DELETE FROM `upload_stats` WHERE `upload_id` = {item.UploadId}");
                        Database.RunNonQuery($"UPDATE `user` SET `disk_usage` = GREATEST(0, cast(`disk_usage` as signed) - {item.Filesize}) WHERE `user_id` = {item.UserId}");
                        Database.RunNonQuery($"UPDATE `pool` SET `count` = GREATEST(0, `count` - 1) WHERE `pool_id` = {item.Pool}");
                    }
                }
                finally
                {
                    Interlocked.Increment(ref chunksProcessed);
                    Interlocked.Decrement(ref running);
                }
            });
        }

        private static void StartConsoleLogging()
        {
            var logger = new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    Console.WriteLine($"running {running} completed {chunksProcessed:n0} deleted {deletions:n0} errors {errors:n0} dupes {existing:n0} space {freedBytes / 1024 / 1024 / 1024:n0}GB skipped {proFilesSkipped:n0}");
                }
            }) { IsBackground = true };
            logger.Start();
        }

        private static IEnumerable<List<T>> Partition<T>(this IEnumerable<T> items, int partitionSize)
        {
            var partition = new List<T>(partitionSize);
            foreach (T item in items)
            {
                partition.Add(item);
                if (partition.Count == partitionSize)
                {
                    yield return partition;
                    partition = new List<T>(partitionSize);
                }
            }

            // Cope with items.Count % partitionSize != 0
            if (partition.Count > 0) yield return partition;
        }
    }
}