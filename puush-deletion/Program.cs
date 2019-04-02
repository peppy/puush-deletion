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
        private static long freed_bytes;
        private static int skipped_endpoint;
        private static int skipped_pro;
        private static int chunks_processed;

        static void Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
                .Build();

            ServicePointManager.DefaultConnectionLimit = 128;

            ThreadPool.GetMinThreads(out _, out int completion);
            ThreadPool.SetMinThreads(256, completion);

            Database.ConnectionString = config["database"];
            Database.ConnectionStringSlave = config["database_slave"];

            var partitionSize = args.Length > 1 ? int.Parse(args[1]) : int.Parse(config["partition_size"]);
            var workerCount = int.Parse(config["worker_count"]);

            var endpoints = new Dictionary<int, PuushEndpointStore>();
            var specificEndpoints = new List<int>();

            if (args.Length > 0)
                specificEndpoints.Add(int.Parse(args[0]));
            else
            {
                foreach (var v in config.GetSection("run_endpoints").GetChildren())
                    specificEndpoints.Add(int.Parse(v.Value));
            }

            foreach (var section in config.GetSection("endpoints").GetChildren())
            {
                int poolId = int.Parse(section["Pool"]);

                if (specificEndpoints.Count == 0 || specificEndpoints.Contains(poolId))
                    endpoints.Add(int.Parse(section["Pool"]), new PuushEndpointStore(poolId, section["Key"], section["Secret"], section["Bucket"], section["Endpoint"], bool.Parse(section["RequiresDeletion"])));
            }

            if (specificEndpoints.Count == 0)
                foreach (var e in endpoints)
                    specificEndpoints.Add(e.Key);

            var proUsers = new List<int>();

            Console.Write("Populating pro users...");
            var results = Database.RunQuery("SELECT user_id FROM user WHERE status > 0");
            while (results.Read())
                proUsers.Add(results.GetInt32("user_id"));

            Console.WriteLine($" {proUsers.Count} users found!");

            var endpointsString = string.Join(", ", specificEndpoints);

            Console.WriteLine();
            Console.WriteLine("Running for endpoints: " + endpointsString);
            Console.WriteLine("Parition size:         " + partitionSize);
            Console.WriteLine("Workers:               " + workerCount);

            Console.WriteLine("Fetching deletable items...");
            results = Database.RunQuerySlave(
                $"SELECT `upload`.`upload_id`, `upload`.`user_id`, `upload`.`filestore`, `upload`.`filesize`, `upload`.`pool_id`, `upload`.`path` FROM `upload_stats` FORCE INDEX (delete_lookup) INNER JOIN `upload` ON `upload`.`upload_id` = `upload_stats`.`upload_id` WHERE `upload_stats`.`last_access` < DATE_ADD(NOW(), INTERVAL -90 DAY) AND `filestore` in ({endpointsString})");

            var options = new ParallelOptions { MaxDegreeOfParallelism = workerCount };

            StartConsoleLogging();

            var existingCounts = new Dictionary<string, int>();

            Parallel.ForEach(results.Cast<IDataRecord>().Partition(partitionSize), options, records =>
            {
                try
                {
                    Interlocked.Increment(ref running);

                    var chunk = records.Select(r => new PuushUpload(r)).ToList();

                    Interlocked.Add(ref skipped_pro, chunk.RemoveAll(i => proUsers.Contains(i.UserId)));

                    Interlocked.Add(ref skipped_endpoint, chunk.RemoveAll(i => !specificEndpoints.Contains(i.Filestore)));

                    List<PuushUpload> toDeleteFromStores = new List<PuushUpload>(chunk);

                    // we can't delete items from stores which have a null path
                    toDeleteFromStores.RemoveAll(i => string.IsNullOrEmpty(i.Path));

                    for (var index = 0; index < toDeleteFromStores.Count; index++)
                    {
                        var i = toDeleteFromStores[index];
                        var key = $"{i.Filestore}/{i.Path}";

                        lock (existingCounts)
                        {
                            bool didExist;
                            if (!(didExist = existingCounts.TryGetValue(key, out int count)))
                            {
                                count = (int)(long)Database.RunQueryOne("SELECT COUNT(*) FROM `upload` WHERE `upload`.`path` = @path AND `upload`.`filestore` = @filestore",
                                    new MySqlParameter("id", i.UploadId),
                                    new MySqlParameter("path", i.Path),
                                    new MySqlParameter("filestore", i.Filestore));
                            }

                            if (--count > 0)
                            {
                                existingCounts[key] = count;
                                Console.WriteLine($"Found {count} remaining instances for {key}; skipping delete.");
                                Interlocked.Increment(ref existing);
                                toDeleteFromStores.Remove(i);
                                index--;
                            }
                            else if (didExist)
                            {
                                Console.WriteLine($"All remaining instances for {key} are purged; performing delete.");
                                existingCounts.Remove(key);
                            }
                        }
                    }

                    if (toDeleteFromStores.Count > 0)
                    {
                        try
                        {
                            Task.WaitAll(
                                toDeleteFromStores.GroupBy(i => i.Filestore)
                                    .Select(e => endpoints[e.Key].Delete(e.Select(i => $"files/{i.Path}"))).ToArray()
                            );
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Error on chunk: " + string.Join(",", chunk.Select(c => c.UploadId)));
                            Console.WriteLine(e.ToString());
                            Interlocked.Increment(ref errors);
                            return;
                        }

                        Interlocked.Add(ref freed_bytes, toDeleteFromStores.Sum(i => i.Filesize));
                    }

                    foreach (var item in chunk)
                    {
                        Database.RunNonQuery($"DELETE FROM `upload` WHERE `upload_id` = {item.UploadId}");
                        Database.RunNonQuery($"DELETE FROM `upload_stats` WHERE `upload_id` = {item.UploadId}");
                        Database.RunNonQuery($"UPDATE `user` SET `disk_usage` = GREATEST(0, cast(`disk_usage` as signed) - {item.Filesize}) WHERE `user_id` = {item.UserId}");
                        Database.RunNonQuery($"UPDATE `pool` SET `count` = GREATEST(0, `count` - 1) WHERE `pool_id` = {item.Pool}");
                    }

                    Interlocked.Add(ref deletions, chunk.Count);
                }
                finally
                {
                    Interlocked.Increment(ref chunks_processed);
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
                    Console.WriteLine($"active {running} chunks {chunks_processed:n0} delrows {deletions:n0} errors {errors:n0} dupes {existing:n0} space {freed_bytes / 1024f / 1024 / 1024:n1}GB pro {skipped_pro:n0} skip {skipped_endpoint:n0}");
                }

                // ReSharper disable once FunctionNeverReturns
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