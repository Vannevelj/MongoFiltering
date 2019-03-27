using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Mongo2Go;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace MongoFiltering
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var numberOfOuters = new[] { 1000, 100 };
            var numberOfElements = new[] { 100, 10000 };
            var percentageOfDeletedElements = new[] { 0.5, 1 };

            var results = new List<BenchmarkResult>();
            var benchmark = new Comparison();
            foreach(var outers in numberOfOuters)
            {
                foreach(var elements in numberOfElements)
                {
                    foreach(var deletedPercentage in percentageOfDeletedElements)
                    {
                        var dbIterations = new List<long>();
                        var serverIterations = new List<long>();
                        for (var i = 0; i < 20; i++)
                        {
                            var collection = await benchmark.IterationSetup(outers, elements, deletedPercentage);
                            var sw = Stopwatch.StartNew();
                            await benchmark.GetDbFilter(collection);
                            sw.Stop();
                            dbIterations.Add(sw.ElapsedMilliseconds);

                            collection = await benchmark.IterationSetup(outers, elements, deletedPercentage);
                            sw = Stopwatch.StartNew();
                            await benchmark.GetServerFilter(collection);
                            sw.Stop();
                            serverIterations.Add(sw.ElapsedMilliseconds);
                        }

                        Console.ForegroundColor = ConsoleColor.White;
                        Console.WriteLine($"Outer objects: {outers}");
                        Console.WriteLine($"Inner elements: {elements}");
                        Console.WriteLine($"Deleted percentage: {deletedPercentage}");
                        Console.ForegroundColor = ConsoleColor.Red;
                        var db = dbIterations.Average();
                        var server = serverIterations.Average();
                        Console.WriteLine($"DB filtering: {db}");
                        Console.WriteLine($"Server filtering: {server}");
                        Console.ForegroundColor = ConsoleColor.Gray;

                        results.Add(new BenchmarkResult
                        {
                            Database = db,
                            Server = server,
                            Conditions = $"Outer: {outers}\tElements: {elements}\tPercentage: {deletedPercentage}"
                        });
                    }
                }
            }

            Console.WriteLine($"===============================================");
            foreach(var result in results)
            {
                Console.WriteLine(result.Conditions);
                Console.WriteLine($"\tServer: {result.Server}");
                Console.WriteLine($"\tDatabase: {result.Database}");
            }
            Console.WriteLine($"===============================================");

            Console.Read();
        }
    }

    public class Comparison
    {
        public async Task<IMongoCollection<Outer>> IterationSetup(int outerCount, int elements, double percentage)
        {
            var runner = MongoDbRunner.Start(singleNodeReplSet: true);
            //var colon = runner.ConnectionString.LastIndexOf(":");
            //var host = runner.ConnectionString.Substring(0, colon);
            //var port = runner.ConnectionString.Substring(colon + 1);
            //var settings = new MongoClientSettings
            //{
            //    SocketTimeout = TimeSpan.FromMinutes(10),
            //    ConnectTimeout = TimeSpan.FromMinutes(10),
            //    HeartbeatTimeout = TimeSpan.FromMinutes(10),
            //    Server = new MongoServerAddress(host, int.Parse(port))
            //};
            var connectionString = runner.ConnectionString + "&socketTimeoutMS=99999999&connectTimeoutMS=99999999&wtimeoutMS=99999999";
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase("testdb");
            var random = new Random(32);
            var collection = database.GetCollection<Outer>(Guid.NewGuid().ToString());

            var outers = new List<Outer>();
            for (var o = 0; o < outerCount; o++)
            {
                var data = new Outer
                {
                    Id = Guid.NewGuid().ToString(),
                    Elements = new List<Element>(),
                };

                for (var i = 0; i < elements; i++)
                {
                    var element = new Element { Id = Guid.NewGuid().ToString() };
                    element.DeletedAt = random.NextDouble() <= percentage ? (DateTime?)DateTime.Now : null;
                    data.Elements.Add(element);
                }

                outers.Add(data);
            }

            await collection.InsertManyAsync(outers);
            return collection;
        }

        public async Task<List<Outer>> GetDbFilter(IMongoCollection<Outer> collection)
        {
            var aggregate = collection.Aggregate();

            var expression = new BsonDocument
            {
                { "input", "$e" },
                { "cond", new BsonDocument
                    {
                        {
                            "$eq", new BsonArray(new BsonValue[] { new BsonString("$$this.da"), BsonNull.Value })
                        }
                    }
                }
            };

            var filterExpression = new BsonDocument { { "$filter", expression } };
            var overwriteExpression = new BsonDocument { { "e", filterExpression } };
            var addFieldsStage = new BsonDocument(new BsonElement("$addFields", overwriteExpression));

            return await aggregate.AppendStage<Outer>(addFieldsStage).ToListAsync();
        }

        public async Task<List<Outer>> GetServerFilter(IMongoCollection<Outer> collection)
        {
            var docs = await (await collection.FindAsync(x => true)).ToListAsync();
            foreach (var doc in docs)
            {
                doc.Elements = doc.Elements.Where(x => x.DeletedAt == null).ToList();
            }

            return docs;
        }
    }

    public class Outer
    {
        public string Id { get; set; }

        [BsonElement("e")]
        public List<Element> Elements { get; set; } = new List<Element>();
    }

    public class Element
    {
        public string Id { get; set; }

        [BsonElement("da")]
        public DateTime? DeletedAt { get; set; }
    }

    public class BenchmarkResult
    {
        public double Server { get; set; }
        public double Database { get; set; }
        public string Conditions { get; set; }
    }
}
