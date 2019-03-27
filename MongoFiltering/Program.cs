using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Mongo2Go;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MongoFiltering
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<Comparison>(new DebugInProcessConfig());
            Console.Read();
        }
    }

    [MemoryDiagnoser]
    public class Comparison
    {
        private IMongoDatabase database;
        private Random random;
        private IMongoCollection<Outer> collection;
        private string collectionName;

        [Params(1, 100, 10000)]
        public int NumberOfOuters { get; set; }

        [Params(1, 100, 10000)]
        public int NumberOfElements { get; set; }

        [Params(0, 0.5, 1)]
        public double PercentageOfDeletedElements { get; set; }

        [IterationSetup]
        public void IterationSetup()
        {
            var runner = MongoDbRunner.Start(singleNodeReplSet: true);
            var client = new MongoClient(runner.ConnectionString);
            database = client.GetDatabase("testdb");
            random = new Random(32);
            collectionName = Guid.NewGuid().ToString();
            collection = database.GetCollection<Outer>(collectionName);

            var outers = new List<Outer>();
            for (var o = 0; o < NumberOfOuters; o++)
            {
                var data = new Outer
                {
                    Id = "OuterX",
                    Elements = new List<Element>(),
                };

                for (var i = 0; i < NumberOfElements; i++)
                {
                    var element = new Element { Id = "ElementX" };
                    element.DeletedAt = random.NextDouble() <= PercentageOfDeletedElements ? (DateTime?)DateTime.Now : null;
                    data.Elements.Add(element);
                }

                outers.Add(data);
            }

            collection.InsertMany(outers);
        }

        [Benchmark]
        public async Task<Outer> GetDbFilter()
        {
            var runner = MongoDbRunner.Start(singleNodeReplSet: true);
            var client = new MongoClient(runner.ConnectionString);
            database = client.GetDatabase("testdb");
            collection = database.GetCollection<Outer>(collectionName);
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

            var obj = await aggregate.AppendStage<Outer>(addFieldsStage).As<Outer>().SingleAsync();
            return obj;
        }

        [Benchmark]
        public async Task<List<Outer>> GetServerFilter()
        {
            var runner = MongoDbRunner.Start(singleNodeReplSet: true);
            var client = new MongoClient(runner.ConnectionString);
            database = client.GetDatabase("testdb");
            collection = database.GetCollection<Outer>(collectionName);
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
}
