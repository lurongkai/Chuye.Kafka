using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Protocol.Implement.Management;

namespace Chuye.Kafka {
    class Program {
        const String DemoTopic = "t1";
        const String DemoGroup = "demoConsumerGroup";
        static Connection _connection;

        static void Main(string[] args) {
            var option = new Option("jusfr.kafka", 9093, requestBufferSize: 40960);
            const String demoTopic = "t2";
            //const String demoConsumerGroup = "demoConsumerGroup";
            var producer = new Producer(new Router(option));
            producer.Strategy = AcknowlegeStrategy.Written;
            var msgs = Enumerable.Range(0, 10)
                .Select(x => new KeyedMessage(null, Guid.NewGuid().ToString("n") + "#" + x))
                .ToArray();
            producer.Post(demoTopic, msgs);
            

            Debug.Listeners.Clear();
            //Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));
            _connection = new Connection(Option.LoadDefault());

            //Client_TopicMetadata_Demo();
            //Consumer_Offset_Fetch_Demo();
            //Consumer_Group_Operate();
            //Producer_Post_Demo();
            //Producer_Post_Immediate_Benchmark();
            Producer_Post_Written_Benchmark();

            if (Debugger.IsAttached) {
                Console.WriteLine("Press <Enter> to exit");
                Console.ReadLine();
            }
        }

        static void Client_TopicMetadata_Demo() {
            var option = Option.LoadDefault();
            using (var connection = new Connection(option)) {
                var metadata = connection.TopicMetadata(DemoTopic);
            }
        }

        static void Consumer_Offset_Fetch_Demo() {
            var option = Option.LoadDefault();
            using (var connection = new Router(option)) {
                var consumer = new Consumer(connection);
                var offset = consumer.Offset(DemoTopic, OffsetTimeOption.Latest);
                var messages = consumer.FetchAll(DemoTopic, 0);

                foreach (var msg in messages) {
                    Console.WriteLine(msg);
                }
            }
        }
        
        static void Producer_Post_Demo() {
            var messages = new List<KeyedMessage>();
            //messages.Add("Demo message begin at " + DateTime.Now);
            //messages.Add("Set some cloud service");
            //messages.Add(new KeyedMessage("cloud", "Microsoft Azure"));
            //messages.Add(new KeyedMessage("cloud", "Amazon Aws"));
            //messages.Add("Set some animals");
            //messages.Add(new KeyedMessage("animals", "penguin"));
            //messages.Add(new KeyedMessage("animals", "bear"));
            //messages.Add(new KeyedMessage("animals", "anteater"));
            //messages.Add("Demo message ended at " + DateTime.Now);

            var option = Option.LoadDefault();
            using (var connection = new Router(option)) {
                var producer = new Producer(connection);
                producer.Strategy = AcknowlegeStrategy.Written;
                producer.Post(DemoTopic, messages);
            }
        }

        static void Producer_Post_Written_Benchmark() {
            const String targetTopic = "written-topic";
            var option = Option.LoadDefault();
            using (var connection = new Router(option)) {
                var producer = new Producer(connection);
                producer.Strategy = AcknowlegeStrategy.Written;

                const Int32 count = 100;
                var stopwatch = Stopwatch.StartNew();
                for (int i = 0; i < count; i++) {
                    var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
                    Console.WriteLine("Sending... {0}", message);
                    producer.Post(targetTopic, message);
                }
                stopwatch.Stop();
                Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                    count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
            }
        }

        static void Producer_Post_Immediate_Benchmark() {
            const String targetTopic = "async-topic";
            var option = Option.LoadDefault();
            using (var connection = new Router(option)) {
                var producer = new Producer(connection);
                producer.Strategy = AcknowlegeStrategy.Immediate;

                const Int32 count = 10;
                var stopwatch = Stopwatch.StartNew();
                for (int i = 0; i < count; i++) {
                    var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
                    Console.WriteLine("Sending... {0}", message);
                    producer.Post(targetTopic, message);
                }
                stopwatch.Stop();
                Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                    count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
            }
        }

        static void Consumer_Group_Operate() {
            var option = Option.LoadDefault();
            using (var connection = new Router(option)) {
                var consumer = new Consumer(connection);
                consumer.GroupCoordinator(DemoGroup);
                consumer.JoinGroup(DemoGroup);
                consumer.SyncGroup(new Byte[0]);
                consumer.Heartbeat();
                consumer.ListGroups();
                consumer.DescribeGroups();
                consumer.LeaveGroup();
            }
        }
    }
}
