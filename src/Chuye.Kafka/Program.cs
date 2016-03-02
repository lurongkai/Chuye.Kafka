using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Chuye.Kafka {
    class Program {
        const String DemoTopic = "demoTopic";
        const String DemoGroup = "demoConsumerGroup";
        static Connection _connection;

        static void Main(string[] args) {
            Debug.Listeners.Clear();
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));

            //_connection = new Connection();
            _connection = new Router();

            //Client_TopicMetadata_Demo();
            //Producer_Post_Demo();
            //Consumer_Offset_Fetch_Demo();
            //Producer_Post_Immediate_Benchmark();
            //Producer_Post_Written_Benchmark();
            //Consumer_Group_Operate();

            if (Debugger.IsAttached) {
                Console.WriteLine("Press <Enter> to exit");
                Console.ReadLine();
            }
        }

        static void Client_TopicMetadata_Demo() {
            var metadata = _connection.TopicMetadata(DemoTopic);
        }

        static void Consumer_Offset_Fetch_Demo() {
            var consumer = new Consumer(_connection);
            var offset = consumer.Offset(DemoTopic, OffsetTimeOption.Latest);
            var messages = consumer.FetchAll(DemoTopic, 0);

            foreach (var msg in messages) {
                Console.WriteLine(msg);
            }
        }

        static void Producer_Post_Demo() {
            var messages = new List<KeyedMessage>();
            messages.Add("Demo message begin at " + DateTime.Now);
            messages.Add("Set some cloud service");
            messages.Add(new KeyedMessage("cloud", "Microsoft Azure"));
            messages.Add(new KeyedMessage("cloud", "Amazon Aws"));
            messages.Add("Set some animals");
            messages.Add(new KeyedMessage("animals", "penguin"));
            messages.Add(new KeyedMessage("animals", "bear"));
            messages.Add(new KeyedMessage("animals", "anteater"));
            messages.Add("Demo message ended at " + DateTime.Now);

            var producer = new Producer(_connection);
            producer.Strategy = AcknowlegeStrategy.Written;
            producer.Post(DemoTopic, messages);
        }

        static void Producer_Post_Written_Benchmark() {
            const String targetTopic = "writtenTopic";
            var producer = new Producer(_connection);
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

        static void Producer_Post_Immediate_Benchmark() {
            const String targetTopic = "asyncTopic";
            var producer = new Producer(_connection);
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

        static void Consumer_Group_Operate() {
            var consumer = new Consumer(_connection);
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
