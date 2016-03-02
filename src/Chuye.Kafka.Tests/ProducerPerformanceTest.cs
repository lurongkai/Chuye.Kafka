using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class ProducerPerformanceTest {
        const Int32 count = 10000;

        [TestMethod]
        public void SendMessag_Immediate() {
            const String demoTopic = "demoTopic";
            var connection = new Connection();
            connection.TopicMetadata(demoTopic);
            var producer = new Producer(connection);
            producer.Strategy = AcknowlegeStrategy.Immediate;
            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < count; i++) {
                var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
                producer.Post(demoTopic, message);
            }
            stopwatch.Stop();
            connection.TopicMetadata(demoTopic);
            Console.WriteLine("Handle {0} messages in {1}, {2:f3} /sec.",
                count, stopwatch.Elapsed,
                count / stopwatch.Elapsed.TotalSeconds);
            Console.WriteLine("Bytes sended {0:f2} MB, {1:f2} MB/spc.",
                //connection.ByteSended >> 20, (connection.ByteSended >> 20) / stopwatch.Elapsed.TotalSeconds);
                connection.ByteSended / 1048576.0,
                connection.ByteSended / 1048576.0 / stopwatch.Elapsed.TotalSeconds);
        }

        [TestMethod]
        public void SendMessag_Written() {
            const String demoTopic = "demoTopic";
            var connection = new Connection();
            connection.TopicMetadata(demoTopic);
            var producer = new Producer(connection);
            producer.Strategy = AcknowlegeStrategy.Written;
            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < count; i++) {
                var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
                producer.Post(demoTopic, message);
            }
            stopwatch.Stop();
            connection.TopicMetadata(demoTopic);
            Console.WriteLine("Handle {0} messages in {1}, {2:f3} /sec.",
                count, stopwatch.Elapsed,
                count / stopwatch.Elapsed.TotalSeconds);
            Console.WriteLine("Bytes sended {0:f2} MB, {1:f2} MB/spc.",
                //connection.ByteSended >> 20, (connection.ByteSended >> 20) / stopwatch.Elapsed.TotalSeconds);
                connection.ByteSended / 1048576.0,
                connection.ByteSended / 1048576.0 / stopwatch.Elapsed.TotalSeconds);
        }
    }
}
