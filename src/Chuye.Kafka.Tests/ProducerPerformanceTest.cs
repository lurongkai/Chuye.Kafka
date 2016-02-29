using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class ProducerPerformanceTest {
        const Int32 count = 1000;

        [TestMethod]
        public void SendMessag_Immediate() {
            Debug.Listeners.Clear();
            const String targetTopic = "immediateTopic";
            var option = Option.LoadDefault();
            var connection = new Router(option);
            var producer = new Producer(connection);
            producer.Strategy = AcknowlegeStrategy.Immediate;

            var stopwatch = Stopwatch.StartNew();
            {
                var message = String.Concat(Guid.NewGuid().ToString("n"), "#", 0);
                //Console.WriteLine("Sending... {0}", message);
                producer.Post(targetTopic, message);
            }
            for (int i = 1; i < count - 1; i++) {
                var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
                //Console.WriteLine("Sending... {0}", message);
                producer.Post(targetTopic, message);
            }
            {
                var message = String.Concat(Guid.NewGuid().ToString("n"), "#", count);
                //Console.WriteLine("Sending... {0}", message);
                producer.Post(targetTopic, message);
            }
            stopwatch.Stop();
            Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
        }

        [TestMethod]
        public void SendMessag_Written() {
            Debug.Listeners.Clear();
            const String targetTopic = "writtenTopic";
            var option = Option.LoadDefault();
            var connection = new Router(option);
            var producer = new Producer(connection);
            producer.Strategy = AcknowlegeStrategy.Written;

            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < count; i++) {
                var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
                //Console.WriteLine("Sending... {0}", message);
                producer.Post(targetTopic, message);
            }
            stopwatch.Stop();
            Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
        }
    }
}
