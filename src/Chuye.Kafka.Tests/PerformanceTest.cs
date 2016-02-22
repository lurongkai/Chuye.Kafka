using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class PerformanceTest {
        [TestMethod]
        public void SendMessag_Async() {
            Debug.Listeners.Clear();
            const String targetTopic = "async-topic";
            var option = Option.LoadDefault();
            var producer = new Producer(option);
            producer.Strategy = AcknowlegeStrategy.Immediate;
            
            const Int32 count = 10000;
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

        [TestMethod]
        public void SendMessag_Written() {
            Debug.Listeners.Clear();
            const String targetTopic = "performace-topic";
            var option = Option.LoadDefault();

            var producer = new Producer(option);
            producer.Strategy = AcknowlegeStrategy.Written;
            const Int32 count = 100;
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

        [TestMethod]
        public void SendMessag_Written_Aysnc() {
            Debug.Listeners.Clear();
            const String targetTopic = "performace-topic";
            var option = Option.LoadDefault();
            
            var producer = new Producer(option);
            producer.Strategy = AcknowlegeStrategy.Written;
            const Int32 count = 10000;
            var stopwatch = Stopwatch.StartNew();
            var tasks = new Task[count];
            for (int i = 0; i < count; i++) {
                var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
                //Console.WriteLine("Sending... {0}", message);
                var j = i;
                tasks[j] = producer.PostAsync(targetTopic, message);
            }
            Task.WaitAll(tasks);
            stopwatch.Stop();
            Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
        }
    }
}
