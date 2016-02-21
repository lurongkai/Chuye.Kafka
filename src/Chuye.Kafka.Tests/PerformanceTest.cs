using System;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class PerformanceTest {
        private const String PerformaceTopic = "performace-topic";

        [TestMethod]
        public void SendMessag_Async() {
            var option = Option.LoadDefault();
            const Int32 count = 10;
            var producer = new Producer(option);
            producer.Strategy = AcknowlegeStrategy.Async;

            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < count; i++) {
                producer.Post(PerformaceTopic, String.Concat(Guid.NewGuid().ToString("n"), "#", i));
            }
            stopwatch.Stop();
            Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
        }

        [TestMethod]
        public void SendMessag_Written() {
            var option = Option.LoadDefault();
            const Int32 count = 1000;
            var producer = new Producer(option);
            producer.Strategy = AcknowlegeStrategy.Written;

            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < count; i++) {
                producer.Post(PerformaceTopic, String.Concat(Guid.NewGuid().ToString("n"), "#", i));
            }
            stopwatch.Stop();
            Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
        }
    }
}
