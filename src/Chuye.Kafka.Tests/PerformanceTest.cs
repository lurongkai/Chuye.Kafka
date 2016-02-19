using System;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class PerformanceTest {
        private const String DemoTopicName = "test6";

        [TestMethod]
        public void SendTenThousandMessagesWithNoResponse() {
            var option = Option.LoadDefault();
            var count = 10000;
            var producer = new Producer(option);
            producer.SendStrategy = ProducerSendStrategy.NoResponse;

            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < count; i++) {
                producer.Post(DemoTopicName, String.Concat(Guid.NewGuid().ToString("n"), ", ", i));
            }
            stopwatch.Stop();
            Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
        }

        [TestMethod]
        public void SendTenThousandMessagesWithWaitLogged() {
            var option = Option.LoadDefault();
            var count = 10000;
            var producer = new Producer(option);
            producer.SendStrategy = ProducerSendStrategy.WaitLogged;

            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < count; i++) {
                producer.Post(DemoTopicName, String.Concat(Guid.NewGuid().ToString("n"), ", ", i));
            }
            stopwatch.Stop();
            Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
        }
    }
}
