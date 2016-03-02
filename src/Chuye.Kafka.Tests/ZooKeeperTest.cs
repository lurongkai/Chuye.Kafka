using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZooKeeperNet;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class ZooKeeperTest {
        [TestMethod]
        public void ListTopic() {
            var section = KafkaConfigurationSection.LoadDefault();
            using (ZooKeeper zk = new ZooKeeper(section.Broker.Host, TimeSpan.FromSeconds(1), null)) {
                var topics = zk.GetChildren("/brokers/topics", false).ToArray();
                Console.WriteLine(String.Join(Environment.NewLine, topics));
            }
        }

        [TestMethod]
        public void TravelPaths() {
            var section = KafkaConfigurationSection.LoadDefault();
            using (ZooKeeper zk = new ZooKeeper(section.Broker.Host, TimeSpan.FromSeconds(10), null)) {
                GetChildren(zk, "/");
            }
        }

        private void GetChildren(ZooKeeper zk, String path) {
            var paths = zk.GetChildren(path, false).ToArray();
            if (paths.Length == 0) {
                return;
            }
            Console.WriteLine(path);
            foreach (var p in paths) {
                Console.WriteLine("\t{0}", p);
            }
            foreach (var p in paths) {
                GetChildren(zk, path.EndsWith("/") ? path + p : path + "/" + p);
            }
        }
    }
}
