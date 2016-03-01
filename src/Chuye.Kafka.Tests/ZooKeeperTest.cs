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
            }
        }
    }
}
