using System;
using System.Linq;
using System.Collections.Generic;
using Chuye.Kafka.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class KafkaConfigurationSectionTest {
        [TestMethod]
        public void Save_config_then_read_success() {
            var sectionWrite = new KafkaConfigurationSection("jusfr.kafka", 9092);
            //sectionWrite.Broker.Host = "jusfr.kafka";
            //sectionWrite.Broker.Port = 9092;
            sectionWrite.TopicPartitions.BaseAdd(new TopicPartitionConfigurationElement {
                Topic = "t1",
                Partition = 0,
            });
            sectionWrite.TopicPartitions.BaseAdd(new TopicPartitionConfigurationElement {
                Topic = "t2",
                Partition = 1,
            });
            var resolver = new ConfigurationResolver();
            resolver.Save(sectionWrite, "chuye.kafka");

            var sectionRead = KafkaConfigurationSection.LoadDefault();
            Assert.IsNotNull(sectionRead.Broker);
            Assert.AreEqual(sectionRead.Broker.Host, sectionRead.Broker.Host);
            Assert.AreEqual(sectionRead.Broker.Port, sectionRead.Broker.Port);

            Assert.IsNotNull(sectionRead.Buffer);
            Assert.AreEqual(sectionRead.Buffer.MaxBufferPoolSize, sectionRead.Buffer.MaxBufferPoolSize);
            Assert.AreEqual(sectionRead.Buffer.MaxBufferSize, sectionRead.Buffer.MaxBufferSize);
            Assert.AreEqual(sectionRead.Buffer.RequestBufferSize, sectionRead.Buffer.RequestBufferSize);
            Assert.AreEqual(sectionRead.Buffer.ResponseBufferSize, sectionRead.Buffer.ResponseBufferSize);
            Assert.IsNotNull(sectionRead.TopicPartitions);

            var topicPartitionWrites = sectionWrite.TopicPartitions
                .OfType<TopicPartitionConfigurationElement>()
                .ToArray();
            var topicPartitionReads = sectionWrite.TopicPartitions
                .OfType<TopicPartitionConfigurationElement>()
                .ToArray();
            Assert.AreEqual(topicPartitionWrites.Length, topicPartitionReads.Length);
            for (int i = 0; i < topicPartitionWrites.Length; i++) {
                Assert.AreEqual(topicPartitionWrites[0].Topic, topicPartitionReads[0].Topic);
                Assert.AreEqual(topicPartitionWrites[0].Partition, topicPartitionReads[0].Partition);
            }
        }
    }
}
