using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    public class Router : Connection, IRouter {
        private readonly KafkaConfigurationSection _section;
        private readonly HashSet<Broker> _brokers;
        private readonly HashSet<TopicMetadata> _topics;

        public Router()
            : this(KafkaConfigurationSection.LoadDefault()) {
        }

        public Router(KafkaConfigurationSection section)
            : base(section) {
            _section = section;
            _brokers = new HashSet<Broker>();
            _topics = new HashSet<TopicMetadata>();
        }

        public override IConnection Route(String topicName) {
            var topic = _topics.SingleOrDefault(r => r.TopicName.Equals(topicName));
            if (topic == null) {
                var resp = TopicMetadata(topicName);
                foreach (var item in resp.Brokers) {
                    _brokers.Add(item);
                }
                foreach (var item in resp.TopicMetadatas) {
                    _topics.Add(item);
                }
                topic = resp.TopicMetadatas.SingleOrDefault(r => r.TopicName.Equals(topicName));
            }

            var topicPartitionsCached = topic.PartitionMetadatas;
            if (topicPartitionsCached.Length == 0) {
                throw new Exception(); //todo
            }

            //topicPartitionsCached.Length == 1, 无视配置
            if (topicPartitionsCached.Length == 1) {
                var topicPartitionSelected = topicPartitionsCached[0];
                var broker = _brokers.SingleOrDefault(b => b.NodeId == topicPartitionSelected.Leader);
                return Clone(broker.Host, broker.Port, topicPartitionSelected.PartitionId);
            }

            var topicPartitionSettings = _section.TopicPartitions.OfType<TopicPartitionConfigurationElement>();
            var topicPartitionSetting = topicPartitionSettings.FirstOrDefault(x => x.Topic.Equals(topicName));
            //topicPartitionsCached.Length > 1 && topicPartitionSetting == null, 取小的 PartitionId 作为分区
            if (topicPartitionSetting == null) {
                var topicPartitionSelected = topicPartitionsCached.OrderBy(r => r.PartitionId).First();
                var broker = _brokers.SingleOrDefault(b => b.NodeId == topicPartitionSelected.Leader);
                return Clone(broker.Host, broker.Port, topicPartitionSelected.PartitionId);
            }

            //topicPartitionsCached.Length > 1 && topicPartitionSetting != null, 自定义 Partition 路由生效
            {
                var topicPartitionSelected = topicPartitionsCached.FirstOrDefault(x => x.PartitionId == topicPartitionSetting.Partition);
                if (topicPartitionSelected == null) {
                    throw new Exception(); //todo
                }
                CurrentPartition = topicPartitionSelected.PartitionId;
                var broker = _brokers.SingleOrDefault(b => b.NodeId == topicPartitionSelected.Leader);
                return Clone(broker.Host, broker.Port, topicPartitionSelected.PartitionId);
            }
        }

        public override TopicMetadataResponse TopicMetadata(String topicName) {
            var attemptLimit = 5;
            var response = base.TopicMetadata(topicName);
            while (attemptLimit-- > 0) {
                var metadata = response.TopicMetadatas[0];
                if (metadata.TopicErrorCode == ErrorCode.NoError) {
                    break;
                }
                if (metadata.TopicErrorCode != ErrorCode.LeaderNotAvailable) {
                    throw new KafkaException(metadata.TopicErrorCode);
                }
                //Debug.WriteLine("LeaderNotAvailable while hanlde TopicMetadata(\"{0}\")", args: topicName);
                if (attemptLimit <= 0) {
                    throw new KafkaException(metadata.TopicErrorCode);
                }
                Thread.Sleep(50);
                response = base.TopicMetadata(topicName);
            }
            return response;
        }
    }
}
