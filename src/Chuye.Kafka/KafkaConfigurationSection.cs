using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Utils;

namespace Chuye.Kafka {
    public class KafkaConfigurationSection : ConfigurationSection {
        [ConfigurationProperty("broker")]
        public BrokerConfigurationElement Broker {
            get {
                return (BrokerConfigurationElement)this["broker"];
            }
            set {
                this["broker"] = value;
            }
        }

        [ConfigurationProperty("buffer")]
        public BufferConfigurationElement Buffer {
            get {
                return (BufferConfigurationElement)this["buffer"];
            }
            set {
                this["buffer"] = value;
            }
        }

        [ConfigurationProperty("topicPartitions", IsDefaultCollection = false, IsRequired = true)]
        [ConfigurationCollection(typeof(List<TopicPartitionConfigurationElement>), AddItemName = "add")]
        public TopicPartitionConfigurationElementCollection TopicPartitions {
            get {
                return (TopicPartitionConfigurationElementCollection)this["topicPartitions"];
            }
            set {
                this["topicPartitions"] = value;
            }
        }

        protected KafkaConfigurationSection() {
            Buffer = new BufferConfigurationElement {
                MaxBufferPoolSize = 1024 * 1024,
                MaxBufferSize = 1024 * 64,
                RequestBufferSize = 1024 * 4,
                ResponseBufferSize = 1024 * 64
            };
        }

        public KafkaConfigurationSection(String host, Int32 port)
            : this() {
            Broker = new BrokerConfigurationElement {
                Host = host,
                Port = port,
            };
        }

        public static KafkaConfigurationSection LoadDefault() {
            var resolver = new ConfigurationResolver();
            var section = resolver.Read<KafkaConfigurationSection>("chuye.kafka");
            return section;
        }
    }

    public class BrokerConfigurationElement : ConfigurationElement {
        [ConfigurationProperty("host", IsRequired = true)]
        public String Host {
            get {
                return (String)this["host"];
            }
            set {
                this["host"] = value;
            }
        }

        [ConfigurationProperty("port", IsRequired = true)]
        public Int32 Port {
            get {
                return (Int32)this["port"];
            }
            set {
                this["port"] = value;
            }
        }

    }

    public class BufferConfigurationElement : ConfigurationElement {

        [ConfigurationProperty("maxBufferPoolSize", IsRequired = false)]
        public Int32 MaxBufferPoolSize {
            get {
                return (Int32)this["maxBufferPoolSize"];
            }
            set {
                this["maxBufferPoolSize"] = value;
            }
        }

        [ConfigurationProperty("maxBufferSize", IsRequired = false)]
        public Int32 MaxBufferSize {
            get {
                return (Int32)this["maxBufferSize"];
            }
            set {
                this["maxBufferSize"] = value;
            }
        }

        [ConfigurationProperty("requestBufferSize", IsRequired = false)]
        public Int32 RequestBufferSize {
            get {
                return (Int32)this["requestBufferSize"];
            }
            set {
                this["requestBufferSize"] = value;
            }
        }

        [ConfigurationProperty("responseBufferSize", IsRequired = false)]
        public Int32 ResponseBufferSize {
            get {
                return (Int32)this["responseBufferSize"];
            }
            set {
                this["responseBufferSize"] = value;
            }
        }

    }

    public class TopicPartitionConfigurationElement : ConfigurationElement {
        [ConfigurationProperty("topic", IsRequired = true)]
        public String Topic {
            get {
                return (String)this["topic"];
            }
            set {
                this["topic"] = value;
            }
        }

        [ConfigurationProperty("partition", IsRequired = true)]
        public Int32 Partition {
            get {
                return (Int32)this["partition"];
            }
            set {
                this["partition"] = value;
            }
        }
    }

    public class TopicPartitionConfigurationElementCollection : ConfigurationElementCollection {
        protected override ConfigurationElement CreateNewElement() {
            return new TopicPartitionConfigurationElement();
        }

        protected override Object GetElementKey(ConfigurationElement element) {
            return ((TopicPartitionConfigurationElement)element);
        }

        public void BaseAdd(TopicPartitionConfigurationElement element) {
            base.BaseAdd(element);
        }
    }
}
