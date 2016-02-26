﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //OffsetResponse => [TopicName [PartitionOffsets]]
    //  PartitionOffsets => Partition ErrorCode [Offset]
    //  Partition => int32
    //  ErrorCode => int16
    //  Offset => int64
    public class OffsetResponse : Response {
        public OffsetResponseTopicPartition[] TopicPartitions { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            TopicPartitions = reader.ReadArray<OffsetResponseTopicPartition>();
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(TopicPartitions);
        }
    }

    public class OffsetResponseTopicPartition : IReadable, IWriteable {
        public String TopicName { get; set; }
        public OffsetResponsePartitionOffset[] PartitionOffsets { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicName        = reader.ReadString();
            PartitionOffsets = reader.ReadArray<OffsetResponsePartitionOffset>();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(PartitionOffsets);
        }
    }

    public class OffsetResponsePartitionOffset : IReadable, IWriteable {
        public Int32 Partition { get; set; }
        //Possible Error Codes
        //* UNKNOWN_TOPIC_OR_PARTITION (3)
        //* NOT_LEADER_FOR_PARTITION (6)
        //* UNKNOWN (-1)
        public ErrorCode ErrorCode { get; set; }
        public Int64[] Offsets { get; set; }

        public void FetchFrom(BufferReader reader) {
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
            Offsets   = reader.ReadInt64Array();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write((Int16)ErrorCode);
            writer.Write(Offsets);
        }
    }
}
