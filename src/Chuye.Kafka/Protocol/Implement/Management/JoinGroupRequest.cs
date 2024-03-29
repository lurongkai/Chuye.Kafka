﻿using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //JoinGroupRequest => GroupId SessionTimeout MemberId ProtocolType GroupProtocols
    //  GroupId => string
    //  SessionTimeout => int32
    //  MemberId => string
    //  ProtocolType => string
    //  GroupProtocols => [ProtocolName ProtocolMetadata]
    //    ProtocolName => string
    //    ProtocolMetadata => bytes
    public class JoinGroupRequest : Request {
        [Required]
        public String GroupId { get; set; }
        public Int32 SessionTimeout { get; set; }
        public String MemberId { get; set; }
        public String ProtocolType { get; set; }
        public JoinGroupRequestGroupProtocol[] GroupProtocols { get; set; }

        public JoinGroupRequest()
            : base(ApiKey.JoinGroupRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(GroupId);
            writer.Write(SessionTimeout);
            writer.Write(MemberId);
            writer.Write(ProtocolType);
            writer.Write(GroupProtocols);
        }

        protected override void DeserializeContent(BufferReader reader) {
            GroupId        = reader.ReadString();
            SessionTimeout = reader.ReadInt32();
            MemberId       = reader.ReadString();
            ProtocolType   = reader.ReadString();
            GroupProtocols = reader.ReadArray<JoinGroupRequestGroupProtocol>();
        }
    }

    public class JoinGroupRequestGroupProtocol : IWriteable, IReadable {
        public String ProtocolName { get; set; }
        public Byte[] ProtocolMetadata { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(ProtocolName);
            writer.Write(ProtocolMetadata);
        }

        public void FetchFrom(BufferReader reader) {
            ProtocolName     = reader.ReadString();
            ProtocolMetadata = reader.ReadBytes();
        }
    }
}
