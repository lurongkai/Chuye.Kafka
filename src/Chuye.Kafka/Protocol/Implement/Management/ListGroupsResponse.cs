﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //ListGroupsResponse => ErrorCode Groups
    //  ErrorCode => int16
    //  Groups => [GroupId ProtocolType]
    //    GroupId => string
    //    ProtocolType => string
    public class ListGroupsResponse : Response {
        /// <summary>
        /// Possible Error Codes:
        //   GROUP_COORDINATOR_NOT_AVAILABLE(15)
        //   AUTHORIZATION_FAILED(29)
        /// </summary>
        public ErrorCode ErrorCode { get; set; }
        public ListGroupsResponseGroup[] Groups { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            ErrorCode = (ErrorCode)reader.ReadInt16();
            Groups    = reader.ReadArray<ListGroupsResponseGroup>();
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write((Int16)ErrorCode);
            writer.Write(Groups);
        }
    }

    public class ListGroupsResponseGroup : IReadable, IWriteable {
        public String GroupId { get; set; }
        public String ProtocolType { get; set; }

        public void FetchFrom(BufferReader reader) {
            GroupId      = reader.ReadString();
            ProtocolType = reader.ReadString();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(GroupId);
            writer.Write(ProtocolType);
        }
    }
}
