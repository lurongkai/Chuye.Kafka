using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        protected override void DeserializeContent(Reader reader) {
            ErrorCode = (ErrorCode)reader.ReadInt16();
            var size = reader.ReadInt32();
            if (size == -1) {
                return;
            }
            Groups = new ListGroupsResponseGroup[size];
            for (int i = 0; i < size; i++) {
                Groups[i] = new ListGroupsResponseGroup();
                Groups[i].FetchFrom(reader);
            }
        }
    }

    public class ListGroupsResponseGroup : IReadable {
        public String GroupId { get; set; }
        public String ProtocolType { get; set; }

        public void FetchFrom(Reader reader) {
            GroupId = reader.ReadString();
            ProtocolType = reader.ReadString();
        }
    }
}
