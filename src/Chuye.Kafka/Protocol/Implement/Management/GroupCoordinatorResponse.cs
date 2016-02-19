using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
    //  ErrorCode => int16
    //  CoordinatorId => int32
    //  CoordinatorHost => string
    //  CoordinatorPort => int32
    public class GroupCoordinatorResponse : Response {
        //Possible Error Codes
        //* GROUP_COORDINATOR_NOT_AVAILABLE (15)
        //* NOT_COORDINATOR_FOR_GROUP (16)
        //  GROUP_AUTHORIZATION_FAILED (30)
        public ErrorCode ErrorCode { get; set; }
        public Int32 CoordinatorId { get; set; }
        public String CoordinatorHost { get; set; }
        public Int32 CoordinatorPort { get; set; }

        protected override void DeserializeContent(Reader reader) {
            ErrorCode = (ErrorCode)reader.ReadInt16();
            CoordinatorId = reader.ReadInt32();
            CoordinatorHost = reader.ReadString();
            CoordinatorPort = reader.ReadInt32();
        }
    }
}
