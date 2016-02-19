﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //HeartbeatResponse => ErrorCode
    //  ErrorCode => int16
    public class HeartbeatResponse : Response {
        //Possible Error Codes:
        //* GROUP_COORDINATOR_NOT_AVAILABLE (15)
        //* NOT_COORDINATOR_FOR_GROUP (16)
        //* ILLEGAL_GENERATION (22)
        //* UNKNOWN_MEMBER_ID (25)
        //* REBALANCE_IN_PROGRESS (27)
        //* GROUP_AUTHORIZATION_FAILED (30)
        public ErrorCode ErrorCode { get; set; }

        protected override void DeserializeContent(Reader reader) {
            ErrorCode = (ErrorCode)reader.ReadInt16();
        }
    }
}
