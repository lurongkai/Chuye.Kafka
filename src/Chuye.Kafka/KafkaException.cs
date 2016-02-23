using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka {
    public class KafkaException : Exception {
        public ErrorCode ErrorCode { get; private set; }

        public KafkaException(ErrorCode errorCode)
            : base(errorCode.ToString()) {
            ErrorCode = errorCode;
        }
    }
}
