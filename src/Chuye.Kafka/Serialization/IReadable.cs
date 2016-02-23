using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    interface IReadable {
        void FetchFrom(BufferReader reader);
    }
}
