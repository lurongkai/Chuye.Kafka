﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    public interface IComputable : IDisposable {
        Int32 Output { get; }
    }
}
