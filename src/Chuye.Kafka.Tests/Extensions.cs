using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Newtonsoft.Json;

namespace Chuye.Kafka.Tests {
    public static class Extensions {
        const String Spliter = "------------------------";

        public static Request Dump(this Request value, String region = null) {
            WirteStartRegion(region);
            WriteObjectJosn(value);
            WirteEndRegion(region);
            Console.WriteLine();
            return value;
        }

        private static void WriteObjectJosn(Object value) {
            Console.WriteLine(JsonConvert.SerializeObject(value, Formatting.Indented));
        }

        private static void WirteStartRegion(String region) {
            if (!String.IsNullOrWhiteSpace(region)) {
                Console.WriteLine(region);
                Console.WriteLine("{0}{1}", Spliter, Spliter);
            }
        }

        private static void WirteEndRegion(String region) {
            if (!String.IsNullOrWhiteSpace(region)) {
                Console.WriteLine("{0}{1}", Spliter, Spliter);
            }
        }

        public static Response Dump(this Response value, String region = null) {
            WirteStartRegion(region);
            WriteObjectJosn(value);
            WirteEndRegion(region);
            Console.WriteLine();
            return value;
        }
    }
}
