using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Chuye.Kafka {
    public struct Option : IEquatable<Option> {
        public String Host;
        public Int32 Port;
        public Int32 MaxBufferPoolSize;
        public Int32 MaxBufferSize;
        public Int32 RequestBufferSize;
        public Int32 ResponseBufferSize;

        public static Option LoadDefault() {
            var config = ConfigurationManager.AppSettings.Get("kafka:server");
            if (config != null) {
                //const String pattern = "^(?<ip>(?:\\d{1,3}\\.){3}\\d{1,3})\\:(?<port>\\d+)$";
                const String pattern = @"^(?<host>[^\:]+)\:(?<port>\d+)$";
                var match = Regex.Match(config, pattern);
                if (match.Success) {
                    var host = match.Groups["host"].Value;
                    var port = Int32.Parse(match.Groups["port"].Value);
                    return new Option(host, port);
                }
                else {
                    throw new ConfigurationErrorsException("AppSettings of \"kafka:server\"=\"{ip}:{port}\" incorrect");
                }
            }
            else {
                throw new ConfigurationErrorsException("AppSettings of \"kafka:server\"=\"{ip}:{port}\" missing");
            }
        }

        public Option(String host, Int32 port,
            Int32 maxBufferPoolSize  = 1024 * 1024,
            Int32 maxBufferSize      = 4096,
            Int32 requestBufferSize  = 4096,
            Int32 responseBufferSize = 1024 * 64) {
            Host               = host;
            Port               = port;
            MaxBufferPoolSize  = maxBufferPoolSize;
            MaxBufferSize      = maxBufferSize;
            RequestBufferSize  = requestBufferSize;
            ResponseBufferSize = responseBufferSize;
        }

        public Boolean Equals(Option other) {
            return String.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase)
                && Port == other.Port;
        }

        public override Boolean Equals(Object obj) {
            return (obj != null)
                && obj is Option
                && Equals((Option)obj);
        }

        public override Int32 GetHashCode() {
            return (Host != null ? Host.GetHashCode() : 0)
                ^ Port.GetHashCode();
        }

        public static implicit operator IPEndPoint(Option option) {
            var addresses = Dns.GetHostAddresses(option.Host);
            if (addresses.Length == 0) {
                throw new InvalidOperationException();
            }
            return new IPEndPoint(addresses[0], option.Port);
        }
    }
}
