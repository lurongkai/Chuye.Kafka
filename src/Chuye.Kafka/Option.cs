using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Chuye.Kafka {
    public struct Option {
        public String Host;
        public Int32 Port;

        public static Option LoadDefault() {
            var config = ConfigurationManager.AppSettings.Get("kafka:server");
            if (config != null) {
                const String pattern = "^(?<ip>(?:\\d{1,3}\\.){3}\\d{1,3})\\:(?<port>\\d+)$";
                var match = Regex.Match(config, pattern);
                if (match.Success) {
                    var host = match.Groups["ip"].Value;
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

        public Option(String host, Int32 port) {
            Host = host;
            Port = port;
        }
    }
}
