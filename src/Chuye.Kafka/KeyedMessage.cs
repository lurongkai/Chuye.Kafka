using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka {
    public struct KeyedMessage : IEquatable<KeyedMessage> {
        public String Key;
        public String Message;

        public KeyedMessage(String key, String message) {
            Key     = key;
            Message = message;
        }

        public override String ToString() {
            if (Key == null) {
                return Message;
            }
            return String.Format("#{0}# {1}", Key, Message);
        }

        public Boolean Equals(KeyedMessage other) {
            return ((Key == null && other.Key == null)
                    || (Key != null && other.Key != null && Key == other.Key))
                && ((Message == null && other.Message == null)
                    || (Message != null && other.Message != null && Message == other.Message));
        }

        public override Boolean Equals(Object obj) {
            return obj != null && obj is KeyedMessage && Equals((KeyedMessage)obj);
        }

        public override Int32 GetHashCode() {
            return (Key == null ? 0 : Key.GetHashCode())
                ^ (Message != null ? 0 : Message.GetHashCode());
        }
        
        public static implicit operator KeyedMessage(String value) {
            return new KeyedMessage(null, value);
        }

        public static implicit operator KeyedMessage(KeyValuePair<String, String> pair) {
            return new KeyedMessage(pair.Key, pair.Value);
        }

        public static implicit operator KeyValuePair<String, String>(KeyedMessage message) {
            return new KeyValuePair<String, String>(message.Key, message.Message);
        }
    }
}
