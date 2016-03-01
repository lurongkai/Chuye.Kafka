using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka {
    public class KeyedMessage : IEquatable<KeyedMessage> {
        public String Key { get; set; }
        public String Message { get; set; }

        public KeyedMessage(String key, String message) {
            Key = key;
            Message = message;
        }

        public override String ToString() {
            if (Key == null) {
                return Message;
            }
            return String.Format("#{0}# {1}", Key, Message);
        }

        public Boolean Equals(KeyedMessage other) {
            return other != null
                && String.Equals(Key, other.Key, StringComparison.Ordinal)
                && String.Equals(Message, other.Message, StringComparison.Ordinal);
        }

        public override Boolean Equals(Object other) {
            return other != null 
                && other is KeyedMessage 
                && Equals((KeyedMessage)other);
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

    public class OffsetKeyedMessage : KeyedMessage {
        public Int64 Offset { get; set; }

        public OffsetKeyedMessage(Int64 offset, String key, String message)
            : base(key, message) {
            Offset = offset;
        }

        public override String ToString() {
            if (Key == null) {
                return String.Format("{0}\t{1}", Offset, Message);
            }
            return String.Format("{0}\t#{1}#\t\t{2}", Offset, Key, Message);
        }
    }
}
