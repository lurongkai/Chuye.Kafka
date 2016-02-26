using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Chuye.Kafka.Serialization {
    public class BufferReader {
        private Int32 _startOffset;
        private Int32 _currentOffset;
        private readonly Byte[] _bytes;

        public Int32 Offset {
            get { return _currentOffset; }
        }

        public Int32 Count {
            get { return _currentOffset - _startOffset; }
        }

        public BufferReader(ArraySegment<Byte> buffer)
            : this(buffer.Array, buffer.Offset) {
        }

        public BufferReader(Byte[] bytes, Int32 offset) {
            _startOffset = offset;
            _bytes = bytes;
        }

        public Byte ReadByte() {
            return (Byte)_bytes[_currentOffset++];
        }

        public Byte[] ReadBytes() {
            var length = ReadInt32();
            if (length == -1) {
                return null;
            }
            if (length == 0) {
                return new Byte[0];
            }
            if (length < 0) {
                Debug.WriteLine("Error length of value {0}", length);
                return null;
            }
            var buffer = new Byte[length];
            for (int i = 0; i < length; i++) {
                buffer[i] = _bytes[_currentOffset++];
            }
            return buffer;
        }

        public Int16 ReadInt16() {
            var buffer = new Byte[2];
            buffer[1] = _bytes[_currentOffset++];
            buffer[0] = _bytes[_currentOffset++];            
            return BitConverter.ToInt16(buffer, 0);
        }

        public Int32 ReadInt32() {
            var buffer = new Byte[4];
            buffer[3] = _bytes[_currentOffset++];
            buffer[2] = _bytes[_currentOffset++];
            buffer[1] = _bytes[_currentOffset++];
            buffer[0] = _bytes[_currentOffset++];
            return BitConverter.ToInt32(buffer, 0);
        }

        public Int64 ReadInt64() {
            var buffer = new Byte[8];
            buffer[7] = _bytes[_currentOffset++];
            buffer[6] = _bytes[_currentOffset++];
            buffer[5] = _bytes[_currentOffset++];
            buffer[4] = _bytes[_currentOffset++];
            buffer[3] = _bytes[_currentOffset++];
            buffer[2] = _bytes[_currentOffset++];
            buffer[1] = _bytes[_currentOffset++];
            buffer[0] = _bytes[_currentOffset++];
            return BitConverter.ToInt64(buffer, 0);
        }
        
        public String ReadString() {
            var length = ReadInt16();
            if (length == -1) {
                return null;
            }
            if (length == 0) {
                return String.Empty;
            }
            var buffer = new Byte[length];
            for (int i = 0; i < length; i++) {
                buffer[i] = _bytes[_currentOffset++];
            }            
            return Encoding.UTF8.GetString(buffer);
        }
    }
}