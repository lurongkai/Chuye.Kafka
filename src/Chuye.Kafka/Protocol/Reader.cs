using System;
using System.IO;
using System.Linq;
using System.Text;

namespace Chuye.Kafka.Protocol {

    public class Reader {
        private Int32 _offset;
        private readonly Byte[] _buffer;

        public Reader(Byte[] buffer) {
            _offset = 0;
            _buffer = buffer;
        }

        public Byte ReadByte() {
            return (Byte)_buffer[_offset++];
        }

        public Byte[] ReadBytes() {
            var length = ReadInt32();
            if (length == -1) {
                return null;
            }
            if (length == 0) {
                return new Byte[0];
            }
            var buffer = new Byte[length];
            for (int i = 0; i < length; i++) {
                buffer[length - i - 1] = _buffer[_offset++];
            }
            return buffer;
        }

        public Int16 ReadInt16() {
            var buffer = new Byte[2];
            buffer[1] = _buffer[_offset++];
            buffer[0] = _buffer[_offset++];            
            return BitConverter.ToInt16(buffer, 0);
        }

        public Int32 ReadInt32() {
            var buffer = new Byte[4];
            buffer[3] = _buffer[_offset++];
            buffer[2] = _buffer[_offset++];
            buffer[1] = _buffer[_offset++];
            buffer[0] = _buffer[_offset++];
            return BitConverter.ToInt32(buffer, 0);
        }

        //public Int32[] ReadInt32Array() {
        //    var length = ReadInt32();
        //    if (length == -1) {
        //        return null;
        //    }
        //    var array = new Int32[length];
        //    for (int i = 0; i < length; i++) {
        //        array[i] = ReadInt32();
        //    }
        //    return array;
        //}

        public Int64 ReadInt64() {
            var buffer = new Byte[8];
            buffer[7] = _buffer[_offset++];
            buffer[6] = _buffer[_offset++];
            buffer[5] = _buffer[_offset++];
            buffer[4] = _buffer[_offset++];
            buffer[3] = _buffer[_offset++];
            buffer[2] = _buffer[_offset++];
            buffer[1] = _buffer[_offset++];
            buffer[0] = _buffer[_offset++];
            return BitConverter.ToInt64(buffer, 0);
        }

        //public Int64[] ReadInt64Array() {
        //    var length = ReadInt32();
        //    if (length == -1) {
        //        return null;
        //    }
        //    var array = new Int64[length];
        //    for (int i = 0; i < length; i++) {
        //        array[i] = ReadInt64();
        //    }
        //    return array;
        //}

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
                buffer[i] = _buffer[_offset++];
            }            
            return Encoding.UTF8.GetString(buffer);
        }
    }
}