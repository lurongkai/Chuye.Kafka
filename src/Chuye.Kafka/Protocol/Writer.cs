using System;
using System.Text;

namespace Chuye.Kafka.Protocol {

    public class Writer {
        private readonly Byte[] _bytes;
        private Int32 _offset;

        public Writer(Byte[] bytes) {
            _bytes = bytes;
            _offset = 0;
        }
        
        public ArraySegment<Byte> Bytes {
            get { return new ArraySegment<byte>(_bytes, _offset, _offset); }
        }
        
        public IDisposable PrepareCrc() {
            var previousPosition = _offset;
            Write(0);
            return new CrcWriter(this, previousPosition);
        }

        public IDisposable PrepareLength() {
            var previousPosition = _offset;
            Write(0);
            return new PositionWriter(this, previousPosition);
        }

        public void Reset() {
            _offset = 0;
        }

        public Writer Write(Int64 value) {
            //Write(BitConverter.GetBytes(value).Reverse().ToArray(), false);
            _bytes[_offset++] = (byte)((value >> 56));
            _bytes[_offset++] = (byte)((value >> 48));
            _bytes[_offset++] = (byte)((value >> 40));
            _bytes[_offset++] = (byte)(value >> 32);
            _bytes[_offset++] = (byte)((value >> 24));
            _bytes[_offset++] = (byte)((value >> 16));
            _bytes[_offset++] = (byte)((value >> 8));
            _bytes[_offset++] = (byte)(value);
            return this;
        }

        public Writer Write(Int32 value) {
            //Write(BitConverter.GetBytes(value).Reverse().ToArray(), false);
            _bytes[_offset++] = (byte)((value >> 24));
            _bytes[_offset++] = (byte)((value >> 16));
            _bytes[_offset++] = (byte)((value >> 8));
            _bytes[_offset++] = (byte)(value);
            return this;
        }

        public Writer Write(Int16 value) {
            //Write(BitConverter.GetBytes(value).Reverse().ToArray(), false);
            _bytes[_offset++] = (byte)((value >> 8));
            _bytes[_offset++] = (byte)(value);
            return this;
        }

        public Writer Write(Byte value) {
            _bytes[_offset++] = value;
            return this;
        }

        public Writer Write(Byte[] value) {
            if (value == null) {
                Write(-1);
                return this;
            }

            Write((Int32)value.Length);
            Write(value, false);
            return this;
        }

        public Writer Write(String value) {
            if (value == null) {
                Write((Int16)(-1));
                return this;
            }

            Write((Int16)value.Length);
            Write(Encoding.UTF8.GetBytes(value), false);
            return this;
        }

        private void Write(Byte[] bytes, Boolean withLength) {
            if (withLength) {
                Write(_bytes.Length);
            }
            bytes.CopyTo(_bytes, _offset);
            //Array.Copy(_bytes, 0, _bytes, _offset++, _bytes.Length);
            _offset += bytes.Length;
        }

        private class CrcWriter : IDisposable {
            private Int32 _previousPosition;
            private Writer _writer;

            public CrcWriter(Writer writer, Int32 previousPosition) {
                _writer = writer;
                _previousPosition = previousPosition;
            }

            public void Dispose() {
                var currentPosition = _writer._offset;
                var subsequent = (Int32)(currentPosition - _previousPosition - 4);
                var crc = KafkaNet.Common.Crc32Provider.Compute(_writer._bytes, _previousPosition + 4, subsequent);
                _writer._offset = _previousPosition;
                _writer.Write((Int32)crc);
                _writer._offset = currentPosition;
            }
        }

        private class PositionWriter : IDisposable {
            private Int32 _previousPosition;
            private Writer _writer;

            public PositionWriter(Writer writer, Int32 previousPosition) {
                _writer = writer;
                _previousPosition = previousPosition;
            }

            public void Dispose() {
                var currentPosition = _writer._offset;
                var subsequent = (Int32)(currentPosition - _previousPosition - 4);
                _writer._offset = _previousPosition;
                _writer.Write(subsequent);
                _writer._offset = currentPosition;
            }
        }
    }
}