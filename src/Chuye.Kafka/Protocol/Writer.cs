using System;
using System.Text;

namespace Chuye.Kafka.Protocol {
    public interface ICompute : IDisposable {
        Int32 Value { get; }
    }


    public class Writer {
        private readonly Byte[] _bytes;
        private Int32 _startOffset;
        private Int32 _currentOffset;

        public Writer(ArraySegment<Byte> buffer)
            : this(buffer.Array, buffer.Offset) {
        }

        public Writer(Byte[] bytes, Int32 offset) {
            _bytes         = bytes;
            _startOffset   = offset;
            _currentOffset = offset;
        }

        public Int32 Count {
            get { return _currentOffset - _startOffset; }
        }

        public ICompute PrepareCrc() {
            var previousPosition = _currentOffset;
            Write(0);
            return new CrcWriter(this, previousPosition);
        }

        public ICompute PrepareLength() {
            var previousPosition = _currentOffset;
            Write(0);
            return new PositionWriter(this, previousPosition);
        }

        public void Reset() {
            _currentOffset = 0;
        }

        public Writer Write(Int64 value) {
            //Write(BitConverter.GetBytes(value).Reverse().ToArray(), false);
            _bytes[_currentOffset++] = (byte)((value >> 56));
            _bytes[_currentOffset++] = (byte)((value >> 48));
            _bytes[_currentOffset++] = (byte)((value >> 40));
            _bytes[_currentOffset++] = (byte)(value >> 32);
            _bytes[_currentOffset++] = (byte)((value >> 24));
            _bytes[_currentOffset++] = (byte)((value >> 16));
            _bytes[_currentOffset++] = (byte)((value >> 8));
            _bytes[_currentOffset++] = (byte)(value);
            return this;
        }

        public Writer Write(Int32 value) {
            //Write(BitConverter.GetBytes(value).Reverse().ToArray(), false);
            _bytes[_currentOffset++] = (byte)((value >> 24));
            _bytes[_currentOffset++] = (byte)((value >> 16));
            _bytes[_currentOffset++] = (byte)((value >> 8));
            _bytes[_currentOffset++] = (byte)(value);
            return this;
        }

        public Writer Write(Int16 value) {
            //Write(BitConverter.GetBytes(value).Reverse().ToArray(), false);
            _bytes[_currentOffset++] = (byte)((value >> 8));
            _bytes[_currentOffset++] = (byte)(value);
            return this;
        }

        public Writer Write(Byte value) {
            _bytes[_currentOffset++] = value;
            return this;
        }

        public Writer Write(Byte[] bytes) {
            if (bytes == null) {
                Write(-1);
                return this;
            }

            Write((Int32)bytes.Length);
            bytes.CopyTo(_bytes, _currentOffset);
            //Array.Copy(_bytes, 0, _bytes, _offset++, _bytes.Length);
            _currentOffset += bytes.Length;
            return this;
        }

        public Writer Write(String value) {
            if (value == null) {
                Write((Int16)(-1));
                return this;
            }

            Write((Int16)value.Length);
            var bytes = Encoding.UTF8.GetBytes(value);
            bytes.CopyTo(_bytes, _currentOffset);
            //Array.Copy(_bytes, 0, _bytes, _offset++, _bytes.Length);
            _currentOffset += value.Length;
            return this;
        }

        private class CrcWriter : ICompute {
            private Int32 _previousPosition;
            private Writer _writer;

            public Int32 Value { get; private set; }

            public CrcWriter(Writer writer, Int32 previousPosition) {
                _writer = writer;
                _previousPosition = previousPosition;
            }

            public void Dispose() {
                var currentPosition = _writer._currentOffset;
                var subsequent = (Int32)(currentPosition - _previousPosition - 4);
                var crc = (Int32)KafkaNet.Common.Crc32Provider.Compute(_writer._bytes, _previousPosition + 4, subsequent);
                _writer._currentOffset = _previousPosition;
                _writer.Write(crc);
                _writer._currentOffset = currentPosition;

                Value = crc;
            }
        }

        private class PositionWriter : ICompute {
            private Int32 _previousPosition;
            private Writer _writer;

            public Int32 Value { get; private set; }

            public PositionWriter(Writer writer, Int32 previousPosition) {
                _writer = writer;
                _previousPosition = previousPosition;
            }

            public void Dispose() {
                var currentPosition = _writer._currentOffset;
                var length = (Int32)(currentPosition - _previousPosition - 4);
                _writer._currentOffset = _previousPosition;
                _writer.Write(length);
                _writer._currentOffset = currentPosition;

                Value = length;
            }
        }
    }
}