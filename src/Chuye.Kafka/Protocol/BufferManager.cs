using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace Chuye.Kafka.Protocol {
    interface IBufferWrapper : IDisposable {
        ArraySegment<Byte> Segment { get; }
    }

    interface IBufferManager {
        IBufferWrapper Borrow();
    }

    class BufferManager : IBufferManager {
        private Int32 _bufferSize;
        private readonly Byte[] _totalBytes;
        private readonly ConcurrentStack<Int32> _bufferOffsets;

        public BufferManager(Int32 bufferSize, Int32 bufferBlock) {
            _bufferSize = bufferSize;
            _totalBytes = new Byte[_bufferSize * bufferBlock];
            _bufferOffsets = new ConcurrentStack<Int32>();
            _bufferOffsets.PushRange(
                Enumerable.Range(0, bufferBlock)
                    .Select(r => _bufferSize * r)
                    .Reverse()
                    .ToArray());
        }

        public IBufferWrapper Borrow() {
            Int32 bufferOffset;
            if (_bufferOffsets.TryPop(out bufferOffset)) {
                return new BufferWrapper(this,
                    new ArraySegment<Byte>(_totalBytes, bufferOffset, _bufferSize));
            }
            //todo: stuff 
            throw new InvalidOperationException("Buffer out of use");
        }

        private void GiveBack(IBufferWrapper buffer) {
            _bufferOffsets.Push(buffer.Segment.Offset);
        }

        private class BufferWrapper : IBufferWrapper {
            private readonly BufferManager _bufferProvider;
            private readonly ArraySegment<Byte> _buffer;

            public BufferWrapper(BufferManager bufferProvider, ArraySegment<Byte> buffer) {
                _bufferProvider = bufferProvider;
                _buffer = buffer;
            }

            public ArraySegment<Byte> Segment {
                get { return _buffer; }
            }

            public void Dispose() {
                _bufferProvider.GiveBack(this);
            }
        }
    }
}
