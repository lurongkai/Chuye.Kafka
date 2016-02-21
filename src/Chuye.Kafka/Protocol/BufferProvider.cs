using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace Chuye.Kafka.Protocol {
    interface IBufferWrapper : IDisposable {
        Byte[] Buffer { get; }
    }

    interface IBufferProvider {
        IBufferWrapper Borrow();
    }

    class BufferProvider : IBufferProvider {
        private const Int32 Capacity = 4096;
        private readonly ConcurrentStack<WeakReference> _buffers;

        public BufferProvider() {
            _buffers = new ConcurrentStack<WeakReference>();
        }

        public IBufferWrapper Borrow() {
            WeakReference item;
            while (!_buffers.IsEmpty) {
                if (_buffers.TryPop(out item) && item.IsAlive) {
                    return new BufferWraper(this, (Byte[])item.Target);
                }
            }

            var buffer = new Byte[Capacity];
            item = new WeakReference(buffer, false);
            _buffers.Push(item);
            return new BufferWraper(this, buffer);
        }

        internal void GiveBack(IBufferWrapper buffer) {
            var item = new WeakReference(buffer.Buffer, false);
            _buffers.Push(item);
        }

        internal class BufferWraper : IBufferWrapper {
            private readonly IBufferProvider _bufferProvider;
            private readonly Byte[] _buffer;

            public BufferWraper(IBufferProvider bufferProvider, Byte[] buffer) {
                _bufferProvider = bufferProvider;
                _buffer = buffer;
            }

            public Byte[] Buffer {
                get { return _buffer; }
            }

            public void Dispose() {
                ((BufferProvider)_bufferProvider).GiveBack(this);
            }
        }
    }
}
