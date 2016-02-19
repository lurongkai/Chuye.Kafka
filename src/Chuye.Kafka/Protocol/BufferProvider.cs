using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace Chuye.Kafka.Protocol {
    interface IBufferProvider {
        Byte[] Borrow();
        void GiveBack(Byte[] buffer);
    }

    class BufferProvider : IBufferProvider {
        private const Int32 Capacity = 4096;
        private readonly ConcurrentStack<WeakReference> _buffers;

        public BufferProvider() {
            _buffers = new ConcurrentStack<WeakReference>();
        }

        public Byte[] Borrow() {
            WeakReference item;
            while (!_buffers.IsEmpty) {
                if (_buffers.TryPop(out item) && item.IsAlive) {
                    return (Byte[])item.Target;
                }
            }

            var buffer = new Byte[Capacity];
            item = new WeakReference(buffer, false);
            _buffers.Push(item);
            return buffer;
        }

        public void GiveBack(Byte[] buffer) {
            var item = new WeakReference(buffer, false);
            _buffers.Push(item);
        }
    }
}
