using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    internal static class BufferWriterExtension {

        public static BufferWriter Write(this BufferWriter writer, Int32[] value) {
            if (value == null) {
                writer.Write(-1);
                return writer;
            }

            writer.Write((Int32)value.Length);
            foreach (var item in value) {
                writer.Write(item);
            }
            return writer;
        }

        public static BufferWriter Write(this BufferWriter writer, Int64[] value) {
            if (value == null) {
                writer.Write(-1);
                return writer;
            }

            writer.Write((Int32)value.Length);
            foreach (var item in value) {
                writer.Write(item);
            }
            return writer;
        }

        public static BufferWriter Write(this BufferWriter writer, String[] value) {
            if (value == null) {
                writer.Write(-1);
                return writer;
            }

            writer.Write((Int32)value.Length);
            foreach (var item in value) {
                writer.Write(item);
            }
            return writer;
        }

        public static BufferWriter Write(this BufferWriter writer, IEnumerable<IWriteable> array) {
            if (array == null) {
                writer.Write(-1);
                return writer;
            }
            var arraySizeWriter = new BufferWriter.ArraySizeWriter(writer, writer.Offset);
            var size = 0;
            foreach (var item in array) {
                item.SaveTo(writer);
                size++;
            }
            arraySizeWriter.SetArraySize(size);
            arraySizeWriter.Dispose();
            return writer;
        }
    }
}
