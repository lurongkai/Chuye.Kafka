using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    internal static class BufferReaderExtension {
        public static Int32[] ReadInt32Array(this BufferReader reader) {
            var size = reader.ReadInt32();
            if (size == -1) {
                return null;
            }
            var array = new Int32[size];
            for (int i = 0; i < size; i++) {
                array[i] = reader.ReadInt32();
            }
            return array;
        }

        public static Int64[] ReadInt64Array(this BufferReader reader) {
            var size = reader.ReadInt32();
            if (size == -1) {
                return null;
            }
            var array = new Int64[size];
            for (int i = 0; i < size; i++) {
                array[i] = reader.ReadInt64();
            }
            return array;
        }

        public static String[] ReadStrings(this BufferReader reader) {
            var size = reader.ReadInt32();
            if (size == -1) {
                return null;
            }
            var array = new String[size];
            for (int i = 0; i < size; i++) {
                array[i] = reader.ReadString();
            }
            return array;
        }

        public static T[] ReadArray<T>(this BufferReader reader) where T : IReadable, new() {
            var size = reader.ReadInt32();
            if (size == -1) {
                return null;
            }

            var array = new T[size];
            for (int i = 0; i < size; i++) {
                array[i] = new T();
                array[i].FetchFrom(reader);
            }
            return array;
        }
    }
}
