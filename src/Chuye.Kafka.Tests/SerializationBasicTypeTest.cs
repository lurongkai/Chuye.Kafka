using System;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using Chuye.Kafka.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class SerializationBasicTypeTest {
        [TestMethod]
        public void Reading_writing_reversible() {
            var bytes = new Byte[10240];
            var writer = new BufferWriter(bytes, 0);
            var reader = new BufferReader(bytes, 0);

            var rand = new Random();
            var byte1 = (Byte)rand.Next(byte.MinValue, byte.MaxValue);
            writer.Write(byte1);
            var byte2 = (Byte)reader.ReadByte();
            Assert.AreEqual(byte1, byte2);
            Assert.AreEqual(writer.Offset, reader.Offset);

            var short1 = (short)rand.Next(short.MinValue, short.MaxValue);
            writer.Write(short1);
            var short2 = reader.ReadInt16();
            Assert.AreEqual(short1, short2);
            Assert.AreEqual(writer.Offset, reader.Offset);

            var int1 = (int)rand.Next(int.MinValue, int.MaxValue);
            writer.Write(int1);
            var int2 = reader.ReadInt32();
            Assert.AreEqual(int1, int2);
            Assert.AreEqual(writer.Offset, reader.Offset);

            var long1 = (long)rand.Next(int.MinValue, int.MaxValue);
            writer.Write(long1);
            var long2 = reader.ReadInt64();
            Assert.AreEqual(long1, long2);
            Assert.AreEqual(writer.Offset, reader.Offset);

            var str1 = Guid.NewGuid().ToString();
            writer.Write(str1);
            var strs = reader.ReadString();
            Assert.AreEqual(str1, strs);
            Assert.AreEqual(writer.Offset, reader.Offset);

            var bytes1 = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());
            writer.Write(bytes1);
            var bytes2 = reader.ReadBytes();

            //Assert.AreEqual(bytes1, bytes2);
            Assert.IsTrue(bytes1.Select((b, i) => new { b, i })
                .All(x => x.b == bytes2[x.i]));
            Assert.AreEqual(writer.Offset, reader.Offset);
        }
    }
}
