using System;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Chuye.Kafka.Utils {

    [DebuggerStepThrough]
    public static class DateTimeExtension {

        //((DateTime.UtcNow.Ticks - DateTime.Parse("01/01/1970 00:00:00").Ticks) / 10000000).Dump();
        //((DateTime.Now.ToUniversalTime().Ticks - 621355968000000000) / 10000000).Dump();
        //((Int64)(DateTime.Now - new DateTime(1970, 1, 1, 0, 0, 0, DateTime.Now.Kind)).TotalSeconds).Dump();
        public static Int64 ToTimestamp(this DateTime time) {
            return (Int64)(time - new DateTime(1970, 1, 1, 0, 0, 0, time.Kind)).TotalSeconds;
        }

        public static DateTime FromTimestamp(Int64 totalSeconds, DateTimeKind kind = DateTimeKind.Utc) {
            return new DateTime(1970, 1, 1, 0, 0, 0, kind).AddSeconds(totalSeconds);
        }

        /// <summary>
        /// Determines whether the source string is a valid Base64 encoded value.
        /// </summary>
        /// <param name="s">The source string to be checked.</param>
        /// <returns>True if it is a valid Base64 encoded value, otherwise, false.</returns>
        public static Boolean IsBase64String(String s) {
            s = s.Trim();
            return (s.Length % 4 == 0) && Regex.IsMatch(s, @"^[a-zA-Z0-9\+/]*={0,3}$", RegexOptions.None);
        }

    }
}