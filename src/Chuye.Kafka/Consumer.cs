using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    public class Consumer {

        public IEnumerable<String> Fetch(String topicName, Int64 fetchOffset) {
            var request = new FetchRequest();
            request.ReplicaId = -1;
            //e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k 
            // would allow the server to wait up to 100ms  
            // to try to accumulate 64k of data before responding
            request.MaxWaitTime = 100;
            request.MinBytes = 64 * 1024;
            request.TopicPartitions = new TopicPartition[1];
            var topicPartition
                = request.TopicPartitions[0]
                = new TopicPartition();
            topicPartition.TopicName = topicName;
            topicPartition.FetchOffsetDetails = new FetchOffsetDetail[1];
            var fetchOffsetDetail
                = topicPartition.FetchOffsetDetails[0]
                = new FetchOffsetDetail();
            fetchOffsetDetail.FetchOffset = fetchOffset;
            fetchOffsetDetail.MaxBytes = 64 * 1024;

            var client = new Client();
            var response = (FetchResponse)(client.Send(request));

            return response.Items.SelectMany(x => x.MessageBodys)
                .SelectMany(x => x.MessageSets.Items)
                .Select(x => x.Message)
                .Select(x => Encoding.UTF8.GetString(x.Value));
        }
    }
}
