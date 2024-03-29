﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol {

    public enum ErrorCode : short {
        //No error--it worked!
        NoError = 0,
        //An unexpected server error
        Unknown = -1,
        //The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
        OffsetOutOfRange = 1,
        //This indicates that a message contents does not match its CRC
        InvalidMessage = 2,// Yes
        //This request is for a topic or partition that does not exist on this broker.
        UnknownTopicOrPartition = 3,// Yes
        //The message has a negative size
        InvalidMessageSize = 4,
        //This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
        LeaderNotAvailable = 5,// Yes
        //This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
        NotLeaderForPartition = 6,// Yes
        //This error is thrown if the request exceeds the user-specified time limit in the request.
        RequestTimedOut = 7,// Yes
        //This is not a client facing error and is used mostly by tools when a broker is not alive.
        BrokerNotAvailable = 8,
        //If replica is expected on a broker, but is not (this can be safely ignored).
        ReplicaNotAvailable = 9,
        //The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
        MessageSizeTooLarge = 10,
        //Internal error code for broker-to-broker communication.
        StaleControllerEpochCode = 11,
        //If you specify a string larger than configured maximum for offset metadata
        OffsetMetadataTooLargeCode = 12,
        //The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator.
        GroupLoadInProgressCode = 14,// Yes
        //The broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created, or if the group coordinator is not active.
        GroupCoordinatorNotAvailableCode = 15,// Yes
        //The broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for.
        NotCoordinatorForGroupCode = 16,// Yes
        //For a request which attempts to access an invalid topic (e.g. one which has an illegal name), or if an attempt is made to write to an internal topic (such as the consumer offsets topic).
        InvalidTopicCode = 17,
        //If a message batch in a produce request exceeds the maximum configured segment size.
        RecordListTooLargeCode = 18,
        //Returned from a produce request when the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1.
        NotEnoughReplicasCode = 19,// Yes
        //Returned from a produce request when the message was written to the log, but with fewer in-sync replicas than required.
        NotEnoughReplicasAfterAppendCode = 20,// Yes
        //Returned from a produce request if the requested requiredAcks is invalid (anything other than -1, 1, or 0).
        InvalidRequiredAcksCode = 21,
        //Returned from group membership requests (such as heartbeats) when the generation id provided in the request is not the current generation.
        IllegalGenerationCode = 22,
        //Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group.
        InconsistentGroupProtocolCode = 23,
        //Returned in join group when the groupId is empty or null.
        InvalidGroupIdCode = 24,
        //Returned from group requests (offset commits/fetches, heartbeats, etc) when the memberId is not in the current generation.
        UnknownMemberIdCode = 25,
        //Return in join group when the requested session timeout is outside of the allowed range on the broker
        InvalidSessionTimeoutCode = 26,
        //Returned in heartbeat requests when the coordinator has begun rebalancing the group. This indicates to the client that it should rejoin the group.
        RebalanceInProgressCode = 27,
        //This error indicates that an offset commit was rejected because of oversize metadata.
        InvalidCommitOffsetSizeCode = 28,
        //Returned by the broker when the client is not authorized to access the requested topic.
        TopicAuthorizationFailedCode = 29,
        //Returned by the broker when the client is not authorized to access a particular groupId.
        GroupAuthorizationFailedCode = 30,
        //Returned by the broker when the client is not authorized to use an inter-broker or administrative API.
        ClusterAuthorizationFailedCode = 31,
    }
}
