package server

// Site N Replicas        [Cloud-1] [Cloud-5] [Cloud-8]
//                             |________|_________|
//                                      |
//                                      |
//                                      y
//                                      |
//                                      |
// Sync Session Coordinator         [Cloud-1]   B (necessarily a site N replica owner)
//                                      |
//                                      |
//                                      x
//                                      |
//                                      |
// Relay Connection Point           [Cloud-4]   A (not necessarily a site N replica owner)
//                                      |
//                                      |
//                                      x
//                                      |
//                                      |
//                                   [Relay]
//
// (A) accepts relay connections and selects a sync coordinator for that relay from the relay's sites's replica nodes
//     proxies connection to that coordinator
// (B) accepts incoming sync session requests from the relay and decides on a responder in a randomized way
//     and periodically chooses one of the site N replicas owners to initiate a sync session with the relay
//
// When an update occurs for a site database a node sends a push message to all currently connected relays for
// which it is acting as a sync coordinator
//
//
// Site N Replicas             -    [Cloud-5] [Cloud-8]
//                                      |         |
//                                      |         |
//                                      y         y
//                                      |         |
//                                      |_________|
//                                      |
//                                  [Cloud-1]
//
// SYNC SESSIONS
// 
// 
// Relay->Cloud: [Relay] -------------------> [Coordinator] --------POST /relays/{relayID}/syncsessions/{sessionID}----------> [Sync Session Owner]
// Relay<-Cloud: [Relay] <------------------- [Coordinator] <-------POST /relays/{relayID}/syncsessions/{sessionID}----------- [Sync Session Owner]
// 
// OPTION 1: Sync Session State Maintained Seperately
// Sync Coordinator Decides It's Time One of the Replica Nodes Initiates A Sync Session
//   1) POST /relays/{relayID}/syncsessions - ask node to create a responder sync session
//   2) POST /relays/{relayID}/syncsessions/{sessionID}/messages
//      If this node is the sync session coordinator for this relay forward the message to the relay and return
//      If message.Direction == REQUEST
//        If this node contains a corresponding responder session for the relay then forward the message to that session
//      If message.Direction == RESPONSE
//        If this node contains a corresponding responder session for the relay then forward the message to that session
//      If message.Direction == PUSH
//   Send POST /syncsessions to selected initiator
// Read repair on ranged reads and single element reads
// 
// OPTION 2: Sync Session State Maintained At Coordinator
// The sync coordinator for a relay's connection maintains all sync session but proxies node state requests to a designated node for each session (or perhaps locally)
// Would need to implement a Proxy Bucket which contains a NodeProxy
// NodeProxy provides similar access methods but they instead do RPC across network to the designated node for the session


// If read repair is in place is there any point in distributing sync sessions per relay over all relays? Best effort to write updates to other replicas instead?
// [Relay] ---------- [Cloud-1]       [Cloud-1] [Cloud-5] [Cloud-6] Cloud 5 and 6 are more up to date than Cloud-1 but Cloud-1 is the one Relay is syncing with.
//     relay wouldn't get the latest values
// 
// 
// 
// 
// 
// 