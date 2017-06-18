/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

// thanks to http://stackoverflow.com/questions/6942273/get-random-element-from-container
#include "MP1Node.h"

#include <sstream>
#include <memory>

template<typename Iter, typename RandomGenerator>
Iter select_randomly(Iter start, Iter end, RandomGenerator& g) {
    std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
    std::advance(start, dis(g));
    return start;
}

template<typename Iter>
Iter select_randomly(Iter start, Iter end) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    return select_randomly(start, end, gen);
}

template<typename Out>
void split(const std::string &s, char delim, Out result) {
    std::stringstream ss;
    ss.str(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        *(result++) = item;
    }
}


std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, std::back_inserter(elems));
    return elems;
}


/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt; 
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}


void MP1Node::updateMemberList(Address& address, long heartbeat) {
	int id = *(int*)(&address.addr);
	int port = *(short*)(&address.addr[4]);

    bool existing = false;
    bool updated = false;
    vector<MemberListEntry>::iterator it;
    for (it=memberNode->memberList.begin();it < memberNode->memberList.end(); it++) {
        // skip myself
        if (it == memberNode->myPos) {
         continue;
        }

        if (it->getid() == id && it->getport() == port) {
            existing = true;
            // FIXME update
            if (it->getheartbeat() < heartbeat) {
              it->setheartbeat(heartbeat);
              it->settimestamp(par->getcurrtime());
              updated = true;
            }
            break;
        } else {
         existing = false;   
        }
    }

    if (!existing) {
        MemberListEntry newEntry(id, port, heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(newEntry);
        log->logNodeAdd(&memberNode->addr, &address);
    }


}


void MP1Node::mergeMemberlist(Member* member, char* data, int size) {

    // Entry: Address(6 byte) + heartbeat(8 byte) 
    size_t entrySize = sizeof(memberNode->addr.addr) + sizeof(long);

    int offset = 0;
    Address sourceAddress;
    memcpy(&sourceAddress, data, sizeof(Address));
    offset += sizeof(Address);
	int totalEntries = -1;
    memcpy(&totalEntries,(data+offset), sizeof(int));
    offset += sizeof(int);


    cout << "address " << sourceAddress.getAddress() << endl;
    cout << "total entries " << totalEntries
         << ",remaining bytes " << (size-offset) << endl;
    for (int i = 0; i<totalEntries; i++) {
        if (size-offset < entrySize) {
            #ifdef DEBUGLOG
                const string msg = "cannot read further member entries " + to_string((size-offset));
                log->LOG(&memberNode->addr, msg.c_str());
            #endif
            exit(1);
        }
        Address entryAddress;
        long heartbeat;
        long timestamp;

        // read out single entry
        memcpy(&entryAddress, (data+offset), sizeof(Address));
        offset += sizeof(Address);
        memcpy(&heartbeat, (data+offset), sizeof(long));
        offset += sizeof(long);
        
        //final MemberListEntry entry(entryAddress.getid(),entryAddress.getport, heartbeat, timestamp);
        updateMemberList(entryAddress, heartbeat);
    }

}

bool MP1Node::handleHeartbeatRequest(Member* member, char* data, int size) {
    #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "received HEARTBEATREQ");
    #endif
    mergeMemberlist(member, data, size);
    return true;

}

bool MP1Node::handleJoinResponse(Member* member, char* data, int size) {
    cout << "handle join response" << endl;

    mergeMemberlist(member, data, size);
    memberNode->inGroup = true;
    return true;
}

bool MP1Node::handleJoinRequest(Member* member, char* data, int size) {
    cout << "handle join request, size " << size << endl;
    Address address;
    long heartbeat;
    memcpy(&address.addr, data, sizeof(Address));
    memcpy(&heartbeat, data + sizeof(address.addr), sizeof(long));


    updateMemberList(address, heartbeat);

    sendWithMemberList(JOINREP,&address);

 return true;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */

    MessageHdr* hdr = (MessageHdr*)data;

    bool result = false;

    size_t offset = sizeof(MessageHdr);
    switch(hdr->msgType) {
        case JOINREQ:
            result = handleJoinRequest((Member*)env,data + offset, size - offset);
            break;
        case JOINREP:
            result = handleJoinResponse((Member*)env,data + offset, size - offset);
            break;
        case HEARTBEATREQ:
            result = handleHeartbeatRequest((Member*)env,data + offset, size - offset);
            break;
    }





    return result;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    if (memberNode->memberList.size() < 1) {
        return;
    }

    vector<MemberListEntry>::iterator entry = select_randomly(memberNode->memberList.begin(),memberNode->memberList.end());

    if(entry == memberNode->myPos) {
        return;
    }

    cleanupMembers();

    Address address = buildAddress(entry->id, entry->port);
    cout << "me: " << memberNode->addr.getAddress()
         << ", gossip to" << address.getAddress()
         << endl;
    sendWithMemberList(HEARTBEATREQ,&address);

    return;
}

void MP1Node::cleanupMembers() {


    for (auto entry: kicklist) {
        
        if((par->getcurrtime() - entry.gettimestamp()) >= TREMOVE) {

          memberNode->memberList.erase(
              std::remove_if(memberNode->memberList.begin(),memberNode->memberList.end(),
              [&](MemberListEntry& subentry){
                  return (subentry.getid() == entry.getid()) && (subentry.getport() == entry.getport());
              }),
              memberNode->memberList.end()
          );
        }

    }

    kicklist.clear();

    std::for_each(memberNode->memberList.begin(),memberNode->memberList.end(),
    [&](MemberListEntry& entry){
      if((par->getcurrtime() - entry.gettimestamp()) >= TFAIL) {  
          //FIXME whatever
          kicklist.push_back(entry);
      }
    }); 



}

bool MP1Node::sendWithMemberList(MsgTypes msgType, Address* targetAddress) {


    // Entry: Address(6 byte) + heartbeat(8 byte) 
    size_t entrySize = sizeof(memberNode->addr.addr) + sizeof(long);
    // total size: Entry size(14 byte) * count memberlist
    int memberlistCount = memberNode->memberList.size();
    size_t totalSize = entrySize * memberlistCount;
    // MessageHdr(4 byte) + Address(6 byte) + Memberlist Size(4 byte) + memberlist size (Entry size 14 byte * count memberlist)
    size_t size = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) +  sizeof(int) +  totalSize;
    char* msg = (char*) malloc(size);
    int offset2 = 0;
    memcpy(msg, &msgType, sizeof(MsgTypes));
    offset2 += sizeof(MsgTypes);
    memcpy((char *)(msg+offset2), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    offset2 += sizeof(memberNode->addr.addr);
    memcpy((char *)(msg+offset2), &memberlistCount, sizeof(int));
    offset2 += sizeof(int);
    for(auto value: memberNode->memberList) {
        Address address = buildAddress(value.id, value.port);
        memcpy((msg+offset2), &address.addr, sizeof(address.addr));
        offset2 += sizeof(address.addr);
        memcpy((msg+offset2), &value.heartbeat, sizeof(long));
        offset2 += sizeof(long);
    }
    cout << "size - offset2 " << (size-offset2) << endl;
    emulNet->ENsend(&memberNode->addr, targetAddress, (char *)msg, size);
    cout << "try to free msg " << sizeof(msg) << endl;
    free(msg);
    cout << "done" << endl;
    msg=NULL;

    return true;
}

Address MP1Node::buildAddress(int id, short port) {
    return Address(to_string(id)+":"+to_string(port));
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();

	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

    MemberListEntry entry(id,port, 0, par->getcurrtime());
    memberNode->memberList.push_back(entry);
    memberNode->myPos = memberNode->memberList.begin();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
