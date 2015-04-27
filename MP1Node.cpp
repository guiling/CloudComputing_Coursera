/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

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
        addToMembershipList(memberNode->addr.addr, memberNode->heartbeat, par->globaltime);

    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

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
    
    return 1;
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

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	
    MessageHdr * messageType = (MessageHdr *) data;
    if(messageType->msgType == JOINREQ)
    {
       
        long heartbeat = 0;
        char * addr = (char *) malloc(sizeof(memberNode->addr.addr));

        memcpy(addr,(char*)(messageType+1), sizeof(memberNode->addr.addr));
        memcpy(&heartbeat, (char *)(messageType+1) + 1 + sizeof(memberNode->addr.addr), sizeof(long));
        addToMembershipList(addr, heartbeat, par->globaltime);

        #ifdef DEBUGLOG

        Address *joinedAddress = constructAddress(addr);
        log->logNodeAdd(&memberNode->addr, joinedAddress);

        // char s[1024];
        // sprintf(s, "heartbeat : %ld", heartbeat);
        // log->LOG(&memberNode->addr, s);
        #endif
        // Send Join Reply message and all its membership info
        sendSelfMembershipMessage(addr, JOINREP);

        free(addr);
    }
    else if(messageType->msgType == JOINREP)
    {
        memberNode->inGroup = true;
        updateMembershipList(messageType, size);

    }
    else if(messageType->msgType == PING)
    {
        updateMembershipList(messageType, size);
    }

    return true;
}

void MP1Node::updateMembershipList(MessageHdr * data, int size)
{
    int readMsgSize = sizeof(MessageHdr);
    MembershipInfo * newMembershipInfo = (MembershipInfo *) malloc(sizeof(MembershipInfo));
    
    while(readMsgSize < size)
    {
        memcpy(newMembershipInfo, (char*)data + readMsgSize, sizeof(MembershipInfo));

        // check whether the reveived item in self membership
        bool isnew = true;
        int memberNumber = memberNode->memberList.size();

        for (int i = 0; i < memberNumber; i++)
        {
            if(isSameMemberInGroup(&memberNode->memberList[i], newMembershipInfo))
            {
                isnew =false;
                if(newMembershipInfo->heartbeat > memberNode->memberList[i].heartbeat)
                {
                    memberNode->memberList[i].heartbeat = newMembershipInfo->heartbeat;
                    memberNode->memberList[i].timestamp = newMembershipInfo->timestamp ;
                }

                break;
            }
        }

        if(isnew)
        {
            addToMembershipList(newMembershipInfo->id,
                                newMembershipInfo->port,
                                newMembershipInfo->heartbeat,
                                newMembershipInfo->timestamp);

            if(!isSameAddress(newMembershipInfo->id, newMembershipInfo->port))
            {
                
                #ifdef DEBUGLOG
                
                Address *joinedAddress = constructAddress(newMembershipInfo->id, newMembershipInfo->port);
                log->logNodeAdd(&memberNode->addr, joinedAddress);
                
                #endif
            }
        }
        
        readMsgSize = readMsgSize + sizeof(MembershipInfo);
    }

    free(newMembershipInfo);

}

bool MP1Node::isSameMemberInGroup(MemberListEntry *listEntry, MembershipInfo * reveivedMembershipInfo)
{
    if(reveivedMembershipInfo->id == listEntry->id && reveivedMembershipInfo->port == listEntry->port)
    {
        return true;
    }

    return false;

}

void MP1Node::addToMembershipList(char * newMemberAddress, long heartbeat, int timestamp)
{
    int id = 0;
    short port = 0;
    memcpy(&id, &newMemberAddress[0], sizeof(int));
    memcpy(&port, &newMemberAddress[4], sizeof(short));
    addToMembershipList(id, port, heartbeat, timestamp);
}

void MP1Node::addToMembershipList(int id, int port, long heartbeat, int timestamp)
{
    MemberListEntry memberEntry(id, port, heartbeat, timestamp);
    memberNode->memberList.push_back(memberEntry);
    memberNode->nnb ++ ;
}

void MP1Node::sendSelfMembershipMessage(char * targetAddress, MsgTypes type)
{
    MessageHdr *msg;
    int memberNumber = memberNode->memberList.size();
    
    int sendNumber = 0;
    for(int i = 0; i < memberNumber;i++)
    {
        if(par->globaltime - memberNode->memberList[i].timestamp > TFAIL)
        {
            continue;
        }

        sendNumber ++;
    }

    size_t size = sizeof(MembershipInfo) * sendNumber + sizeof(MessageHdr);

    msg = (MessageHdr *)malloc(size * sizeof(char));
    msg->msgType = type;

    int sendCount = 0;
    for (int i = 0; i < memberNumber; i++)
    {
        if(par->globaltime - memberNode->memberList[i].timestamp > TFAIL)
        {
            continue;
        }

        MembershipInfo membershipInfo;
        membershipInfo.id = memberNode->memberList[i].id;
        membershipInfo.port = memberNode->memberList[i].port;
        membershipInfo.heartbeat = memberNode->memberList[i].heartbeat;
        membershipInfo.timestamp = memberNode->memberList[i].timestamp;
        memcpy((char*)(msg + 1) + sizeof(MembershipInfo) * sendCount, &membershipInfo,sizeof(MembershipInfo));  

        sendCount ++; 
    }

    Address *toAddress = constructAddress(targetAddress);
    emulNet->ENsend(&memberNode->addr, toAddress, (char *)msg, size);

    free(msg);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    // increment node heartbeat
    memberNode->heartbeat ++;

    // find self note in memberentry list, then increment its heartbeat
    int memberNumber = memberNode->memberList.size();

    for (int i = 0; i < memberNumber; i++)
    {
        if(isSameAddress(memberNode->memberList[i].id, memberNode->memberList[i].port))
        {
            memberNode->memberList[i].heartbeat ++;
            memberNode->memberList[i].timestamp = par->globaltime;
            break;
        }

    }

    // check if any node hasn't responded within a timeout period and then delete
    for(memberNode->myPos = memberNode->memberList.begin(); memberNode->myPos != memberNode->memberList.end();)
    {
        if( par->globaltime - (*(memberNode->myPos)).timestamp > TREMOVE)
        {
            #ifdef DEBUGLOG
                Address *removedAddress = constructAddress((*(memberNode->myPos)).id, (*(memberNode->myPos)).port);
                log->logNodeRemove(&memberNode->addr, removedAddress);
            #endif

            memberNode->myPos = memberNode->memberList.erase(memberNode->myPos);
        }
        else
        {
            (memberNode->myPos)++;
        }

    }

    // use gossip-style membership protocal to send self-membership info to other members in group randomly
    double possibility = 0.4;
    memberNumber = memberNode->memberList.size();
    for (int i = 0; i < memberNumber; i++)
    {
        // do not send self message info to self
        if(!isSameAddress(memberNode->memberList[i].id, memberNode->memberList[i].port))
        {
            if((rand()%100 * 1.0) /100 > possibility )
            {
                Address *gossipAddress = constructAddress(memberNode->memberList[i].id, memberNode->memberList[i].port);
                sendSelfMembershipMessage((*gossipAddress).addr, PING);

                //#ifdef DEBUGLOG
                //string source = memberNode->addr.getAddress();
                //string target = gossipAddress.getAddress();
                //cout << "Send Ping message from" <<  source << "to" << target << endl;
                //#endif
            }
        }

    }

    return;
}

bool MP1Node::isSameAddress(int id, short port)
{
    Address *address = constructAddress(id, port);
    return (memcmp(memberNode->addr.addr,address, 6) == 0 ? 1 : 0);   
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

Address* MP1Node::constructAddress(char * address)
{
    Address *pAddr =  new Address();
    memcpy(pAddr->addr, address, sizeof(pAddr->addr));
    return pAddr;
}

Address* MP1Node::constructAddress(int id, int port)
{
    Address *pAddr =  new Address();
    memcpy(&pAddr->addr[0], &id, sizeof(int));
    memcpy(&pAddr->addr[4], &port, sizeof(short));
    return pAddr;
}
