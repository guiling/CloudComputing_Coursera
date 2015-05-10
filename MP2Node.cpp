/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */

	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	//Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	if(ring.size() != curMemList.size())
	{
		change = true;
	}
	else
	{
		for(int i = 0; i < curMemList.size(); i++)
		{
			if(!isSameNode(curMemList[i], ring[i]))
			{
				change = true;
				break;
			}		
		}	
	}
		
	if(change)
	{
		ring = curMemList;
	}
 
	if(this->hasMyReplicas.size() == 0 && ring.size() > 0)
	{
		int currentIndex = getCurrentNodePosInRing();
		int secondRepPos = (currentIndex + 1) % ring.size();
		int thirdRepPos = (currentIndex + 2) % ring.size();
		// cout<< "firstRepPos=" << currentIndex << "secondRepPos=" << secondRepPos << "   thirdRepPos=" << thirdRepPos <<endl;
		this->hasMyReplicas.push_back(ring[secondRepPos]);
		this->hasMyReplicas.push_back(ring[thirdRepPos]);
	}	

	if(this->haveReplicasOf.size() == 0 && ring.size() > 0)
	{
		int currentIndex = getCurrentNodePosInRing();
		int thirdPossedRepPos = (ring.size() + currentIndex - 2) % ring.size();
		int secondHaveRepPos = (ring.size() + currentIndex -1) % ring.size();
		// cout << "firstHaveRepPos=" << firstHaveRepPos << "   secondHaveRepPos=" << secondHaveRepPos <<endl;

		this->haveReplicasOf.push_back(ring[secondHaveRepPos]);
		this->haveReplicasOf.push_back(ring[thirdPossedRepPos]);
	}

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	 if(change == true && !ht->isEmpty())
	 {
	 	stabilizationProtocol();
	 }
}

int MP2Node::getCurrentNodePosInRing()
{
	int currentIndex = -1;
	for(int i = 0 ; i < ring.size(); i++)
	{
		if( (*ring[i].getAddress()) == this->memberNode->addr)
		{
			currentIndex = i;
			break;
		}
	}

	return currentIndex;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	
	 // Get all the replica Node
	vector<Node> replicaNodes = findNodes(key);
	int transID = g_transID++;

	for(int i = 0; i < replicaNodes.size(); i++)
	{
		ReplicaType replica = static_cast<ReplicaType>(i);
		Message* pMessage = new Message(transID, 
			this->memberNode->addr, CREATE, key, value,replica);

		this->emulNet->ENsend(&memberNode->addr, replicaNodes[i].getAddress(), pMessage->toString());
		delete(pMessage);
	}

	TransInfo transInfo;
	transInfo.type = CREATE;
	transInfo.key = key;
	transInfo.value = value;
	transInfo.replyTimes = 0;
	transInfo.startTime = par->globaltime;

	transIdInfo.emplace(transID, transInfo);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	vector<Node> replicaNodes = findNodes(key);
	int transID = g_transID++;

	for(int i = 0; i < replicaNodes.size(); i++)
	{
		Message* pMessage = new Message(transID, 
			this->memberNode->addr, READ, key);

		this->emulNet->ENsend(&memberNode->addr, replicaNodes[i].getAddress(), pMessage->toString());
		delete(pMessage);
	}

	TransInfo transInfo;
	transInfo.type = READ;
	transInfo.key = key;
	transInfo.replyTimes = 0;
	transInfo.startTime = par->globaltime;

	transIdInfo.emplace(transID, transInfo);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){

	vector<Node> replicaNodes = findNodes(key);
	int transID = g_transID++;

	for(int i = 0; i < replicaNodes.size(); i++)
	{
		ReplicaType replica = static_cast<ReplicaType>(i);
		Message* pMessage = new Message(transID, 
			this->memberNode->addr, UPDATE, key, value, replica);

		this->emulNet->ENsend(&memberNode->addr, replicaNodes[i].getAddress(), pMessage->toString());
		delete(pMessage);
	}

	TransInfo transInfo;
	transInfo.type = UPDATE;
	transInfo.key = key;
	transInfo.value = value;
	transInfo.replyTimes = 0;
	transInfo.startTime = par->globaltime;

	transIdInfo.emplace(transID, transInfo);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	vector<Node> replicaNodes = findNodes(key);
	int transID = g_transID++;

	for(int i = 0; i < replicaNodes.size(); i++)
	{
		Message* pMessage = new Message(transID, 
			this->memberNode->addr, DELETE, key);

		this->emulNet->ENsend(&memberNode->addr, replicaNodes[i].getAddress(), pMessage->toString());
		delete(pMessage);
	}

	TransInfo transInfo;
	transInfo.type = DELETE;
	transInfo.key = key;
	transInfo.replyTimes = 0;
	transInfo.startTime = par->globaltime;

	transIdInfo.emplace(transID, transInfo);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {

	// Insert key, value, replicaType into the hash table
	Entry * entry = new Entry(value, par->globaltime,replica);
	bool isSuccess = ht->create(key, entry->convertToString());
	delete(entry);
	return isSuccess;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	
	// Update key in local hash table and return true or false
	Entry * entry = new Entry(value, par->globaltime,replica);
	bool isSuccess = ht->update(key, entry->convertToString());
	delete(entry);
	return isSuccess;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

void MP2Node::doCreateReplyMessage(Message* receivedMessage)
{
	bool isCreateSucc = createKeyValue(receivedMessage->key,
	receivedMessage->value, receivedMessage->replica);
	//Create reply Message format : 
	//			Message(int _transID, Address _fromAddr, MessageType _type, bool _success)
	if(receivedMessage->transID != -1)
	{
		Message* replyMessage = new Message(receivedMessage->transID, 
			this->memberNode->addr, REPLY, isCreateSucc);

		this->emulNet->ENsend(&memberNode->addr, &receivedMessage->fromAddr, replyMessage->toString());

		delete(replyMessage);

		if(isCreateSucc)
		{
			log->logCreateSuccess(&this->memberNode->addr, false, receivedMessage->transID,
				receivedMessage->key, receivedMessage->value);

		}
		else
		{
			log->logCreateFail(&this->memberNode->addr, false, receivedMessage->transID,
				receivedMessage->key, receivedMessage->value);
		}
	}
}

void MP2Node::doDeleteReplyMessage(Message* receivedMessage)
{
	bool isDeleteSucc = deletekey(receivedMessage->key);

	if(receivedMessage->transID != -1)
	{
		Message* replyMessage = new Message(receivedMessage->transID, 
		this->memberNode->addr, REPLY, isDeleteSucc);

		this->emulNet->ENsend(&memberNode->addr, &receivedMessage->fromAddr, replyMessage->toString());

		delete(replyMessage);	
		if(isDeleteSucc)
		{
			log->logDeleteSuccess(&this->memberNode->addr, false, receivedMessage->transID,
			receivedMessage->key);

		}
		else
		{
			log->logDeleteFail(&this->memberNode->addr, false, receivedMessage->transID,
			receivedMessage->key);
		}	 
	}
}

void MP2Node::doUpdateReplyMessage(Message * receivedMessage)
{
	bool isUpdateSucc = updateKeyValue(receivedMessage->key,
	receivedMessage->value, receivedMessage->replica);
	if(receivedMessage->transID != -1)
	{
		Message* replyMessage = new Message(receivedMessage->transID, 
			this->memberNode->addr, REPLY, isUpdateSucc);

		this->emulNet->ENsend(&memberNode->addr, &receivedMessage->fromAddr, replyMessage->toString());

		delete(replyMessage);

		if(isUpdateSucc)
		{
			log->logUpdateSuccess(&this->memberNode->addr, false, receivedMessage->transID,
				receivedMessage->key, receivedMessage->value);

		}
		else
		{
			log->logUpdateFail(&this->memberNode->addr, false, receivedMessage->transID,
				receivedMessage->key, receivedMessage->value);
		}
	}
}

void MP2Node::doReadReplyMessage(Message * receivedMessage)
{
	string readValue = readKey(receivedMessage->key);
	if(readValue != "")
	{
		Entry * entry = new Entry(readValue);
		readValue = entry->value;
		delete entry;
	}	

	if(receivedMessage->transID != -1)
	{
		Message* replyMessage = new Message(receivedMessage->transID, 
			this->memberNode->addr, readValue);

		this->emulNet->ENsend(&memberNode->addr, &receivedMessage->fromAddr, replyMessage->toString());

		delete(replyMessage);	
		if(readValue != "")
		{
			log->logReadSuccess(&this->memberNode->addr, false, receivedMessage->transID,
				receivedMessage->key, readValue);

		}
		else
		{
			log->logReadFail(&this->memberNode->addr, false, receivedMessage->transID,
				receivedMessage->key);
		}	 
	}
}

void MP2Node::doReadReplyReplyMessage(Message * receivedMessage)
{
	if(receivedMessage->value != "")
	{
		map<int, TransInfo>::iterator search;
		search = transIdInfo.find(receivedMessage->transID);
		if ( search != transIdInfo.end() ) 
		{
			//TODD : how to solve the read conflict
			transIdInfo[receivedMessage->transID].value = receivedMessage->value;
			transIdInfo[receivedMessage->transID].replyTimes ++ ;
		}
	}	
}

void MP2Node::doReplyReplyMessage(Message* receivedMessage)
{
	if(receivedMessage->success)
	{
		map<int, TransInfo>::iterator search;
		search = transIdInfo.find(receivedMessage->transID);
		if ( search != transIdInfo.end() ) 
		{
			transIdInfo[receivedMessage->transID].replyTimes ++ ;
		}
	}		 		
}

void MP2Node::checkCoordinatorCreateMessage(int transID,TransInfo transInfo)
{
	if(par->globaltime - transInfo.startTime > TIME_OUT)
	{
		log->logCreateFail(&this->memberNode->addr,
			true,
		    transID, 
		    transInfo.key, transInfo.value);
		transIdInfo.erase(transID);
	}
	else
	{
		if(transInfo.replyTimes >= 2)
		{
			log->logCreateSuccess(&this->memberNode->addr,
				true, 
				transID,
 				transInfo.key, transInfo.value);
			transIdInfo.erase(transID);
		}
	}	
}

void MP2Node::checkCoordinatorDeleteMessage(int transID, TransInfo transInfo)
{
	if(par->globaltime - transInfo.startTime > TIME_OUT)
	{
		log->logDeleteFail(&this->memberNode->addr,
			true,
		    transID, 
		    transInfo.key);
		transIdInfo.erase(transID);
	}
	else
	{
		if(transInfo.replyTimes >= 2)
		{
			log->logDeleteSuccess(&this->memberNode->addr,
				true, 
				transID,
				transInfo.key);
			transIdInfo.erase(transID);
		}
	}
}

void MP2Node::checkCoordinatorUpdateMessage(int transID, TransInfo transInfo)
{
	if(par->globaltime - transInfo.startTime > TIME_OUT)
	{
		log->logUpdateFail(&this->memberNode->addr,
			true,
		    transID, 
		    transInfo.key,
		    transInfo.value);
		transIdInfo.erase(transID);
	}
	else
	{
		if(transInfo.replyTimes >= 2)
		{
			log->logUpdateSuccess(&this->memberNode->addr,
				true, 
				transID,
				transInfo.key,
				transInfo.value);

			transIdInfo.erase(transID);
		}
	}
}

void MP2Node::checkCoordinatorReadMessage(int transID, TransInfo transInfo)
{
	if(par->globaltime -  transInfo.startTime > TIME_OUT)
	{
		log->logReadFail(&this->memberNode->addr,
			true,
		    transID, 
		    transInfo.key);
		transIdInfo.erase(transID);
	}
	else
	{
		if(transInfo.replyTimes >= 2)
		{
			log->logReadSuccess(&this->memberNode->addr,
				true, 
				transID,
				transInfo.key,
				transInfo.value);
			transIdInfo.erase(transID);
		}
	}
}

void MP2Node::checkCoordinatoReplyStatus()
{
	// checkCoordinator reply status
	// TODO: support rollback. Now we seem all the operation from client as a transcation
	map<int, TransInfo>::iterator it;
	for(it = this->transIdInfo.begin(); it != this->transIdInfo.end(); it++)
	{
		TransInfo transInfo = it->second;
		switch(transInfo.type)
		{
			case CREATE:
			{	
				checkCoordinatorCreateMessage(it->first, transInfo);
				break;
			}
			case DELETE:
			{
				checkCoordinatorDeleteMessage(it->first,transInfo);
				break;
			}
			case UPDATE:
			{
				checkCoordinatorUpdateMessage(it->first,transInfo);	
				break;
			}
			case READ:
			{
				checkCoordinatorReadMessage(it->first,transInfo);		
				break;	
			}
		}
	}		
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;

		memberNode->mp2q.pop();

		string message(data, data + size);
		Message* receivedMessage = new Message(message);
		
		 switch(receivedMessage->type)
		 {
		 	case CREATE:
		 	{
		 		doCreateReplyMessage(receivedMessage);
		 		break;
		 	}
		 	case DELETE:
		 	{
		 		doDeleteReplyMessage(receivedMessage);
		 		break;
		 	}
		 	case UPDATE:
		 	{
				doUpdateReplyMessage(receivedMessage);
		 		break;
		 	}
		 	case READ:
		 	{
				doReadReplyMessage(receivedMessage);
		 		break;
		 	}
		 	case READREPLY:
		 	{
		 		doReadReplyReplyMessage(receivedMessage);
		 		break;	
		 	}
		 	case REPLY:
		 	{
		 		doReplyReplyMessage(receivedMessage);
			 	break;
		 	}
		 }

		 delete(receivedMessage); 
	}

	// checkCoordinator reply status
	checkCoordinatoReplyStatus();
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	

	// First :	update the second and third node who has my replicas

	int currentIndex = getCurrentNodePosInRing();
	int secondRepPos = (currentIndex + 1) % ring.size();
	int thirdRepPos = (currentIndex + 2) % ring.size();
	Node secondRepPosNode = this->hasMyReplicas[0];
	Node thirdRepPosNode = this->hasMyReplicas[1];
	map<string,string> primaryItemsInHashTable = getKeysOfThisNode(PRIMARY);

	if(! isSameNode(secondRepPosNode, ring[secondRepPos]))
	{		
		// the current second replica is not same with third replica, 
		// this happend when the current is a new joined node, 
		// we need send create message to this node
		if(! isSameNode(thirdRepPosNode, ring[secondRepPos]))
		{
			updateMyReplica(ring[secondRepPos].getAddress(), SECONDARY, CREATE, primaryItemsInHashTable);
		}
		else
		{
			updateMyReplica(ring[secondRepPos].getAddress(), SECONDARY,UPDATE, primaryItemsInHashTable);
		}
		
	}

	if(! isSameNode(thirdRepPosNode, ring[thirdRepPos]))
	{
		// the current third replica is not same with second replica,
		// this happened when an existing node deleted
		// we need send create message to this node 
		
		if(! isSameNode(secondRepPosNode, ring[thirdRepPos]))
		{
			updateMyReplica(ring[thirdRepPos].getAddress(), TERTIARY, CREATE, primaryItemsInHashTable);
		}
		else
		{
			updateMyReplica(ring[thirdRepPos].getAddress(), TERTIARY,UPDATE, primaryItemsInHashTable);
		}
	}

	// Second: update the bossed replicas  
	int thirdHaveRepPos = (ring.size() + currentIndex - 2) % ring.size();
	int secondHaveRepPos = (ring.size() + currentIndex -1) % ring.size();
	Node secondHaveRepNode = this->haveReplicasOf[0];
	Node thirdHaveRepNode = this->haveReplicasOf[1];

	bool isSecondHaveNodeLost = false;
	if(! isSameNode(secondRepPosNode, ring[secondHaveRepPos]))
	{
		if(! isSameNode(thirdHaveRepNode, ring[secondHaveRepPos]))
		{
			isSecondHaveNodeLost = true;
			map<string,string> secondaryInHashTable = getKeysOfThisNode(SECONDARY);
			updateBossedReplicaLocally(secondaryInHashTable, PRIMARY);
			updateMyReplica(ring[secondRepPos].getAddress(), SECONDARY, CREATE, secondaryInHashTable);
			
		}
	}

	if(isSecondHaveNodeLost)
	{
		if(!isSameNode(thirdHaveRepNode, ring[thirdHaveRepPos]))
		{
			if(!isSameNode(secondRepPosNode, ring[thirdHaveRepPos]))
			{
			 	map<string,string> tertiaryInHashTable = getKeysOfThisNode(TERTIARY);
			 	updateBossedReplicaLocally(tertiaryInHashTable, PRIMARY);
			 	updateMyReplica(ring[thirdRepPos].getAddress(), TERTIARY, CREATE, tertiaryInHashTable);
			}
		}
	}

	//At last update my replica and possed replica

	vector<Node> newMyReplica;
	newMyReplica.push_back(ring[secondRepPos]);
	newMyReplica.push_back(ring[thirdRepPos]);

	this->hasMyReplicas = newMyReplica;

	vector<Node> newPossedReplica;
	newPossedReplica.push_back(ring[secondHaveRepPos]);
	newPossedReplica.push_back(ring[thirdHaveRepPos]);

	this->haveReplicasOf = newPossedReplica;
}

void MP2Node::updateBossedReplicaLocally(map<string, string> items, ReplicaType replicaType)
{
	for(map<string, string>::iterator it = items.begin(); it != items.end(); it++)
	{
		Entry * entry = new Entry(it->second);
		entry->replica = replicaType;
		string newValue = entry->convertToString();
		ht->update(it->first, newValue);

		delete entry;
	}
}

void MP2Node::updateMyReplica(Address* toAddress, ReplicaType replicaType, MessageType messageType, map<string, string> items)
{	
	// cout << memberNode->addr.getAddress() << "  send message to  " <<toAddress->getAddress() << " at " << par->globaltime << endl;
	// transID::fromAddr::CREATE::key::value::ReplicaType
	
	for(map<string, string>::iterator it = items.begin(); it != items.end(); it++)
	{
		Message* pMessage = new Message(-1 , this->memberNode->addr, messageType, it->first, it->second,replicaType);
		// cout << pMessage->toString() << endl;

		this->emulNet->ENsend(&memberNode->addr, toAddress, pMessage->toString());
		delete(pMessage);
	}
}

bool MP2Node::isSameNode(Node one, Node another)
{
	return (*one.getAddress() == *another.getAddress());
}

map<string, string> MP2Node::getKeysOfThisNode(ReplicaType replica)
{
	map<string, string> primaryItems;

	map<string, string>::iterator it;
	for(it = this->ht->hashTable.begin(); it != this->ht->hashTable.end(); it++)
	{
		string value = it->second;
		Entry * entry = new Entry(value);
		if(entry->replica == replica)
		{
			primaryItems.emplace(it->first,it->second);
		}

		delete entry;
	}

	return primaryItems;
}