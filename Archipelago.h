#pragma once

#include <random>
#include <string>
#include <vector>
#include <map>
#include <cstdint>
#include <json/json.h>
#include <json/reader.h>
#include <json/value.h>
#include <json/writer.h>
#include <set>

extern std::string ap_player_name;

extern "C"
{

struct AP_State;

AP_State* AP_New();
void AP_Free(AP_State*);

void AP_SetPingInterval(AP_State* state, int interval);

void AP_Init(AP_State*, const char*, const char*, const char*, const char*);
void AP_InitSolo(AP_State*, const char* filename, const char* seed);
bool AP_IsInit(AP_State*);
bool AP_IsConnected(AP_State*);
bool AP_ConnectionError(AP_State*);
bool AP_IsScouted(AP_State*);

void AP_Start(AP_State*);
void AP_Stop(AP_State*);

struct AP_NetworkVersion {
    int major;
    int minor;
    int build;
};

struct AP_NetworkItem {
    int64_t item;
    int64_t location;
    int player;
    int flags;
    std::string itemName;
    std::string locationName;
    std::string playerName;
};

enum AP_ItemType {
    ITEM_TYPE_FILLER,
    ITEM_TYPE_PROGRESSION,
    ITEM_TYPE_USEFUL,
    ITEM_TYPE_TRAP
};

struct AP_NetworkPlayer {
    int team;
    int slot;
    std::string name;
    std::string alias;
    std::string game;
};

struct AP_DictIterator {
    const Json::Value* dict;
    Json::Value::const_iterator it;
    uintptr_t current_key;
    uintptr_t current_value;
};

// Set current client version
void AP_SetClientVersion(AP_State*, AP_NetworkVersion*);

/* Configuration Functions */

void AP_EnableQueueItemRecvMsgs(AP_State*, bool);

void AP_SetDeathLinkSupported(AP_State*, bool);

/* Required Callback Functions */

//Parameter Function must reset local state
void AP_SetItemClearCallback(AP_State*, void (*f_itemclr)());

//Parameter Function must collect item id given with parameter
//Second parameter indicates player who sent the item
//Third parameter indicates whether or not to notify player
void AP_SetItemRecvCallback(AP_State*, void (*f_itemrecv)(int64_t,int,bool));

//Parameter Function must mark given location id as checked
void AP_SetLocationCheckedCallback(AP_State*, void (*f_locrecv)(int64_t));

/* Optional Callback Functions */

//Parameter Function will be called when Death Link is received. Alternative to Pending/Clear usage
void AP_SetDeathLinkRecvCallback(AP_State*, void (*f_deathrecv)());

// Parameter Function receives Slotdata of respective type
void AP_RegisterSlotDataIntCallback(AP_State*, std::string, void (*f_slotdata)(int));
void AP_RegisterSlotDataMapIntIntCallback(AP_State*, std::string, void (*f_slotdata)(std::map<int,int>));
void AP_RegisterSlotDataRawCallback(AP_State*, std::string, void (*f_slotdata)(std::string));

int64_t AP_GetSlotDataInt(AP_State*, const char* key);
const char* AP_GetSlotDataString(AP_State*, const char* key);

uintptr_t AP_GetSlotDataRaw(AP_State* state, const char* key);
uintptr_t AP_AccessSlotDataRawArray(AP_State* state, uintptr_t jsonValue, size_t index);
bool AP_AccessSlotDataRawDictHasMember(AP_State* state, uintptr_t jsonValue, const char* key);
uintptr_t AP_AccessSlotDataRawDict(AP_State* state, uintptr_t jsonValue, const char* key);
int64_t AP_AccessSlotDataRawInt(AP_State* state, uintptr_t jsonValue);
const char* AP_AccessSlotDataRawString(AP_State* state, uintptr_t jsonValue);

uintptr_t AP_IterSlotDataRawDict(AP_State* state, uintptr_t jsonValue);
bool AP_IterSlotDataRawDictNext(AP_State* state, uintptr_t it_ptr, uintptr_t* dict_key_out, uintptr_t* dict_value_out);

char* AP_GetDataStorageSync(AP_State* state, const char* key);
void AP_SetDataStorageSync(AP_State* state, const char* key, char* value);
void AP_SetDataStorageAsync(AP_State* state, const char* key, char* value);

bool AP_GetDataPkgReceived(AP_State*);

// Send LocationScouts packet
void AP_QueueLocationScout(AP_State*, int64_t location);
void AP_RemoveQueuedLocationScout(AP_State*, int64_t location);
void AP_QueueLocationScoutsAll(AP_State*);
void AP_SendQueuedLocationScouts(AP_State*, int create_as_hint);
void AP_SendLocationScoutsAll(AP_State*, int create_as_hint);
void AP_SendLocationScouts(AP_State*, std::set<int64_t> locations, int create_as_hint);
// Receive Function for LocationInfo
void AP_SetLocationInfoCallback(AP_State*, void (*f_locrecv)(std::vector<AP_NetworkItem>));
bool AP_LocationExists(AP_State*, int64_t location_idx);

/* Game Management Functions */

// Sends LocationCheck for given index
void AP_SendItem(AP_State*, int64_t location);
void AP_SendItems(AP_State*, std::set<int64_t> const& locations);

// Gives all Items/Locations in current game
bool AP_GetLocationIsChecked(AP_State*, int64_t location_idx);
const char* AP_GetItemNameFromID(AP_State* state, int64_t item_id);
size_t AP_GetReceivedItemsSize(AP_State*);
int64_t AP_GetReceivedItem(AP_State*, size_t item_idx);
int64_t AP_GetReceivedItemLocation(AP_State*, size_t item_idx);
int64_t AP_GetReceivedItemType(AP_State*, size_t item_idx);
int64_t AP_GetSendingPlayer(AP_State* state, size_t item_idx);
int64_t AP_GetItemAtLocation(AP_State*, int64_t location_id);
bool AP_GetLocationHasLocalItem(AP_State*, int64_t location_id);
AP_ItemType AP_GetLocationItemType(AP_State*, int64_t location_id);
const char* AP_GetLocationItemName(AP_State*, int64_t location_id);
const char* AP_GetLocationItemPlayer(AP_State*, int64_t location_id);
int64_t AP_GetLocationItemPlayerID(AP_State* state, int64_t location_id);
const char* AP_GetPlayerFromSlot(AP_State*, int64_t slot);
const char* AP_GetPlayerGameFromSlot(AP_State* state, int64_t slot);

std::string AP_GetItemName(AP_State*, std::string game, int64_t id);
std::string AP_GetLocationName(AP_State*, std::string game, int64_t id);

// Called when Story completed, sends StatusUpdate
void AP_StoryComplete(AP_State*);

/* Deathlink Functions */

bool AP_DeathLinkPending(AP_State*);
void AP_DeathLinkClear(AP_State*);
void AP_DeathLinkSend(AP_State*);

/* Message Management Types */

enum struct AP_MessageType {
    Plaintext, ItemSend, ItemRecv, Hint, Countdown
};

struct AP_Message {
    AP_MessageType type = AP_MessageType::Plaintext;
    std::string text;
};

struct AP_ItemSendMessage : AP_Message {
    std::string item;
    std::string recvPlayer;
};

struct AP_ItemRecvMessage : AP_Message {
    std::string item;
    std::string sendPlayer;
};

struct AP_HintMessage : AP_Message {
    std::string item;
    std::string sendPlayer;
    std::string recvPlayer;
    std::string location;
    bool checked;
};

struct AP_CountdownMessage : AP_Message {
    int timer;
};

/* Message Management Functions */

bool AP_IsMessagePending(AP_State*);
AP_Message* AP_GetEarliestMessage(AP_State*);
AP_Message* AP_GetLatestMessage(AP_State*);
void AP_ClearEarliestMessage(AP_State*);
void AP_ClearLatestMessage(AP_State*);

void AP_Say(AP_State*, std::string);

/* Connection Information Types */

enum struct AP_ConnectionStatus {
    Disconnected, Connected, Authenticated, ConnectionRefused, NotFound
};

#define AP_PERMISSION_DISABLED 0b000
#define AP_PERMISSION_ENABLED 0b001
#define AP_PERMISSION_GOAL 0b010
#define AP_PERMISSION_AUTO 0b110

struct AP_RoomInfo {
    AP_NetworkVersion version;
    std::vector<std::string> tags;
    bool password_required;
    std::map<std::string, int> permissions;
    int hint_cost;
    int location_check_points;
    //MISSING: games
    std::map<std::string, std::string> datapackage_checksums;
    std::string seed_name;
    double time;
};

/* Connection Information Functions */

int AP_GetRoomInfo(AP_State*, AP_RoomInfo*);
AP_ConnectionStatus AP_GetConnectionStatus(AP_State*);
uint64_t AP_GetUUID(AP_State*);
int AP_GetTeamID(AP_State*);
int AP_GetPlayerID(AP_State*);
const char* AP_GetPlayerName(AP_State*);

/* Serverside Data Types */

enum struct AP_RequestStatus {
    Pending = 0,
    Done = 1,
    Error = 2
};

enum struct AP_DataType {
    Raw = 0,
    Int = 1,
    Double = 2
};

struct AP_GetServerDataRequest {
    volatile AP_RequestStatus status;
    std::string key;
    char* value;
    AP_DataType type;
};

struct AP_DataStorageOperation {
    std::string operation;
    char* value;
};

struct AP_SetServerDataRequest {
    volatile AP_RequestStatus status;
    std::string key;
    std::vector<AP_DataStorageOperation> operations;
    char* default_value;
    AP_DataType type;
    bool want_reply;
};

struct AP_SetReply {
    std::string key;
    char* original_value;
    char* value;
};

/* Serverside Data Functions */

// Set and Receive Data
void AP_SetServerData(AP_State*, AP_SetServerDataRequest* request);

void AP_GetServerDataSync(AP_State*, const char* key);

// This returns a string prefix, consistent across game connections and unique to the player slot.
// Intended to be used for getting / setting private server data
// No guarantees are made regarding the content of the prefix!
std::string AP_GetPrivateServerDataPrefix(AP_State*);

// Parameter Function receives all SetReply's
// ! Pointers in AP_SetReply struct only valid within function !
// If values are required beyond that a copy is needed
void AP_RegisterSetReplyCallback(AP_State*, void (*f_setreply)(AP_SetReply));

// Receive all SetReplys with Keys in parameter list
void AP_SetNotifies(AP_State*, std::map<std::string,AP_DataType>);
// Single Key version of above for convenience
void AP_SetNotify(AP_State*, std::string, AP_DataType);

// If using APCpp from a language with garbage collection,
// call this and set to false
void AP_SetManageMemory(AP_State*, bool);

}
