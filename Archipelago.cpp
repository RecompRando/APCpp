#include "Archipelago.h"

#include "ixwebsocket/IXNetSystem.h"
#include "ixwebsocket/IXWebSocket.h"
#include "ixwebsocket/IXUserAgent.h"

#include "unzip.h"

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <json/json.h>
#include <json/reader.h>
#include <json/value.h>
#include <json/writer.h>
#include <deque>
#include <set>
#include <string>
#include <chrono>
#include <functional>
#include <utility>
#include <vector>
#include <filesystem>
#include <mutex>

constexpr int AP_OFFLINE_SLOT = 1;
#define AP_OFFLINE_NAME "You"

//Setup Stuff

// PRIV Func Declarations Start
void AP_Init_Generic(AP_State* state);
bool parse_response(AP_State* state, std::string msg, std::string &request);
void AP_GetServerData(AP_State* state, AP_GetServerDataRequest* request);
void APSend(AP_State* state, std::string req);
void WriteFileJSON(AP_State* state, Json::Value val, const std::filesystem::path& path);
std::string getItemName(AP_State* state, std::string game, int64_t id);
std::string getLocationName(AP_State* state, std::string game, int64_t id);
void parseDataPkg(AP_State* state, Json::Value new_datapkg);
void parseDataPkg(AP_State* state);
AP_NetworkPlayer* getPlayer(AP_State* state, int team, int slot);
// PRIV Func Declarations End

#define MAX_RETRIES 1

extern "C"
{

struct AP_State
{
    bool init = false;
    bool auth = false;
    bool refused = false;
    bool multiworld = true;
    bool isSSL = true;
    bool ssl_success = false;
    bool connected = false;
    bool skip_first_receive = false;
    bool scouted = false;
    bool notfound = false;
    bool datapkg_received = false;
    bool queue_all_locations;
    bool scout_queued_locations;
    bool scout_all_locations;
    bool manage_memory = true;
    int ping_interval = 5;
    int scout_queued_locations_hint;
    int scout_all_locations_hint;
    int ap_team_id;
    int ap_player_id;
    std::string ap_player_name;
    size_t ap_player_name_hash;
    std::string ap_ip;
    std::string ap_game;
    std::string ap_passwd;
    std::uint64_t ap_uuid = 0;
    std::mt19937 rando;
    AP_NetworkVersion client_version = {0,5,0};

    // Deathlink Stuff
    bool deathlinkstat = false;
    bool deathlinksupported = false;
    bool enable_deathlink = false;
    int deathlink_amnesty = 0;
    int cur_deathlink_amnesty = 0;

    // Message System
    std::deque<AP_Message*> messageQueue;
    bool queueitemrecvmsg = true;

    // Data Maps
    std::map<int64_t, AP_NetworkPlayer> map_players;
    std::map<std::pair<std::string,int64_t>, std::string> map_location_id_name;
    std::map<std::pair<std::string,int64_t>, std::string> map_item_id_name;
    std::map<int64_t, std::string> player_id_to_game_name;
    std::map<int64_t, std::string> item_to_name;
    std::map<int64_t, int64_t> location_to_item;
    std::map<int64_t, bool> location_has_local_item;
    std::map<int64_t, AP_ItemType> location_item_type;
    std::map<int64_t, std::string> location_item_name;
    std::map<int64_t, std::string> location_item_player;
    std::map<int64_t, int64_t> location_item_player_id;
    std::map<std::string, std::string> slot_data;
    std::map<std::string, Json::Value> slot_data_raw;

    // Sets
    std::set<int64_t> all_items;
    std::set<int64_t> all_locations;

    std::set<int64_t> queued_scout_locations;
    std::set<int64_t> removed_scout_locations;

    std::set<int64_t> checked_locations;
    std::set<int64_t> pending_locations;

    // Vectors
    std::vector<int64_t> received_items;
    std::vector<int64_t> received_item_locations;
    std::vector<int64_t> received_item_types;
    std::vector<int64_t> sending_player_ids;

    // Mutexes
    std::mutex cache_mutex;

    // Callback function pointers
    void (*resetItemValues)();
    void (*getitemfunc)(int64_t,int,bool);
    void (*checklocfunc)(int64_t);
    void (*locinfofunc)(std::vector<AP_NetworkItem>) = nullptr;
    void (*recvdeath)() = nullptr;
    void (*setreplyfunc)(AP_SetReply) = nullptr;

    // Serverdata Management
    std::map<std::string, AP_DataType> map_serverdata_typemanage;
    std::map<std::string, AP_RequestStatus> map_serverdata_set_status;
    AP_GetServerDataRequest resync_serverdata_request;
    size_t last_item_idx = 0;

    // Singleplayer Seed Info
    std::filesystem::path sp_save_path;
    Json::Value sp_save_root;

    // Misc Data for Clients
    AP_RoomInfo lib_room_info;

    // Server Data Stuff
    std::map<std::string, AP_GetServerDataRequest*> map_server_data;

    // Slot Data Stuff
    std::map<std::string, void (*)(int)> map_slotdata_callback_int;
    std::map<std::string, void (*)(std::string)> map_slotdata_callback_raw;
    std::map<std::string, void (*)(std::map<int,int>)> map_slotdata_callback_mapintint;
    std::vector<std::string> slotdata_strings;

    // Datapackage Stuff
    std::filesystem::path datapkg_cache_path = "APCpp_datapkg.cache";
    Json::Value datapkg_cache;
    std::set<std::string> datapkg_outdated_games;

    ix::WebSocket webSocket;
    Json::Reader reader;
    Json::FastWriter writer;

    Json::Value sp_ap_root;
};

AP_State* AP_New() {
    AP_State* state = new AP_State();
    return state;
}

void AP_Free(AP_State* state) {
    delete state;
}

void AP_SetPingInterval(AP_State* state, int interval) {
    state->ping_interval = interval;
    if (state->init) {
        state->webSocket.setPingInterval(interval);
    }
}

bool firstInit = false;

void AP_Init(AP_State* state, const char* ip, const char* game, const char* player_name, const char* passwd) {
    state->multiworld = true;
    state->notfound = false;
    state->refused = false;
    state->queue_all_locations = false;
    state->scout_queued_locations = false;
    state->scout_all_locations = false;
    
    uint64_t milliseconds_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    state->rando = std::mt19937((uint32_t) milliseconds_since_epoch);

    if (!strcmp(ip,"")) {
        ip = "archipelago.gg:38281";
        //printf("AP: Using default Server Adress: '%s'\n", ip);
    } else {
        //printf("AP: Using Server Adress: '%s'\n", ip);
    }
    state->ap_ip = ip;
    state->ap_game = game;
    state->ap_player_name = player_name;
    state->ap_passwd = passwd;

    //printf("AP: Initializing...\n");

    //Connect to server
    if (!firstInit) {
        ix::initNetSystem();
        firstInit = true;
    }
    state->webSocket.setUrl("wss://" + state->ap_ip);
    state->webSocket.setOnMessageCallback([=](const ix::WebSocketMessagePtr& msg)
        {
            if (msg->errorInfo.retries >= MAX_RETRIES) {
                if (/*msg->errorInfo.retries-1 >= 1 && */state->isSSL) {
                    //printf("AP: SSL connection failed. Attempting encrypted...\n");
                    state->webSocket.setUrl("ws://" + state->ap_ip);
                    state->isSSL = false;
                } else if (msg->errorInfo.retries >= 2*MAX_RETRIES) {
                    state->notfound = true;
                }
            }
            if (msg->type == ix::WebSocketMessageType::Message)
            {
                std::string request;
                if (parse_response(state, msg->str, request)) {
                    APSend(state, request);
                }
            }
            else if (msg->type == ix::WebSocketMessageType::Open)
            {
                //printf("AP: Connected to Archipelago\n");
            }
            else if (msg->type == ix::WebSocketMessageType::Error || msg->type == ix::WebSocketMessageType::Close)
            {
                state->datapkg_received = false;
                state->auth = false;
                state->skip_first_receive = true;
                for (std::pair<std::string, AP_GetServerDataRequest*> itr : state->map_server_data) {
                    itr.second->status = AP_RequestStatus::Error;
                    state->map_server_data.erase(itr.first);
                }
                //printf("AP: Error connecting to Archipelago. Retries: %d\n", msg->errorInfo.retries-1);
            }
        }
    );
    state->webSocket.enablePerMessageDeflate();
    state->webSocket.setPingInterval(state->ping_interval);

    AP_NetworkPlayer archipelago {
        -1,
        0,
        "Archipelago",
        "Archipelago",
        "__Server"

    };
    state->map_players[0] = archipelago;
    AP_Init_Generic(state);
}

struct zip_mem_data {
    std::vector<char> data;
    size_t cur_offset;
};

zip_mem_data load_zip_file(const std::filesystem::path& path) {
    zip_mem_data ret{};

    std::ifstream stream{path, std::ios::binary};
    stream.seekg(0, std::ios::end);
    ret.data.resize(stream.tellg());
    stream.seekg(0, std::ios::beg);

    stream.read(ret.data.data(), ret.data.size());

    return ret;
}

// Functions for reading an in-memory zip with minizip.
void fill_mem_filefunc(zlib_filefunc_def* pzlib_filefunc_def, zip_mem_data* data) {
    pzlib_filefunc_def->zopen_file = [](voidpf opaque, const char* unused, int mode) {
        (void) unused;
        (void) mode;

        zip_mem_data *mem = reinterpret_cast<zip_mem_data*>(opaque);
        mem->cur_offset = 0;

        return static_cast<void*>(mem);
    };
    pzlib_filefunc_def->zread_file = [](voidpf opaque, voidpf stream, void* buf, uLong size) {
        (void) opaque;
        zip_mem_data *mem = reinterpret_cast<zip_mem_data*>(stream);

        if (size > mem->data.size() - mem->cur_offset) {
            size = mem->data.size() - mem->cur_offset;
        }

        memcpy(buf, mem->data.data() + mem->cur_offset, size);
        mem->cur_offset += size;

        return size;
    };
    pzlib_filefunc_def->zwrite_file = [](voidpf, voidpf, const void*, uLong) {
        // Writing not supported.
        return uLong{0};
    };
    pzlib_filefunc_def->ztell_file = [](voidpf opaque, voidpf stream) {
        (void) opaque;
        zip_mem_data *mem = reinterpret_cast<zip_mem_data*>(stream);

        return static_cast<long>(mem->cur_offset);
    };
    pzlib_filefunc_def->zseek_file = [](voidpf opaque, voidpf stream, uLong offset, int origin) {
        (void) opaque;
        zip_mem_data *mem = reinterpret_cast<zip_mem_data*>(stream);
        uLong new_pos;
        switch (origin)
        {
            case ZLIB_FILEFUNC_SEEK_CUR:
                new_pos = mem->cur_offset + offset;
                break;
            case ZLIB_FILEFUNC_SEEK_END:
                new_pos = mem->data.size() + offset;
                break;
            case ZLIB_FILEFUNC_SEEK_SET:
                new_pos = offset;
                break;
            default: 
                return -1L;
        }

        if (new_pos > mem->data.size()) {
            return 1L; /* Failed to seek that far */
        }
        mem->cur_offset = new_pos;
        return 0L;
    };
    pzlib_filefunc_def->zclose_file = [](voidpf, voidpf) {
        // Nothing to do.
        return 0;
    };
    pzlib_filefunc_def->zerror_file = [](voidpf, voidpf) {
        // Nothing to do.
        return 0;
    };
    pzlib_filefunc_def->opaque = static_cast<void*>(data);
}

bool load_solo_zip(AP_State* state, const std::filesystem::path& zip_path, const char* seed) {
    // Read the zip into memory and pull the multidata json file from it. 
    zlib_filefunc_def zlib_funcs{};
    zip_mem_data zip_data = load_zip_file(zip_path);

    if (zip_data.data.size() == 0) {
        // Failed to open
        return false;
    }
    // printf("Found zip\n");

    // Open the zip in miniunzip.
    fill_mem_filefunc(&zlib_funcs, &zip_data);   
    unzFile gen_zip = unzOpen2(nullptr, &zlib_funcs);

    // Look for the multidata file in the zip.
    int result = unzLocateFile(gen_zip, ("AP_" + std::string{seed} + ".archipelago").c_str(), 0);
    if (result != UNZ_OK) {
        // Failed to find save bin
        return false;
    }
    // printf("Found multidata file\n");

    // Get the multidata file info.
    unz_file_info multidata_file_info;
    unzGetCurrentFileInfo(gen_zip, &multidata_file_info, nullptr, 0, nullptr, 0, nullptr, 0);

    // printf("Got multidata file info, size %lu\n", multidata_file_info.uncompressed_size);

    // Open the file.
    int open_result = unzOpenCurrentFile(gen_zip);

    // printf("Open multidata result: %d\n", open_result);
    if (open_result != UNZ_OK) {
        return false;
    }
    
    // Read the multidata file into a vector.
    std::vector<char> multidata{};
    multidata.resize(multidata_file_info.uncompressed_size);
    int bytes_read = unzReadCurrentFile(gen_zip, multidata.data(), multidata.size());

    // printf("Read multidata file, bytes read %d\n", bytes_read);
    if (bytes_read <= 0) {
        return false;
    }

    // Close the minizip handle.
    unzCloseCurrentFile(gen_zip);
    unzClose(gen_zip);

    // Parse the multidata file, skipping the first byte (since it's an identifier byte written during generation).
    bool parse_result = state->reader.parse(multidata.data() + 1, multidata.data() + multidata.size(), state->sp_ap_root);

    // printf("Parse result: %d\n", (int)parse_result);
    return parse_result;
}

void AP_InitSolo(AP_State* state, const char* filename, const char* seed) {
    state->multiworld = false;
    state->notfound = false;
    state->queue_all_locations = false;
    state->scout_queued_locations = false;
    state->scout_all_locations = false;

    std::filesystem::path mwfile_path{ std::u8string{ reinterpret_cast<const char8_t*>(filename) } };
    if (!load_solo_zip(state, mwfile_path, seed)) {
        state->auth = false;
        state->notfound = false;
    }

    state->sp_save_path = mwfile_path;
    state->sp_save_path.replace_extension("save.json");
    if (std::filesystem::exists(state->sp_save_path)) {
        std::ifstream savefile{ state->sp_save_path };
        state->reader.parse(savefile, state->sp_save_root);
        savefile.close();
    }
    else {
        state->sp_save_root = {};
    }

    WriteFileJSON(state, state->sp_save_root, state->sp_save_path);
    state->ap_player_name = AP_OFFLINE_NAME;
    AP_Init_Generic(state);
}

void AP_Start(AP_State* state) {
    state->init = true;
    if (state->multiworld) {
        state->webSocket.start();
    } else {
        if (!state->sp_save_root.get("init", false).asBool()) {
            state->sp_save_root["init"] = true;
            state->sp_save_root["checked_locations"] = Json::arrayValue;
            state->sp_save_root["store"] = Json::objectValue;
        }
        // Only game in the data package is our game
        state->ap_game = state->sp_ap_root["datapackage"].getMemberNames()[0];
        Json::Value fake_msg;
        fake_msg[0]["cmd"] = "Connected";
        fake_msg[0]["slot"] = AP_OFFLINE_SLOT;
        fake_msg[0]["slot_info"][std::to_string(AP_OFFLINE_SLOT)]["game"] = state->ap_game;
        fake_msg[0]["players"] = Json::arrayValue;
        fake_msg[0]["players"][0]["team"] = 0;
        fake_msg[0]["players"][0]["slot"] = AP_OFFLINE_SLOT;
        fake_msg[0]["players"][0]["alias"] = AP_OFFLINE_NAME;
        fake_msg[0]["players"][0]["name"] = AP_OFFLINE_NAME;
        fake_msg[0]["checked_locations"] = state->sp_save_root["checked_locations"];
        fake_msg[0]["slot_data"] = state->sp_ap_root["slot_data"][std::to_string(AP_OFFLINE_SLOT)];
        std::string req;
        parse_response(state, state->writer.write(fake_msg), req);
        fake_msg.clear();
        state->datapkg_outdated_games.insert(state->ap_game);
        fake_msg[0]["cmd"] = "DataPackage";
        fake_msg[0]["data"]["games"] = state->sp_ap_root["datapackage"];
        parse_response(state, state->writer.write(fake_msg), req);
        fake_msg.clear();
        fake_msg[0]["cmd"] = "LocationInfo";
        fake_msg[0]["locations"] = Json::arrayValue;
        for (auto location_id : state->sp_ap_root["locations"][std::to_string(AP_OFFLINE_SLOT)].getMemberNames()) {
            Json::Value location;
            location["item"] = state->sp_ap_root["locations"][std::to_string(AP_OFFLINE_SLOT)][location_id][0];
            location["location"] = (int64_t) strtoll(location_id.c_str(), NULL, 10);
            location["player"] = state->ap_player_id;
            location["flags"] = state->sp_ap_root["locations"][std::to_string(AP_OFFLINE_SLOT)][location_id][2];
            fake_msg[0]["locations"].append(location);
        }
        parse_response(state, state->writer.write(fake_msg), req);
        fake_msg.clear();
        fake_msg[0]["cmd"] = "ReceivedItems";
        fake_msg[0]["index"] = 0;
        fake_msg[0]["items"] = Json::arrayValue;
        for (unsigned int i = 0; i < state->sp_save_root["checked_locations"].size(); i++) {
            int64_t location_id = state->sp_save_root["checked_locations"][i].asInt64();
            Json::Value item;
            item["item"] = state->location_to_item[location_id];
            item["location"] = location_id;
            item["player"] = state->ap_player_id;
            fake_msg[0]["items"].append(item);
        }
        for (unsigned int i = 0; i < state->sp_ap_root["precollected_items"][std::to_string(AP_OFFLINE_SLOT)].size(); ++i) {
            Json::Value item;
            item["item"] = state->sp_ap_root["precollected_items"][std::to_string(AP_OFFLINE_SLOT)][i].asInt64();
            item["location"] = 0;
            item["player"] = 0;
            fake_msg[0]["items"].append(item);
        }
        parse_response(state, state->writer.write(fake_msg), req);
    }
}

void AP_Stop(AP_State* state) {
    state->webSocket.stop();
    state->init = false;
    state->notfound = false;
    state->refused = false;
    state->datapkg_received = false;
}

bool AP_IsInit(AP_State* state) {
    return state->init;
}

bool AP_IsConnected(AP_State* state) {
    return state->connected;
}

bool AP_ConnectionError(AP_State* state) {
    return state->notfound || state->refused;
}

bool AP_IsScouted(AP_State* state) {
    return state->scouted;
}

void AP_SetClientVersion(AP_State* state, AP_NetworkVersion* version) {
    state->client_version.major = version->major;
    state->client_version.minor = version->minor;
    state->client_version.build = version->build;
}

bool AP_LocationExists(AP_State* state, int64_t location_idx) {
    return state->all_locations.find(location_idx) != state->all_locations.end();
}

void AP_SendItem(AP_State* state, int64_t location_idx) {
    AP_SendItems(state, std::set<int64_t>{ location_idx });
}

void AP_SendItems(AP_State* state, std::set<int64_t> const& locations) {
    state->cache_mutex.lock();
    for (int64_t idx : locations) {
        //printf("AP: Checked '%s'.\n", getLocationName(ap_game, idx).c_str());
        state->pending_locations.insert(idx);
        if (state->location_has_local_item[idx]) {
            int64_t item = state->location_to_item[idx];
            int64_t location = idx;
            int64_t type = state->location_item_type[idx];
            int64_t sending_player = state->location_item_player_id[idx];
            state->received_items.push_back(item);
            state->received_item_locations.push_back(location);
            state->received_item_types.push_back(type);
            state->sending_player_ids.push_back(sending_player);
        }
    }
    state->cache_mutex.unlock();
    if (state->multiworld) {
        Json::Value req_t;
        req_t[0]["cmd"] = "LocationChecks";
        req_t[0]["locations"] = Json::arrayValue;
        for (int64_t loc : locations) {
            req_t[0]["locations"].append(loc);
        };
        APSend(state, state->writer.write(req_t));
    } else {
        std::set<int64_t> new_locations;
        for (int64_t idx : locations) {
            bool was_previously_checked = false;
            for (auto itr : state->sp_save_root["checked_locations"]) {
                if (itr.asInt64() == idx) {
                    was_previously_checked = true;
                    break;
                }
            }
            if (!was_previously_checked) new_locations.insert(idx);
        }

        Json::Value fake_msg;
        fake_msg[0]["cmd"] = "ReceivedItems";
        fake_msg[0]["index"] = state->last_item_idx+1;
        fake_msg[0]["items"] = Json::arrayValue;
        for (int64_t location_id : new_locations) {
            int64_t recv_item_id = state->location_to_item[location_id];
            if (recv_item_id == 0) continue;
            Json::Value item;
            item["item"] = recv_item_id;
            item["location"] = location_id;
            item["player"] = state->ap_player_id;
            fake_msg[0]["items"].append(item);
        }
        std::string req;
        parse_response(state, state->writer.write(fake_msg), req);

        fake_msg.clear();
        fake_msg[0]["cmd"] = "RoomUpdate";
        fake_msg[0]["checked_locations"] = Json::arrayValue;
        for (int64_t idx : new_locations) {
            fake_msg[0]["checked_locations"].append(idx);
            state->sp_save_root["checked_locations"].append(idx);
        }
        WriteFileJSON(state, state->sp_save_root, state->sp_save_path);
        parse_response(state, state->writer.write(fake_msg), req);
    }
}

bool AP_GetDataPkgReceived(AP_State* state) {
    return state->datapkg_received;
}

void AP_QueueLocationScout(AP_State* state, int64_t location) {
    state->queued_scout_locations.insert(location);
}

void AP_RemoveQueuedLocationScout(AP_State* state, int64_t location) {
    if (state->datapkg_received) {
        state->queued_scout_locations.erase(location);
        return;
    }
    state->removed_scout_locations.insert(location);
}

void AP_QueueLocationScoutsAll(AP_State* state) {
    if (state->datapkg_received) {
        for (auto location : state->all_locations) {
            state->queued_scout_locations.insert(location);
        }
        return;
    }
    state->queue_all_locations = true;
}

void AP_SendQueuedLocationScouts(AP_State* state, int create_as_hint) {
    if (state->datapkg_received) {
        AP_SendLocationScouts(state, state->queued_scout_locations, create_as_hint);
        state->queued_scout_locations.clear();
        return;
    }
    state->scout_queued_locations_hint = create_as_hint;
    state->scout_queued_locations = true;
}

void AP_SendLocationScoutsAll(AP_State* state, int create_as_hint) {
    if (state->datapkg_received) {
        AP_SendLocationScouts(state, state->all_locations, create_as_hint);
        return;
    }
    state->scout_all_locations_hint = create_as_hint;
    state->scout_all_locations = true;
}

void AP_SendLocationScouts(AP_State* state, std::set<int64_t> locations, int create_as_hint) {
    if (state->multiworld) {
        Json::Value req_t;
        req_t[0]["cmd"] = "LocationScouts";
        req_t[0]["locations"] = Json::arrayValue;
        for (auto location : locations) {
            req_t[0]["locations"].append(location);
        }
        req_t[0]["create_as_hint"] = create_as_hint;
        APSend(state, state->writer.write(req_t));
    } else {
        Json::Value fake_msg;
        fake_msg[0]["cmd"] = "LocationInfo";
        fake_msg[0]["locations"] = Json::arrayValue;
        for (auto location : locations) {
            Json::Value netitem;
            netitem["item"] = state->sp_ap_root["location_to_item"].get(std::to_string(location), 0).asInt64();
            netitem["location"] = location;
            netitem["player"] = state->ap_player_id;
            netitem["flags"] = 0b001; // Hardcoded for SP seeds.
            fake_msg[0]["locations"].append(netitem);
        }
    }
}

void AP_StoryComplete(AP_State* state) {
    if (!state->multiworld) return;
    Json::Value req_t;
    req_t[0]["cmd"] = "StatusUpdate";
    req_t[0]["status"] = 30; //CLIENT_GOAL
    APSend(state, state->writer.write(req_t));
}

void AP_DeathLinkSend(AP_State* state) {
    if (!state->enable_deathlink || !state->multiworld) return;
    if (state->cur_deathlink_amnesty > 0) {
        state->cur_deathlink_amnesty--;
        return;
    }
    state->cur_deathlink_amnesty = state->deathlink_amnesty;
    std::chrono::time_point<std::chrono::system_clock> timestamp = std::chrono::system_clock::now();
    Json::Value req_t;
    req_t[0]["cmd"] = "Bounce";
    req_t[0]["data"]["time"] = std::chrono::duration_cast<std::chrono::seconds>(timestamp.time_since_epoch()).count();
    req_t[0]["data"]["source"] = state->ap_player_name; // Name and Shame >:D
    req_t[0]["tags"][0] = "DeathLink";
    APSend(state, state->writer.write(req_t));
}

void AP_EnableQueueItemRecvMsgs(AP_State* state, bool b) {
    state->queueitemrecvmsg = b;
}

void AP_SetItemClearCallback(AP_State* state, void (*f_itemclr)()) {
    state->resetItemValues = f_itemclr;
}

void AP_SetItemRecvCallback(AP_State* state, void (*f_itemrecv)(int64_t,int,bool)) {
    state->getitemfunc = f_itemrecv;
}

void AP_SetLocationCheckedCallback(AP_State* state, void (*f_loccheckrecv)(int64_t)) {
    state->checklocfunc = f_loccheckrecv;
}

void AP_SetLocationInfoCallback(AP_State* state, void (*f_locinfrecv)(std::vector<AP_NetworkItem>)) {
    state->locinfofunc = f_locinfrecv;
}

void AP_SetDeathLinkRecvCallback(AP_State* state, void (*f_deathrecv)()) {
    state->recvdeath = f_deathrecv;
}

void AP_RegisterSlotDataIntCallback(AP_State* state, std::string key, void (*f_slotdata)(int)) {
    state->map_slotdata_callback_int[key] = f_slotdata;
    state->slotdata_strings.push_back(key);
}

void AP_RegisterSlotDataRawCallback(AP_State* state, std::string key, void (*f_slotdata)(std::string)) {
    state->map_slotdata_callback_raw[key] = f_slotdata;
    state->slotdata_strings.push_back(key);
}

void AP_RegisterSlotDataMapIntIntCallback(AP_State* state, std::string key, void (*f_slotdata)(std::map<int,int>)) {
    state->map_slotdata_callback_mapintint[key] = f_slotdata;
    state->slotdata_strings.push_back(key);
}

void AP_SetDeathLinkSupported(AP_State* state, bool supdeathlink) {
    state->deathlinksupported = supdeathlink;
}

bool AP_DeathLinkPending(AP_State* state) {
    return state->deathlinkstat;
}

void AP_DeathLinkClear(AP_State* state) {
    state->deathlinkstat = false;
}

bool AP_IsMessagePending(AP_State* state) {
    return !state->messageQueue.empty();
}

AP_Message* AP_GetEarliestMessage(AP_State* state) {
    return state->messageQueue.front();
}

AP_Message* AP_GetLatestMessage(AP_State* state) {
    return state->messageQueue.back();
}

void AP_ClearEarliestMessage(AP_State* state) {
    if (AP_IsMessagePending(state)) {
        delete state->messageQueue.front();
        state->messageQueue.pop_front();
    }
}

void AP_ClearLatestMessage(AP_State* state) {
    if (AP_IsMessagePending(state)) {
        delete state->messageQueue.back();
        state->messageQueue.pop_back();
    }
}

void AP_Say(AP_State* state, std::string text) {
    Json::Value req_t;
    req_t[0]["cmd"] = "Say";
    req_t[0]["text"] = text;
    APSend(state, state->writer.write(req_t));
}

int AP_GetRoomInfo(AP_State* state, AP_RoomInfo* client_roominfo) {
    if (!state->auth) return 1;
    *client_roominfo = state->lib_room_info;
    return 0;
}

AP_ConnectionStatus AP_GetConnectionStatus(AP_State* state) {
    if (!state->multiworld && state->auth) return AP_ConnectionStatus::Authenticated;
    if (state->refused) {
        return AP_ConnectionStatus::ConnectionRefused;
    }
    if (state->webSocket.getReadyState() == ix::ReadyState::Open) {
        if (state->auth) {
            return AP_ConnectionStatus::Authenticated;
        } else {
            return AP_ConnectionStatus::Connected;
        }
    }
    if (state->notfound) {
        return AP_ConnectionStatus::NotFound;
    }
    return AP_ConnectionStatus::Disconnected;
}

uint64_t AP_GetUUID(AP_State* state) {
    return state->ap_uuid;
}

int AP_GetTeamID(AP_State* state) {
    return state->ap_team_id;
}

int AP_GetPlayerID(AP_State* state) {
    return state->ap_player_id;
}

const char* AP_GetPlayerName(AP_State* state) {
    return state->ap_player_name.c_str();
}

void AP_SetServerData(AP_State* state, AP_SetServerDataRequest* request) {
    request->status = AP_RequestStatus::Pending;

    Json::Value req_t;
    req_t[0]["cmd"] = "Set";
    req_t[0]["key"] = request->key;
    switch (request->type) {
        case AP_DataType::Int:
            for (int i = 0; i < request->operations.size(); i++) {
                req_t[0]["operations"][i]["operation"] = request->operations[i].operation;
                req_t[0]["operations"][i]["value"] = *((int*)request->operations[i].value);
            }
            break;
        case AP_DataType::Double:
            for (int i = 0; i < request->operations.size(); i++) {
                req_t[0]["operations"][i]["operation"] = request->operations[i].operation;
                req_t[0]["operations"][i]["value"] = *((double*)request->operations[i].value);
            }
            break;
        default:
            for (int i = 0; i < request->operations.size(); i++) {
                req_t[0]["operations"][i]["operation"] = request->operations[i].operation;
                Json::Value data;
                state->reader.parse(*((std::string*)request->operations[i].value), data);
                req_t[0]["operations"][i]["value"] = data;
            }
            Json::Value default_val_json;
            state->reader.parse(*((std::string*)request->default_value), default_val_json);
            req_t[0]["default"] = default_val_json;
            break;
    }
    req_t[0]["want_reply"] = request->want_reply;
    state->map_serverdata_typemanage[request->key] = request->type;
    APSend(state, state->writer.write(req_t));
    request->status = AP_RequestStatus::Done;
}

void AP_RegisterSetReplyCallback(AP_State* state, void (*f_setreply)(AP_SetReply)) {
    state->setreplyfunc = f_setreply;
}

void AP_SetNotifies(AP_State* state, std::map<std::string, AP_DataType> keylist) {
    Json::Value req_t;
    req_t[0]["cmd"] = "SetNotify";
    int i = 0;
    for (std::pair<std::string,AP_DataType> keytypepair : keylist) {
        req_t[0]["keys"][i] = keytypepair.first;
        state->map_serverdata_typemanage[keytypepair.first] = keytypepair.second;
        i++;
    }
    APSend(state, state->writer.write(req_t));
}

void AP_SetNotify(AP_State* state, std::string key, AP_DataType type) {
    std::map<std::string, AP_DataType> keylist;
    keylist[key] = type;
    AP_SetNotifies(state, keylist);
}

void AP_SetManageMemory(AP_State* state, bool b) {
    state->manage_memory = b;
}

char* AP_GetDataStorageSync(AP_State* state, const char* key) {
    if (state->map_server_data.count(key) == 0) {
        state->map_server_data[key] = new AP_GetServerDataRequest();
    } else {
        if (state->manage_memory) {
            delete[] state->map_server_data[key]->value;
        }
    }

    AP_GetServerDataRequest* request = state->map_server_data[key];
    request->status = AP_RequestStatus::Pending;
    request->type = AP_DataType::Raw;
    request->key = key;

    Json::Value req_t;
    req_t[0]["cmd"] = "Get";
    req_t[0]["keys"][0] = key;
    APSend(state, state->writer.write(req_t));

    while (request->status == AP_RequestStatus::Pending);

    return (char*) request->value;
}

void AP_SetDataStorageSync(AP_State* state, const char* key, char* value) {
    AP_SetDataStorageAsync(state, key, value);
    while (state->map_serverdata_set_status[key] == AP_RequestStatus::Pending);
}

void AP_SetDataStorageAsync(AP_State* state, const char* key, char* value) {
    state->map_serverdata_set_status[key] = AP_RequestStatus::Pending;

    Json::Value req_t;
    req_t[0]["cmd"] = "Set";
    req_t[0]["key"] = key;
    req_t[0]["want_reply"] = "true";
    req_t[0]["operations"][0]["operation"] = "replace";
    req_t[0]["operations"][0]["value"] = value;
    APSend(state, state->writer.write(req_t));
}

std::string AP_GetPrivateServerDataPrefix(AP_State* state) {
    return "APCpp" + std::to_string(state->ap_player_name_hash) + "APCpp" + std::to_string(state->ap_player_id) + "APCpp";
}

bool AP_GetLocationIsChecked(AP_State* state, int64_t location_idx) {
    state->cache_mutex.lock();
    bool is_checked = state->checked_locations.find(location_idx) != state->checked_locations.end() ||
                      state->pending_locations.find(location_idx) != state->pending_locations.end();
    state->cache_mutex.unlock();
    return is_checked;
}

const char* AP_GetItemNameFromID(AP_State* state, int64_t item_id) {
    return state->item_to_name[item_id].c_str();
}

size_t AP_GetReceivedItemsSize(AP_State* state) {
    state->cache_mutex.lock();
    size_t size = state->received_items.size();
    state->cache_mutex.unlock();
    return size;
}

int64_t AP_GetReceivedItem(AP_State* state, size_t item_idx) {
    int64_t item;
    state->cache_mutex.lock();
    item = state->received_items[item_idx];
    state->cache_mutex.unlock();
    return item;
}

int64_t AP_GetReceivedItemLocation(AP_State* state, size_t item_idx) {
    int64_t location;
    state->cache_mutex.lock();
    location = state->received_item_locations[item_idx];
    state->cache_mutex.unlock();
    return location;
}

int64_t AP_GetReceivedItemType(AP_State* state, size_t item_idx) {
    int64_t type;
    state->cache_mutex.lock();
    type = state->received_item_types[item_idx];
    state->cache_mutex.unlock();
    return type;
}

int64_t AP_GetSendingPlayer(AP_State* state, size_t item_idx) {
    int64_t player;
    state->cache_mutex.lock();
    player = state->sending_player_ids[item_idx];
    state->cache_mutex.unlock();
    return player;
}

int64_t AP_GetSlotDataInt(AP_State* state, const char* key) {
    return stol(state->slot_data[key]);
}

const char* AP_GetSlotDataString(AP_State* state, const char* key) {
    return state->slot_data[key].c_str();
}

uintptr_t AP_GetSlotDataRaw(AP_State* state, const char* key) {
    return (uintptr_t) &state->slot_data_raw[key];
}

uintptr_t AP_AccessSlotDataRawArray(AP_State* state, uintptr_t jsonValue, size_t index) {
    const Json::Value* value = &(*((Json::Value*) jsonValue))[(Json::ArrayIndex) index];
    return (uintptr_t) value;
}

uintptr_t AP_AccessSlotDataRawDict(AP_State* state, uintptr_t jsonValue, const char* key) {
    const Json::Value* value = &(*((Json::Value*) jsonValue))[key];
    return (uintptr_t) value;
}

int64_t AP_AccessSlotDataRawInt(AP_State* state, uintptr_t jsonValue) {
    const Json::Value* value = (Json::Value*) jsonValue;
    return (*value).asInt64();
}

const char* AP_AccessSlotDataRawString(AP_State* state, uintptr_t jsonValue) {
    const Json::Value* value = (Json::Value*) jsonValue;
    return (*value).asString().c_str();
}

int64_t AP_GetItemAtLocation(AP_State* state, int64_t location_id) {
    return state->location_to_item[location_id];
}

bool AP_GetLocationHasLocalItem(AP_State* state, int64_t location_id) {
    return state->location_has_local_item[location_id];
}

AP_ItemType AP_GetLocationItemType(AP_State* state, int64_t location_id) {
    return state->location_item_type[location_id];
}

const char* AP_GetLocationItemName(AP_State* state, int64_t location_id) {
    return state->location_item_name[location_id].c_str();
}

const char* AP_GetLocationItemPlayer(AP_State* state, int64_t location_id) {
    return state->location_item_player[location_id].c_str();
}

int64_t AP_GetLocationItemPlayerID(AP_State* state, int64_t location_id) {
    return state->location_item_player_id[location_id];
}

const char* AP_GetPlayerFromSlot(AP_State* state, int64_t slot) {
    return state->map_players[slot].alias.c_str();
}

const char* AP_GetPlayerGameFromSlot(AP_State* state, int64_t slot) {
    return state->player_id_to_game_name[slot].c_str();
}

}

// PRIV

void AP_Init_Generic(AP_State* state) {
    state->ap_player_name_hash = std::hash<std::string>{}(state->ap_player_name);
    std::ifstream datapkg_cache_file(state->datapkg_cache_path);
    state->reader.parse(datapkg_cache_file, state->datapkg_cache);
    datapkg_cache_file.close();
}

void syncDataPkg(AP_State* state) {
    state->datapkg_received = true;
    if (state->queue_all_locations) {
        AP_QueueLocationScoutsAll(state);
        state->queue_all_locations = false;
    }
    if (state->removed_scout_locations.size() > 0) {
        for (auto location : state->removed_scout_locations) {
            state->queued_scout_locations.erase(location);
        }
        state->removed_scout_locations.clear();
    }
    if (state->scout_queued_locations) {
        AP_SendQueuedLocationScouts(state, state->scout_queued_locations_hint);
        state->scout_queued_locations = false;
    }
    if (state->scout_all_locations) {
        AP_SendLocationScouts(state, state->all_locations, state->scout_all_locations_hint);
        state->scout_all_locations = false;
    }
}

bool parse_response(AP_State* state, std::string msg, std::string &request) {
    Json::Value root;
    state->reader.parse(msg, root);
    for (unsigned int i = 0; i < root.size(); i++) {
        std::string cmd = root[i]["cmd"].asString();
        if (cmd == "RoomInfo") {
            state->lib_room_info.version.major = root[i]["version"]["major"].asInt();
            state->lib_room_info.version.minor = root[i]["version"]["minor"].asInt();
            state->lib_room_info.version.build = root[i]["version"]["build"].asInt();
            std::vector<std::string> serv_tags;
            for (auto itr : root[i]["tags"]) {
                serv_tags.push_back(itr.asString());
            }
            state->lib_room_info.tags = serv_tags;
            state->lib_room_info.password_required = root[i]["password"].asBool();
            std::map<std::string,int> serv_permissions;
            for (auto itr : root[i]["permissions"].getMemberNames()) {
                serv_permissions[itr] = root[i]["permissions"][itr].asInt();
            }
            state->lib_room_info.permissions = serv_permissions;
            state->lib_room_info.hint_cost = root[i]["hint_cost"].asInt();
            state->lib_room_info.location_check_points = root[i]["location_check_points"].asInt();
            std::map<std::string,std::string> serv_datapkg_checksums;
            for (auto itr : root[i]["datapackage_checksums"].getMemberNames()) {
                serv_datapkg_checksums[itr] = root[i]["datapackage_checksums"][itr].asString();
            }
            state->lib_room_info.datapackage_checksums = serv_datapkg_checksums;
            state->lib_room_info.seed_name = root[i]["seed_name"].asString();
            state->lib_room_info.time = root[i]["time"].asFloat();

            if (!state->auth) {
                Json::Value req_t;
                state->ap_uuid = state->rando();
                req_t[0]["cmd"] = "Connect";
                req_t[0]["game"] = state->ap_game;
                req_t[0]["name"] = state->ap_player_name;
                req_t[0]["password"] = state->ap_passwd;
                req_t[0]["uuid"] = state->ap_uuid;
                req_t[0]["tags"] = Json::arrayValue;
                req_t[0]["version"]["major"] = state->client_version.major;
                req_t[0]["version"]["minor"] = state->client_version.minor;
                req_t[0]["version"]["build"] = state->client_version.build;
                req_t[0]["version"]["class"] = "Version";
                req_t[0]["items_handling"] = 7; // Full Remote
                request = state->writer.write(req_t);
                return true;
            }
        } else if (cmd == "Connected") {
            // Avoid inconsistency if we disconnected before
            //printf("AP: Authenticated\n");
            state->ap_player_id = root[i]["slot"].asInt(); // MUST be called before resetitemvalues, otherwise PrivateServerDataPrefix, GetPlayerID return broken values!
            if (state->resetItemValues) {
                (*state->resetItemValues)();
            }

            state->auth = true;
            state->ssl_success = state->auth && state->isSSL;
            state->refused = false;

            for (unsigned int j = 0; j < root[i]["checked_locations"].size(); j++) {
                //Sync checks with server
                int64_t loc_id = root[i]["checked_locations"][j].asInt64();
                if (state->checklocfunc) {
                    (*state->checklocfunc)(loc_id);
                }
                state->checked_locations.insert(loc_id);
            }
            for (unsigned int j = 0; j < root[i]["players"].size(); j++) {
                AP_NetworkPlayer player = {
                    root[i]["players"][j]["team"].asInt(),
                    root[i]["players"][j]["slot"].asInt(),
                    root[i]["players"][j]["name"].asString(),
                    root[i]["players"][j]["alias"].asString(),
                    "PLACEHOLDER"
                };
                player.game = root[i]["slot_info"][std::to_string(player.slot)]["game"].asString();
                state->map_players[root[i]["players"][j]["slot"].asInt()] = player;
                state->ap_team_id = root[i]["team"].asInt();
                state->player_id_to_game_name[player.slot] = player.game;
            }
            if ((root[i]["slot_data"].get("death_link", false).asBool() || root[i]["slot_data"].get("DeathLink", false).asBool()) && state->deathlinksupported) state->enable_deathlink = true;
            if (root[i]["slot_data"]["death_link_amnesty"] != Json::nullValue)
                state->deathlink_amnesty = root[i]["slot_data"].get("death_link_amnesty", 0).asInt();
            else if (root[i]["slot_data"]["DeathLink_Amnesty"] != Json::nullValue)
                state->deathlink_amnesty = root[i]["slot_data"].get("DeathLink_Amnesty", 0).asInt();
            state->cur_deathlink_amnesty = state->deathlink_amnesty;
            for (const auto& key : root[i]["slot_data"].getMemberNames()) {
                state->slot_data_raw[key] = root[i]["slot_data"][key];
                if (root[i]["slot_data"][key].isConvertibleTo(Json::stringValue)) {
                    state->slot_data[key] = root[i]["slot_data"][key].asString();
                }
            }
            for (std::string key : state->slotdata_strings) {
                if (state->map_slotdata_callback_int.count(key)) {
                    (*state->map_slotdata_callback_int.at(key))(root[i]["slot_data"][key].asInt());
                    } else if (state->map_slotdata_callback_raw.count(key)) {
                    (*state->map_slotdata_callback_raw.at(key))(state->writer.write(root[i]["slot_data"][key]));
                } else if (state->map_slotdata_callback_mapintint.count(key)) {
                    std::map<int,int> out;
                    for (auto itr : root[i]["slot_data"][key].getMemberNames()) {
                        out[std::stoi(itr)] = root[i]["slot_data"][key][itr.c_str()].asInt();
                    }
                    (*state->map_slotdata_callback_mapintint.at(key))(out);
                }
                
            }

            //~ state->resync_serverdata_request.key = "APCppLastRecv" + state->ap_player_name + std::to_string(state->ap_player_id);
            //~ state->resync_serverdata_request.value = &state->last_item_idx;
            //~ state->resync_serverdata_request.type = AP_DataType::Int;
            //~ AP_GetServerData(state, &state->resync_serverdata_request);

            AP_RoomInfo info;
            AP_GetRoomInfo(state, &info);
            Json::Value req_t = Json::arrayValue;
            if (state->enable_deathlink && state->deathlinksupported) {
                Json::Value setdeathlink;
                setdeathlink["cmd"] = "ConnectUpdate";
                setdeathlink["tags"][0] = "DeathLink";
                req_t.append(setdeathlink);
            }
            // Get datapackage for outdated games
            if (state->multiworld) {
                for (std::pair<std::string,std::string> game_pkg : info.datapackage_checksums) {
                    if (state->datapkg_cache.get("games", Json::objectValue).get(game_pkg.first, Json::objectValue).get("checksum", "_None") != game_pkg.second) {
                        //printf("AP: Cache outdated for game: %s\n", game_pkg.first.c_str());
                        state->datapkg_outdated_games.insert(game_pkg.first);
                    }
                }
            }
            if (!state->datapkg_outdated_games.empty()) {
                Json::Value resync_datapkg;
                resync_datapkg["cmd"] = "GetDataPackage";
                resync_datapkg["games"] = Json::arrayValue;
                resync_datapkg["games"].append(*state->datapkg_outdated_games.begin());
                req_t.append(resync_datapkg);
            } else {
                parseDataPkg(state);
                Json::Value sync;
                sync["cmd"] = "Sync";
                req_t.append(sync);
                syncDataPkg(state);
            }
            request = state->writer.write(req_t);
            state->connected = true;
            return true;
        } else if (cmd == "DataPackage") {
            parseDataPkg(state, root[i]["data"]);
            Json::Value req_t;
            if (!state->datapkg_outdated_games.empty()) {
                req_t[0]["cmd"] = "GetDataPackage";
                req_t[0]["games"] = Json::arrayValue;
                req_t[0]["games"].append(*state->datapkg_outdated_games.begin());
            } else {
                req_t[0]["cmd"] = "Sync";
                syncDataPkg(state);
            }
            request = state->writer.write(req_t);
            return true;
        } else if (cmd == "Retrieved") {
            for (auto itr : root[i]["keys"].getMemberNames()) {
                if (!state->map_server_data.count(itr)) continue;
                AP_GetServerDataRequest* target = state->map_server_data[itr];
                std::string value_str = state->writer.write(root[i]["keys"][itr]);
                if (value_str[value_str.length() - 1] == '\n') {
                    value_str = value_str.substr(0, value_str.length() - 1);
                }
                if (value_str[value_str.length() - 1] == '\r') {
                    value_str = value_str.substr(0, value_str.length() - 1);
                }
                if (value_str[0] == '\"' && value_str[value_str.length() - 1] == '\"') {
                    value_str = value_str.substr(1, value_str.length() - 2);
                }
                char* value_c_str = new char[value_str.length() + 1];
                memcpy(value_c_str, value_str.c_str(), value_str.length() + 1);
                target->value = value_c_str;
                target->status = AP_RequestStatus::Done;
            }
        } else if (cmd == "SetReply") {
            //~ int int_val;
            //~ int int_orig_val;
            //~ double dbl_val;
            //~ double dbl_orig_val;
            //~ std::string raw_val;
            //~ std::string raw_orig_val;
            AP_SetReply setreply;
            setreply.key = root[i]["key"].asString();
            //~ switch (state->map_serverdata_typemanage[setreply.key]) {
                //~ case AP_DataType::Int:
                    //~ int_val = root[i]["value"].asInt();
                    //~ int_orig_val = root[i]["original_value"].asInt();
                    //~ setreply.value = &int_val;
                    //~ setreply.original_value = &int_orig_val;
                    //~ break;
                //~ case AP_DataType::Double:
                    //~ dbl_val = root[i]["value"].asDouble();
                    //~ dbl_orig_val = root[i]["original_value"].asDouble();
                    //~ setreply.value = &dbl_val;
                    //~ setreply.original_value = &dbl_orig_val;
                    //~ break;
                //~ default:
                    //~ raw_val = state->writer.write(root[i]["value"]);
                    //~ raw_orig_val = state->writer.write(root[i]["original_value"]);
                    //~ setreply.value = &raw_val;
                    //~ setreply.original_value = &raw_orig_val;
                    //~ break;
            //~ }
            //~ if (state->setreplyfunc) {
                //~ (*state->setreplyfunc)(setreply);
            //~ }
            state->map_serverdata_set_status[setreply.key] = AP_RequestStatus::Done;
        } else if (cmd == "PrintJSON") {
            const std::string printType = root[i].get("type","").asString();
            if (printType == "ItemSend" || printType == "ItemCheat") {
                if (getPlayer(state, state->ap_team_id, root[i]["receiving"].asInt())->alias == getPlayer(state, state->ap_team_id, state->ap_player_id)->alias || getPlayer(state, state->ap_team_id, root[i]["item"]["player"].asInt())->alias != getPlayer(state, state->ap_team_id, state->ap_player_id)->alias) continue;
                AP_NetworkPlayer* recv_player = getPlayer(state, state->ap_team_id, root[i]["receiving"].asInt());
                AP_ItemSendMessage* msg = new AP_ItemSendMessage;
                msg->type = AP_MessageType::ItemSend;
                msg->item = getItemName(state, recv_player->game, root[i]["item"]["item"].asInt64());
                msg->recvPlayer = recv_player->alias;
                msg->text = msg->item + std::string(" was sent to ") + msg->recvPlayer;
                state->messageQueue.push_back(msg);
            } else if (printType == "Hint") {
                AP_NetworkPlayer* send_player = getPlayer(state, state->ap_team_id, root[i]["item"]["player"].asInt());
                AP_NetworkPlayer* recv_player = getPlayer(state, state->ap_team_id, root[i]["receiving"].asInt());
                AP_HintMessage* msg = new AP_HintMessage;
                msg->type = AP_MessageType::Hint;
                msg->item = getItemName(state, recv_player->game, root[i]["item"]["item"].asInt64());
                msg->sendPlayer = send_player->alias;
                msg->recvPlayer = recv_player->alias;
                msg->location = getLocationName(state, send_player->game, root[i]["item"]["location"].asInt64());
                msg->checked = root[i]["found"].asBool();
                msg->text = std::string("Item ") + msg->item + std::string(" from ") + msg->sendPlayer + std::string(" to ") + msg->recvPlayer + std::string(" at ") + msg->location + std::string((msg->checked ? " (Checked)" : " (Unchecked)"));
                state->messageQueue.push_back(msg);
            } else if (printType == "Countdown") {
                AP_CountdownMessage* msg = new AP_CountdownMessage;
                msg->type = AP_MessageType::Countdown;
                msg->timer = root[i]["countdown"].asInt();
                msg->text = root[i]["data"][0]["text"].asString();
                state->messageQueue.push_back(msg);
            } else {
                AP_Message* msg = new AP_Message;
                msg->text = "";
                for (auto itr : root[i]["data"]) {
                    if (itr.get("type","").asString() == "player_id") {
                        msg->text += getPlayer(state, state->ap_team_id, itr["text"].asInt())->alias;
                    } else if (itr.get("text","") != "") {
                        msg->text += itr["text"].asString();
                    }
                }
                state->messageQueue.push_back(msg);
            }
        } else if (cmd == "LocationInfo") {
            std::vector<AP_NetworkItem> locations;
            for (unsigned int j = 0; j < root[i]["locations"].size(); j++) {
                AP_NetworkItem item;
                item.item = root[i]["locations"][j]["item"].asInt64();
                item.location = root[i]["locations"][j]["location"].asInt64();
                AP_NetworkPlayer* player = getPlayer(state, state->ap_team_id, root[i]["locations"][j]["player"].asInt());
                item.player = player->slot;
                item.flags = root[i]["locations"][j]["flags"].asInt();
                item.itemName = getItemName(state, player->game, item.item);
                item.locationName = getLocationName(state, state->ap_game, item.location);
                item.playerName = player->alias;
                locations.push_back(item);
                state->location_to_item[item.location] = item.item;
                AP_ItemType type;
                if (item.flags == 0) {
                    type = ITEM_TYPE_FILLER;
                } else if (item.flags & 0b001) {
                    type = ITEM_TYPE_PROGRESSION;
                } else if (item.flags & 0b010) {
                    type = ITEM_TYPE_USEFUL;
                } else if (item.flags & 0b100) {
                    type = ITEM_TYPE_TRAP;
                }
                state->location_item_type[item.location] = type;
                state->location_has_local_item[item.location] = item.player == state->ap_player_id;
                state->location_item_name[item.location] = item.itemName;
                state->location_item_player[item.location] = item.playerName;
                state->location_item_player_id[item.location] = item.player;
            }
            state->cache_mutex.lock();
            if (!state->scouted) {
                for (int64_t loc_id : state->checked_locations) {
                    if (state->location_has_local_item[loc_id]) {
                        int64_t item = state->location_to_item[loc_id];
                        int64_t location = loc_id;
                        int64_t type = state->location_item_type[loc_id];
                        int64_t sending_player = state->location_item_player_id[loc_id];
                        state->received_items.push_back(item);
                        state->received_item_locations.push_back(location);
                        state->received_item_types.push_back(type);
                        state->sending_player_ids.push_back(sending_player);
                    }
                }
            }
            state->cache_mutex.unlock();
            state->scouted = true;
            if (state->locinfofunc) {
                state->locinfofunc(locations);
            } else {
                //printf("AP: Received LocationInfo but no handler registered!\n");
            }
        } else if (cmd == "ReceivedItems") {
            int item_idx = root[i]["index"].asInt();
            bool notify;
            state->cache_mutex.lock();
            unsigned int initial_j = state->skip_first_receive ? state->received_items.size() : 0;
            state->cache_mutex.unlock();
            for (unsigned int j = initial_j; j < root[i]["items"].size(); j++) {
                int64_t item_id = root[i]["items"][j]["item"].asInt64();
                int64_t location_id = root[i]["items"][j]["location"].asInt64();
                int sending_player_id = root[i]["items"][j]["player"].asInt();
                int flags = root[i]["items"][j]["flags"].asInt();
                int type = ITEM_TYPE_FILLER;
                if (flags & 0b100 || flags & 0b001) {
                    type = ITEM_TYPE_PROGRESSION;
                } else if (flags & 0b010) {
                    type = ITEM_TYPE_USEFUL;
                }
                notify = (item_idx == 0 && state->last_item_idx <= j && state->multiworld) || item_idx != 0;
                if (state->getitemfunc) {
                    (*state->getitemfunc)(item_id, sending_player_id, notify);
                }
                if (sending_player_id != state->ap_player_id || location_id <= 0) {
                    state->cache_mutex.lock();
                    state->received_items.push_back(item_id);
                    state->received_item_locations.push_back(location_id);
                    state->received_item_types.push_back(type);
                    state->sending_player_ids.push_back(sending_player_id);
                    state->cache_mutex.unlock();
                }
                if (state->queueitemrecvmsg && notify) {
                    AP_ItemRecvMessage* msg = new AP_ItemRecvMessage;
                    AP_NetworkPlayer* sender = getPlayer(state, state->ap_team_id, sending_player_id);
                    msg->type = AP_MessageType::ItemRecv;
                    msg->item = getItemName(state, state->ap_game, item_id);
                    msg->sendPlayer = sender->alias;
                    msg->text = std::string("Received ") + msg->item + std::string(" from ") + msg->sendPlayer;
                    state->messageQueue.push_back(msg);
                }
            }
            state->skip_first_receive = false;
            state->last_item_idx = item_idx == 0 ? root[i]["items"].size() : state->last_item_idx + root[i]["items"].size();
            //~ AP_SetServerDataRequest request;
            //~ request.key = "APCppLastRecv" + state->ap_player_name + std::to_string(state->ap_player_id);
            //~ AP_DataStorageOperation replac;
            //~ replac.operation = "replace";
            //~ replac.value = &state->last_item_idx;
            //~ std::vector<AP_DataStorageOperation> operations;
            //~ operations.push_back(replac);
            //~ request.operations = operations;
            //~ request.default_value = 0;
            //~ request.type = AP_DataType::Int;
            //~ request.want_reply = false;
            //~ AP_SetServerData(state, &request);
        } else if (cmd == "RoomUpdate") {
            //Sync checks with server
            state->cache_mutex.lock();
            for (unsigned int j = 0; j < root[i]["checked_locations"].size(); j++) {
                int64_t loc_id = root[i]["checked_locations"][j].asInt64();
                if (state->checklocfunc) {
                    (*state->checklocfunc)(loc_id);
                }
                state->checked_locations.insert(loc_id);
                if (state->pending_locations.count(loc_id) != 0) {
                    state->pending_locations.erase(loc_id);
                }
            }
            state->cache_mutex.unlock();
            //Update Player aliases if present
            for (auto itr : root[i].get("players", Json::arrayValue)) {
                state->map_players[itr["slot"].asInt()].alias = itr["alias"].asString();
            }
        } else if (cmd == "ConnectionRefused") {
            state->auth = false;
            state->refused = true;
            //printf("AP: Archipelago Server has refused connection. Check Password / Name / IP and restart the Game.\n");
            fflush(stdout);
        } else if (cmd == "Bounced") {
            // Only expected Packages are DeathLink Packages. RIP
            if (!state->enable_deathlink) continue;
            for (unsigned int j = 0; j < root[i]["tags"].size(); j++) {
                if (root[i]["tags"][j].asString() == "DeathLink") {
                    // Suspicions confirmed ;-; But maybe we died, not them?
                    if (root[i]["data"]["source"].asString() == state->ap_player_name) break; // We already paid our penance
                    state->deathlinkstat = true;
                    std::string out = root[i]["data"]["source"].asString() + " killed you";
                    if (state->recvdeath != nullptr) {
                        (*state->recvdeath)();
                    }
                    break;
                }
            }
        }
    }
    return false;
}

void AP_GetServerData(AP_State* state, AP_GetServerDataRequest* request) {
    request->status = AP_RequestStatus::Pending;

    if (state->map_server_data.find(request->key) != state->map_server_data.end()) return;

    state->map_server_data[request->key] = request;

    Json::Value req_t;
    req_t[0]["cmd"] = "Get";
    req_t[0]["keys"][0] = request->key;
    APSend(state, state->writer.write(req_t));
}

void APSend(AP_State* state, std::string req) {
    if (state->webSocket.getReadyState() != ix::ReadyState::Open) {
        //printf("AP: Not Connected. Send will fail.\n");
        return;
    }
    //printf("%s\n", req.c_str());
    state->webSocket.send(req);
}

void WriteFileJSON(AP_State* state, Json::Value val, const std::filesystem::path& path) {
    std::ofstream out;
    out.open(path);
    out.seekp(0);
    out << state->writer.write(val).c_str();
    out.flush();
    out.close();
}

void parseDataPkg(AP_State* state, Json::Value new_datapkg) {
    for (std::string game : new_datapkg["games"].getMemberNames()) {
        Json::Value game_data = new_datapkg["games"][game];
        state->datapkg_cache["games"][game] = game_data;
        state->datapkg_outdated_games.erase(game);
        //printf("AP: Game Cache updated for %s\n", game.c_str());
    }
    WriteFileJSON(state, state->datapkg_cache, state->datapkg_cache_path);
    parseDataPkg(state);
}

void parseDataPkg(AP_State* state) {
    for (std::string game : state->datapkg_cache["games"].getMemberNames()) {
        Json::Value game_data = state->datapkg_cache["games"][game];
        for (std::string item_name : game_data["item_name_to_id"].getMemberNames()) {
            int64_t item_id = game_data["item_name_to_id"][item_name].asInt64();
            state->map_item_id_name[{game, item_id}] = item_name;
            state->item_to_name[item_id] = item_name;
            if (game == state->ap_game) {
                state->all_items.insert(item_id);
            }
        }
        for (std::string location : game_data["location_name_to_id"].getMemberNames()) {
            int64_t location_id = game_data["location_name_to_id"][location].asInt64();
            state->map_location_id_name[{game,location_id}] = location;
            if (game == state->ap_game) {
                state->all_locations.insert(location_id);
            }
        }
    }
}

std::string getItemName(AP_State* state, std::string game, int64_t id) {
    std::pair<std::string,int64_t> item = {game, id};
    return state->map_item_id_name.count(item) ? state->map_item_id_name.at(item) : std::string("Unknown Item") + std::to_string(id) + " from " + game;
}

std::string getLocationName(AP_State* state, std::string game, int64_t id) {
    std::pair<std::string,int64_t> location = {game, id};
    return state->map_location_id_name.count(location) ? state->map_location_id_name.at(location) : std::string("Unknown Location") + std::to_string(id) + " from " + game;
}

AP_NetworkPlayer* getPlayer(AP_State* state, int team, int slot) {
    return &state->map_players[slot];
}
