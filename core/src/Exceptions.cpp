/*
 * Exceptions.cpp
 *
 *  Created on: Feb 11, 2016
 *      Author: dzhou
 */
#include "Exceptions.h"

#include "Util.h"

#include <cstddef>
#include <unordered_map>


namespace {

using namespace dolphindb;

const static std::unordered_map<TagType, std::string> tagTypeToStringMap = {
    {TagType::TT_NotLeader, "<NotLeader>"},
    {TagType::TT_ChunkInTransaction, "<ChunkInTransaction>"},
    {TagType::TT_DataNodeNotAvail, "<DataNodeNotAvail>"},
    {TagType::TT_DataNodeNotReady, "<DataNodeNotReady>"},
    {TagType::TT_ControllerNotReady, "<ControllerNotReady>"},
    {TagType::TT_ControllerNotAvail, "<ControllerNotAvail>"},
    {TagType::TT_DFSIsNotEnabled, "DFS is not enabled"},
};

} // namespace

namespace dolphindb {

TagType parseEnumTagType(const string &msg) {
    if (msg == "<NotLeader>") {
        return TagType::TT_NotLeader;
    } else if (msg == "<ChunkInTransaction>") {
        return TagType::TT_ChunkInTransaction;
    } else if (msg == "<DataNodeNotAvail>") {
        return TagType::TT_DataNodeNotAvail;
    } else if (msg == "<DataNodeNotReady>") {
        return TagType::TT_DataNodeNotReady;
    } else if (msg == "<ControllerNotReady>") {
        return TagType::TT_ControllerNotReady;
    } else if (msg == "<ControllerNotAvail>") {
        return TagType::TT_ControllerNotAvail;
    } else if (msg == "DFS is not enabled") {
        return TagType::TT_DFSIsNotEnabled;
    } else {
        return TagType::TT_Invalid;
    }
}

std::string getTagTypeString(TagType tagType) {
    auto it = tagTypeToStringMap.find(tagType);
    if (it != tagTypeToStringMap.end())
        return it->second;
    return "<Invalid>";
}

static inline bool parseSite(const std::string &s, std::string &ip, int &port, std::string &alias) {
    size_t last_colon = s.rfind(':');
    if (last_colon == std::string::npos) {
        return false;
    }
    size_t second_last_colon = s.rfind(':', last_colon - 1);
    if (second_last_colon == std::string::npos) {
        ip = s.substr(0, last_colon);
        port = std::stoi(s.substr(last_colon + 1));
        alias = "";
    } else {
        ip = s.substr(0, second_last_colon);
        try {
            port = std::stoi(s.substr(second_last_colon + 1, last_colon - second_last_colon - 1));
        } catch (...) {
            return false;
        }
        alias = s.substr(last_colon + 1);
    }
    return true;
}

void ServerResponse::throwServerResponse(const string &rawMsg, const string &script) {
    if (rawMsg.find(tagTypeToStringMap.at(TagType::TT_NotLeader)) == 0) {
        std::string ip;
        int port;
        std::string alias;
        if (parseSite(rawMsg.substr(tagTypeToStringMap.at(TagType::TT_NotLeader).length()), ip, port, alias)) {
            throw TagResponseNotLeader(script, ip, port, alias);
        }
        throw ServerResponse(rawMsg, script);
    }
    for (const auto &pair : tagTypeToStringMap) {
        if (pair.first == TagType::TT_NotLeader) {
            continue;
        }
        if (rawMsg.find(pair.second) == 0) {
            throw TagResponse(pair.first, rawMsg, script);
        }
    }
    throw ServerResponse(rawMsg, script);
}

IncompatibleTypeException::IncompatibleTypeException(DATA_TYPE expected, DATA_TYPE actual)
    : expected_(expected), actual_(actual) {
    errMsg_.append("Incompatible type. Expected: " + Util::getDataTypeString(expected_)
                   + ", Actual: " + Util::getDataTypeString(actual_));
}

string IOException::getCodeDescription(IO_ERR errCode) const {
    switch (errCode) {
        case OK:
            return "";
        case DISCONNECTED:
            return "Socket is disconnected/closed or file is closed.";
        case NODATA:
            return "In non-blocking socket mode, there is no data ready for retrieval yet.";
        case NOSPACE:
            return "Out of memory, no disk space, or no buffer for sending data in non-blocking socket mode.";
        case TOO_LARGE_DATA:
            return "String size exceeds 64K or code size exceeds 1 MB during serialization over network.";
        case INPROGRESS:
            return "In non-blocking socket mode, a program is in pending connection mode.";
        case INVALIDDATA:
            return "Invalid message format";
        case END_OF_STREAM:
            return "Reach the end of a file or a buffer.";
        case READONLY:
            return "File is readable but not writable.";
        case WRITEONLY:
            return "File is writable but not readable.";
        case NOTEXIST:
            return "A file doesn't exist or the socket destination is not reachable.";
        case OTHERERR:
            return "Unknown IO error.";
        default:
            return "";
    }
}

} // namespace dolphindb
