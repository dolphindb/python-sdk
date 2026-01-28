// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Exceptions.h"
#include "Exports.h"
#include <cstdint>
#include <cstring>
#include <string>

namespace dolphindb {

uint32_t murmur32_16b(const unsigned char* key);
uint32_t murmur32(const char *key, size_t len);

inline unsigned char hexPairToChar(char a) {
	if (islower(a) != 0) {
		return a - 'a' + 10;
	}
	if (isupper(a) != 0) {
		return a - 'A' + 10;
	}
	return a - '0';
}

inline unsigned char hexPairToChar(char a, char b) {
	return (hexPairToChar(a) << 4) + hexPairToChar(b);
}

inline bool fromGuid(const char* str, unsigned char* data){
	if(str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-')
		return false;
#ifndef BIGENDIANNESS
	for(int i=0; i<4; ++i)
		data[15 - i] = hexPairToChar(str[2*i], str[(2*i)+1]);
	data[11] = hexPairToChar(str[9], str[10]);
	data[10] = hexPairToChar(str[11], str[12]);
	data[9] = hexPairToChar(str[14], str[15]);
	data[8] = hexPairToChar(str[16], str[17]);
	data[7] = hexPairToChar(str[19], str[20]);
	data[6] = hexPairToChar(str[21], str[22]);

	for(int i=10; i<16; ++i)
		data[15 - i] = hexPairToChar(str[(2*i)+4], str[(2*i)+5]);
#else
	for(int i=0; i<4; ++i)
		data[i] = hexPairToChar(str[2*i], str[2*i+1]);
	data[4] = hexPairToChar(str[9], str[10]);
	data[5] = hexPairToChar(str[11], str[12]);
	data[6] = hexPairToChar(str[14], str[15]);
	data[7] = hexPairToChar(str[16], str[17]);
	data[8] = hexPairToChar(str[19], str[20]);
	data[9] = hexPairToChar(str[21], str[22]);

	for(int i=10; i<16; ++i)
		data[i] = hexPairToChar(str[2*i+4], str[2*i+5]);
#endif
	return true;
}

class EXPORT_DECL Guid {
public:
    explicit Guid(bool newGuid = false);
    explicit Guid(const unsigned char guid[]) { memcpy(uuid_, guid, 16); }
    explicit Guid(const std::string &guid)
    {
        if (guid.size() != 36 || !fromGuid(guid.c_str(), uuid_))
            throw RuntimeException("Invalid UUID string");
    }
    Guid(unsigned long long high, unsigned long long low){
#ifndef BIGENDIANNESS
        memcpy((char*)uuid_, (char*)&low, 8);
        memcpy((char*)uuid_ + 8, (char*)&high, 8);
#else
        memcpy((char*)uuid_, (char*)&high, 8);
        memcpy((char*)uuid_ + 8, (char*)&low, 8);
#endif
    }

    bool operator==(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
        return (*(long long*)a) == (*(long long*)b) && (*(long long*)(a+8)) == (*(long long*)(b+8));
    }
    bool operator!=(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
        return (*(long long*)a) != (*(long long*)b) || (*(long long*)(a+8)) != (*(long long*)(b+8));
    }
    bool operator<(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) < (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) < (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) < (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) < (*(unsigned long long*)(b+8)));
#endif
    }
    bool operator>(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) > (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) > (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) > (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) > (*(unsigned long long*)(b+8)));
#endif
    }
    bool operator<=(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) < (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) <= (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) < (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) <= (*(unsigned long long*)(b+8)));
#endif
    }
    bool operator>=(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) > (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) >= (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) > (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) >= (*(unsigned long long*)(b+8)));
#endif
    }
    int compare(const Guid &other) const
    {
        if (*this < other) {
            return -1;
        }
        if (*this > other) {
            return 1;
        }
        return 0;
    }
    unsigned char operator[](int i) const { return uuid_[i];}
    bool isZero() const;
    bool isNull() const {
        const auto* a = (const unsigned char*)uuid_;
        return (*(long long*)a) == 0 && (*(long long*)(a+8)) == 0;
    }
    bool isValid() const {
        const auto* a = (const unsigned char*)uuid_;
        return (*(long long*)a) != 0 || (*(long long*)(a+8)) != 0;
    }
    std::string getString() const;
    const unsigned char* bytes() const { return uuid_;}
    static std::string getString(const unsigned char* guid);

private:
    unsigned char uuid_[16];
};

struct GuidHash {
	uint64_t operator()(const Guid& guid) const;
};


} // namespace dolphindb

namespace std {
template<>
struct hash<dolphindb::Guid> {
    size_t operator()(const dolphindb::Guid& val) const{
        return dolphindb::murmur32_16b(val.bytes());
    }
};

} // namespace std
