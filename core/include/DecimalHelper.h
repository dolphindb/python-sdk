#ifndef DECIMAL_HELPER_H_
#define DECIMAL_HELPER_H_


#include "ConstantImp.h"
#include "TypeDefine.h"
#include "Types.h"
#include "Util.h"


namespace _decimal_util {

using namespace dolphindb;
using pybind_dolphindb::int128;
using decimal_util::exp10_i128;
using decimal_util::parseString;


inline bool isDecimalType(DATA_TYPE type) {
    if (type >= ARRAY_TYPE_BASE) {
        type = static_cast<DATA_TYPE>(type - ARRAY_TYPE_BASE);
    }
    return (Util::getCategory(type) == DENARY);
}

inline int packDecimalTypeAndScale(DATA_TYPE type, int scale) {
    if (false == isDecimalType(type)) {
        return static_cast<int>(type);
    }
    /*
     *     31          16            0
     * +---+-----------+-------------+
     * | 1 |   scale   |  data type  |
     * +---+-----------+-------------+
     */
    return ((scale << 16) | 0x80000000) | (type & 0xffff);
}

inline std::pair<DATA_TYPE, int> unpackDecimalTypeAndScale(const int value) {
    DATA_TYPE type = static_cast<DATA_TYPE>(value);
    int scale = 0;
    if (value & 0x80000000) {
        /*
         *     31          16            0
         * +---+-----------+-------------+
         * | 1 |   scale   |  data type  |
         * +---+-----------+-------------+
         */
        scale = (value & (~0x80000000)) >> 16;
        type = static_cast<DATA_TYPE>(value & 0xffff);
        if (type >= ARRAY_TYPE_BASE) {
            if (Util::getCategory(static_cast<DATA_TYPE>(static_cast<int>(type) - ARRAY_TYPE_BASE)) != DENARY) {
                type = DT_VOID;
            }
        } else {
            if (Util::getCategory(type) != DENARY) {
                type = DT_VOID;
            }
        }
    }
    return {type, scale};
}


} // namespace _decimal_util


#endif  // DECIMAL_HELPER_H_