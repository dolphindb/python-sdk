#ifndef CONVERTER_TYPEDEFINE_H_
#define CONVERTER_TYPEDEFINE_H_

#include "Constant.h"
#include "ConstantImp.h"
#include "Dictionary.h"
#include "SmartPointer.h"
#include "SymbolBase.h"
#include "Table.h"
#include "Vector.h"
#include "WideInteger.h"
#include "Util.h"
#include "Types.h"
#include "ScalarImp.h"
#include "Set.h"
#include "Logger.h"

#include <pybind11/pybind11.h>


#if defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#endif


namespace pybind_dolphindb {

using int128 = wide_integer::int128;

using dolphindb::INDEX;
using dolphindb::ARRAY_TYPE_BASE;

using dolphindb::FLT_NMIN;
using dolphindb::DBL_NMIN;

using dolphindb::Constant;
using dolphindb::ConstantSP;

using dolphindb::Util;
using dolphindb::DATA_TYPE;
using dolphindb::DATA_FORM;
using dolphindb::DATA_CATEGORY;

using dolphindb::Void;
using dolphindb::Bool;
using dolphindb::Char;
using dolphindb::Short;
using dolphindb::Int;
using dolphindb::Long;
using dolphindb::Float;
using dolphindb::Double;
using dolphindb::String;
using dolphindb::Date;
using dolphindb::Month;
using dolphindb::NanoTime;
using dolphindb::Time;
using dolphindb::Second;
using dolphindb::Minute;
using dolphindb::NanoTimestamp;
using dolphindb::Timestamp;
using dolphindb::DateTime;
using dolphindb::DateHour;
using dolphindb::Decimal32;
using dolphindb::Decimal64;
using dolphindb::Decimal128;
using dolphindb::Int128;
using dolphindb::Uuid;
using dolphindb::IPAddr;

using dolphindb::Vector;
using dolphindb::VectorSP;

using dolphindb::Set;
using dolphindb::SetSP;

using dolphindb::Dictionary;
using dolphindb::DictionarySP;

using dolphindb::Table;
using dolphindb::TableSP;

using dolphindb::DLogger;

using dolphindb::SmartPointer;

using dolphindb::FastSymbolVector;
using dolphindb::SymbolBaseSP;

using dolphindb::Guid;

}   // pybind_dolphindb


namespace converter {

using namespace pybind_dolphindb;

enum HELPER_TYPE {
    HT_VOID, HT_BOOL, HT_CHAR, HT_SHORT, HT_INT,
    HT_LONG, HT_DATE, HT_MONTH, HT_TIME, HT_MINUTE,
    HT_SECOND, HT_DATETIME, HT_TIMESTAMP, HT_NANOTIME, HT_NANOTIMESTAMP,
    HT_FLOAT, HT_DOUBLE, HT_SYMBOL, HT_STRING, HT_UUID,
    HT_FUNCTIONDEF, HT_HANDLE, HT_CODE, HT_DATASOURCE, HT_RESOURCE,
    HT_ANY, HT_COMPRESS, HT_DICTIONARY, HT_DATEHOUR, HT_DATEMINUTE,
    HT_IP, HT_INT128, HT_BLOB, HT_DECIMAL, HT_COMPLEX,
    HT_POINT, HT_DURATION, HT_DECIMAL32, HT_DECIMAL64, HT_DECIMAL128,
    HT_OBJECT, HT_EXTENSION, HT_UNK, HT_COUNT
};

enum HELPER_CATEGORY {
    HC_NOTHING,
    HC_LOGICAL,
    HC_INTEGRAL,
    HC_FLOATING,
    HC_TEMPORAL,
    HC_LITERAL,
    HC_SYSTEM,
    HC_MIXED,
    HC_BINARY,
    HC_COMPLEX,
    HC_ARRAY,
    HC_DENARY,
    HC_PYTYPE,
};

typedef std::pair<HELPER_TYPE, int> Type;

#define EXPARAM_DEFAULT      INT_MIN
#define EXPARM_VALUE(v)     (v == EXPARAM_DEFAULT ? 0 : v)

#define MAX_NULL_PRI        999

enum FUNCTION_TYPE { INNER_FUNC, OPTR_FUNC, OPTR2_FUNC, SYS_FUNC, UDF_FUNC, PARTIAL_FUNC };

}   // converter



#endif  // CONVERTER_TYPEDEFINE_H_