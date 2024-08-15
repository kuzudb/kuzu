#include "function/cast/functions/cast_string_non_nested_functions.h"

#include "common/constants.h"
#include "common/types/timestamp_t.h"
#include "function/cast/functions/numeric_limits.h"
#include "re2.h"

namespace kuzu {
namespace function {

bool tryCastToBool(const char* input, uint64_t len, bool& result) {
    StringUtils::removeCStringWhiteSpaces(input, len);

    switch (len) {
    case 1: {
        char c = std::tolower(*input);
        if (c == 't' || c == '1') {
            result = true;
            return true;
        } else if (c == 'f' || c == '0') {
            result = false;
            return true;
        }
        return false;
    }
    case 4: {
        auto t = std::tolower(input[0]);
        auto r = std::tolower(input[1]);
        auto u = std::tolower(input[2]);
        auto e = std::tolower(input[3]);
        if (t == 't' && r == 'r' && u == 'u' && e == 'e') {
            result = true;
            return true;
        }
        return false;
    }
    case 5: {
        auto f = std::tolower(input[0]);
        auto a = std::tolower(input[1]);
        auto l = std::tolower(input[2]);
        auto s = std::tolower(input[3]);
        auto e = std::tolower(input[4]);
        if (f == 'f' && a == 'a' && l == 'l' && s == 's' && e == 'e') {
            result = false;
            return true;
        }
        return false;
    }
    default:
        return false;
    }
}

void castStringToBool(const char* input, uint64_t len, bool& result) {
    if (!tryCastToBool(input, len, result)) {
        throw ConversionException{
            stringFormat("Value {} is not a valid boolean", std::string{input, len})};
    }
}

template<>
bool TryCastStringToTimestamp::tryCast<timestamp_ns_t>(const char* input, uint64_t len,
    timestamp_t& result) {
    if (!Timestamp::tryConvertTimestamp(input, len, result)) {
        return false;
    }
    result = Timestamp::getEpochNanoSeconds(result);
    return true;
}

template<>
bool TryCastStringToTimestamp::tryCast<timestamp_ms_t>(const char* input, uint64_t len,
    timestamp_t& result) {
    if (!Timestamp::tryConvertTimestamp(input, len, result)) {
        return false;
    }
    result = Timestamp::getEpochMilliSeconds(result);
    return true;
}

template<>
bool TryCastStringToTimestamp::tryCast<timestamp_sec_t>(const char* input, uint64_t len,
    timestamp_t& result) {
    if (!Timestamp::tryConvertTimestamp(input, len, result)) {
        return false;
    }
    result = Timestamp::getEpochSeconds(result);
    return true;
}

static bool isDate(const std::string& str) {
    return RE2::FullMatch(str, "\\d{4}/\\d{1,2}/\\d{1,2}") ||
           RE2::FullMatch(str, "\\d{4}-\\d{1,2}-\\d{1,2}") ||
           RE2::FullMatch(str, "\\d{4} \\d{1,2} \\d{1,2}") ||
           RE2::FullMatch(str, "\\d{4}\\\\\\d{1,2}\\\\\\d{1,2}");
}

static bool isUUID(const std::string& str) {
    return RE2::FullMatch(str, "(?i)[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}");
}

static bool isInterval(const std::string& str) {
    static constexpr auto pattern =
        "(?i)((0|[1-9]\\d*) "
        "+(YEARS?|YRS?|Y|MONS?|MONTHS?|DAYS?|D|DAYOFMONTH|DECADES?|DECS?|CENTURY|CENTURIES|CENT|C|"
        "MILLENN?IUMS?|MILS?|MILLENNIA|MICROSECONDS?|US|USECS?|USECONDS?|SECONDS?|SECS?|S|MINUTES?|"
        "MINS?|M|HOURS?|HRS?|H|WEEKS?|WEEKOFYEAR|W|QUARTERS?))( +(0|[1-9]\\d*) "
        "+(YEARS?|YRS?|Y|MONS?|MONTHS?|DAYS?|D|DAYOFMONTH|DECADES?|DECS?|CENTURY|CENTURIES|CENT|C|"
        "MILLENN?IUMS?|MILS?|MILLENNIA|MICROSECONDS?|US|USECS?|USECONDS?|SECONDS?|SECS?|S|MINUTES?|"
        "MINS?|M|HOURS?|HRS?|H|WEEKS?|WEEKOFYEAR|W|QUARTERS?))*( +\\d+:\\d{2}:\\d{2}(\\.\\d+)?)?";
    static constexpr auto pattern2 = "\\d+:\\d{2}:\\d{2}(\\.\\d+)?";
    return RE2::FullMatch(str, pattern2) || RE2::FullMatch(str, pattern);
}

static LogicalType inferMapOrStruct(const std::string& str) {
    auto split = StringUtils::smartSplit(str.substr(1, str.size() - 2), ',');
    bool isMap = true, isStruct = true; // Default match to map if both are true
    for (auto& ele : split) {
        if (StringUtils::smartSplit(ele, '=', 2).size() != 2) {
            isMap = false;
        }
        if (StringUtils::smartSplit(ele, ':', 2).size() != 2) {
            isStruct = false;
        }
    }
    if (isMap) {
        auto childKeyType = LogicalType::ANY();
        auto childValueType = LogicalType::ANY();
        for (auto& ele : split) {
            auto split = StringUtils::smartSplit(ele, '=', 2);
            auto& key = split[0];
            auto& value = split[1];
            childKeyType =
                LogicalTypeUtils::combineTypes(childKeyType, inferMinimalTypeFromString(key));
            childValueType =
                LogicalTypeUtils::combineTypes(childValueType, inferMinimalTypeFromString(value));
        }
        return LogicalType::MAP(std::move(childKeyType), std::move(childValueType));
    } else if (isStruct) {
        std::vector<StructField> fields;
        for (auto& ele : split) {
            auto split = StringUtils::smartSplit(ele, ':', 2);
            auto fieldKey = StringUtils::ltrim(StringUtils::rtrim(split[0]));
            if (fieldKey.front() == '\'') {
                fieldKey.erase(fieldKey.begin());
            }
            if (fieldKey.back() == '\'') {
                fieldKey.pop_back();
            }
            auto fieldType = inferMinimalTypeFromString(split[1]);
            fields.emplace_back(fieldKey, std::move(fieldType));
        }
        return LogicalType::STRUCT(std::move(fields));
    } else {
        return LogicalType::STRING();
    }
}

LogicalType inferMinimalTypeFromString(const std::string& str) {
    constexpr char array_begin = common::CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR;
    constexpr char array_end = common::CopyConstants::DEFAULT_CSV_LIST_END_CHAR;
    auto cpy = StringUtils::ltrim(StringUtils::rtrim(str));
    if (cpy.size() == 0) {
        return LogicalType::ANY();
    }
    // Boolean
    if (RE2::FullMatch(cpy, "(?i)(TRUE|FALSE)")) {
        return LogicalType::BOOL();
    }
    // The reason we're not going to try to match to a minimal width integer
    // is because if we're infering the type of integer from a sequence of
    // increasing integers, we're bound to underestimate the width
    // if we only sniff the first few elements; a rather common occurrence.

    // integer
    if (RE2::FullMatch(cpy, "(-?0)|(-?[1-9]\\d*)")) {
        if (cpy.size() >= 1 + NumericLimits<int128_t>::digits()) {
            return LogicalType::DOUBLE();
        }
        int128_t val;
        if (!trySimpleInt128Cast(cpy.c_str(), cpy.length(), val)) {
            return LogicalType::STRING();
        }
        if (NumericLimits<int64_t>::isInBounds(val)) {
            return LogicalType::INT64();
        }
        return LogicalType::INT128();
    }
    // Real value checking
    if (RE2::FullMatch(cpy, "(\\+|-)?(0|[1-9]\\d*)?\\.(\\d*)")) {
        if (cpy[0] == '-') {
            cpy.erase(cpy.begin());
        }
        if (cpy.size() <= DECIMAL_PRECISION_LIMIT) {
            auto decimalPoint = cpy.find('.');
            KU_ASSERT(decimalPoint != std::string::npos);
            return LogicalType::DECIMAL(cpy.size() - 1, cpy.size() - decimalPoint - 1);
        } else {
            return LogicalType::DOUBLE();
        }
    }
    // date
    if (isDate(cpy)) {
        return LogicalType::DATE();
    }
    // It might just be quicker to try cast to timestamp.
    timestamp_t tmp;
    if (common::Timestamp::tryConvertTimestamp(cpy.c_str(), cpy.length(), tmp)) {
        return LogicalType::TIMESTAMP();
    }

    // UUID
    if (isUUID(cpy)) {
        return LogicalType::UUID();
    }

    // interval checking
    if (isInterval(cpy)) {
        return LogicalType::INTERVAL();
    }

    // array_begin and array_end are constants
    if (cpy.front() == array_begin && cpy.back() == array_end) {
        auto split = StringUtils::smartSplit(cpy.substr(1, cpy.size() - 2), ',');
        auto childType = LogicalType::ANY();
        for (auto& ele : split) {
            childType = LogicalTypeUtils::combineTypes(childType, inferMinimalTypeFromString(ele));
        }
        return LogicalType::LIST(std::move(childType));
    }

    if (cpy.front() == '{' && cpy.back() == '}') {
        return inferMapOrStruct(cpy);
    }

    return LogicalType::STRING();
}

} // namespace function
} // namespace kuzu
