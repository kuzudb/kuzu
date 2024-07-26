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
           RE2::FullMatch(str, "\\d{4}\\\\d{1,2}\\\\d{1,2}");
}

LogicalType inferMinimalTypeFromString(const std::string& str) {
    constexpr char array_begin = common::CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR;
    constexpr char array_end = common::CopyConstants::DEFAULT_CSV_LIST_END_CHAR;
    auto cpy = StringUtils::ltrim(StringUtils::rtrim(str));
    StringUtils::toUpper(cpy);
    if (cpy.size() == 0) {
        return LogicalType::STRING();
    }
    // Boolean
    if (cpy == "TRUE" || cpy == "FALSE") {
        return LogicalType::BOOL();
    }
    // Unsigned number
    if (RE2::FullMatch(cpy, "(0|[1-9]\\d*)")) {
        if (cpy.size() >= NumericLimits<int128_t>::digits()) {
            return LogicalType::DOUBLE();
        }
        int128_t val;
        if (!trySimpleInt128Cast(cpy.c_str(), cpy.length(), val)) {
            return LogicalType::STRING();
        }
        if (val <= NumericLimits<uint8_t>::maximum()) {
            return LogicalType::UINT8();
        }
        if (val <= NumericLimits<uint16_t>::maximum()) {
            return LogicalType::UINT16();
        }
        if (val <= NumericLimits<uint32_t>::maximum()) {
            return LogicalType::UINT32();
        }
        if (val <= NumericLimits<uint64_t>::maximum()) {
            return LogicalType::UINT64();
        }
        return LogicalType::INT128();
    }
    // Signed number
    if (RE2::FullMatch(cpy, "-(0|[1-9]\\d*)")) {
        if (cpy.size() >= 1 + NumericLimits<int128_t>::digits()) {
            return LogicalType::DOUBLE();
        }
        int128_t val;
        if (!trySimpleInt128Cast(cpy.c_str(), cpy.length(), val)) {
            return LogicalType::STRING();
        }
        if (val >= NumericLimits<int8_t>::minimum()) {
            return LogicalType::INT8();
        }
        if (val >= NumericLimits<int16_t>::minimum()) {
            return LogicalType::INT16();
        }
        if (val >= NumericLimits<int32_t>::minimum()) {
            return LogicalType::INT32();
        }
        if (val >= NumericLimits<int64_t>::minimum()) {
            return LogicalType::INT64();
        }
        return LogicalType::INT128();
    }
    // Real value checking
    if (RE2::FullMatch(cpy, "-?(0|[1-9]\\d*)?\\.\\d*")) {
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
    // it might just be quicker to try cast to timestamp
    timestamp_t tmp;
    if (common::Timestamp::tryConvertTimestamp(cpy.c_str(), cpy.length(), tmp)) {
        return LogicalType::TIMESTAMP();
    }

    if (cpy.front() == array_begin && cpy.back() == array_end) {
        auto split = StringUtils::split(cpy.substr(1, cpy.size() - 2), ",", false);
        auto childType = LogicalType::STRING();
        for (auto& ele : split) {
            LogicalType combinedType;
            if (!LogicalTypeUtils::tryGetMaxLogicalType(childType, inferMinimalTypeFromString(ele),
                    combinedType)) {
                childType = LogicalType::STRING();
                break;
            }
            childType = std::move(combinedType);
        }
        return LogicalType::LIST(std::move(childType));
    }

    if (cpy.front() == '{' && cpy.back() == '}') {
        auto split = StringUtils::split(cpy.substr(1, cpy.size() - 2), ",", false);
        auto childKeyType = LogicalType::STRING();
        bool cannotResolveKey = false;
        auto childValueType = LogicalType::STRING();
        bool cannotResolveValue = false;
        for (auto& ele : split) {
            LogicalType combinedKey = LogicalType::STRING(), combinedValue = LogicalType::STRING();
            auto splitEle = StringUtils::split(ele, "=", false);
            if (splitEle.size() != 2) {
                // invalid map; give string
                return LogicalType::STRING();
            }
            if (!cannotResolveKey && !LogicalTypeUtils::tryGetMaxLogicalType(childKeyType,
                                         inferMinimalTypeFromString(splitEle[0]), combinedKey)) {
                cannotResolveKey = true;
            }
            if (!cannotResolveValue &&
                !LogicalTypeUtils::tryGetMaxLogicalType(childValueType,
                    inferMinimalTypeFromString(splitEle[1]), combinedValue)) {
                cannotResolveValue = true;
            }
            childKeyType = std::move(combinedKey);
            childValueType = std::move(combinedValue);
            if (cannotResolveKey && cannotResolveValue) {
                break;
            }
        }
        return LogicalType::MAP(std::move(childKeyType), std::move(childValueType));
    }
    return LogicalType::STRING();
}

} // namespace function
} // namespace kuzu
