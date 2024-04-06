#include "function/cast/functions/cast_string_non_nested_functions.h"

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

} // namespace function
} // namespace kuzu
