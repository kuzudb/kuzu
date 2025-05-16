#pragma once

#include <string>
#include <string_view>
#if USE_STD_FORMAT
#include <format>
#else
#include "common/exception/internal.h"
#endif

namespace kuzu {
namespace common {

#if USE_STD_FORMAT

// Wrapper around `std::format`. Checks format string at compile time.
template<typename... Args>
inline std::string stringFormat(std::format_string<Args...> format, Args&&... args) {
    return std::format(format, std::forward<Args>(args)...);
}

// Wrapper around `std::vformat`. Used for dynamic format strings.
template<typename... Args>
inline std::string vStringFormat(std::string_view format, Args&&... args) {
    return std::vformat(format, std::make_format_args(args...));
}

#else

namespace string_format_detail {

#define MAP_STD_TO_STRING(typ)                                                                     \
    inline std::string map(typ v) {                                                                \
        return std::to_string(v);                                                                  \
    }

MAP_STD_TO_STRING(short)
MAP_STD_TO_STRING(unsigned short)
MAP_STD_TO_STRING(int)
MAP_STD_TO_STRING(unsigned int)
MAP_STD_TO_STRING(long)
MAP_STD_TO_STRING(unsigned long)
MAP_STD_TO_STRING(long long)
MAP_STD_TO_STRING(unsigned long long)
MAP_STD_TO_STRING(float)
MAP_STD_TO_STRING(double)
#undef MAP_STD_TO_STRING

#define MAP_SELF(typ)                                                                              \
    inline typ map(typ v) {                                                                        \
        return v;                                                                                  \
    }
MAP_SELF(const char*);
// Also covers std::string
MAP_SELF(std::string_view)

// Chars are mapped to themselves, but signed char and unsigned char (which are used for int8_t and
// uint8_t respectively), need to be cast to be properly output as integers. This is consistent with
// fmt's behavior.
MAP_SELF(char)
inline std::string map(signed char v) {
    return std::to_string(int(v));
}
inline std::string map(unsigned char v) {
    return std::to_string(unsigned(v));
}
#undef MAP_SELF

template<typename... Args>
inline void stringFormatHelper(std::string& ret, std::string_view format, Args&&... args) {
    size_t len = format.size();
    size_t openBracket = len;
    size_t closeBracket = len;

    for (size_t i = 0; i < len; ++i) {
        if (format[i] == '{') {
            openBracket = i;
            break;
        }
        if (format[i] == '}') {
            closeBracket = i;
            break;
        }
    }

    // No '{' or '}' found.
    if (openBracket == len && closeBracket == len) {
        ret += format;
        return;
    }

    // Process '{'.
    if (openBracket < closeBracket) {
        ret += format.substr(0, openBracket);
        if (format.substr(openBracket, 2) == "{{") {
            // Escaped {.
            ret += "{";
            return stringFormatHelper(ret, format.substr(openBracket + 2),
                std::forward<Args>(args)...);
        }
        if (format.substr(openBracket, 2) == "{}") {
            // Formatted {}.
            throw InternalException("Not enough values for '{}'");
        }
        throw InternalException("Unescaped '{'");
    }

    // Process '}'.
    ret += format.substr(0, closeBracket);
    if (format.substr(closeBracket, 2) == "}}") {
        // Escaped }.
        ret += "}";
        return stringFormatHelper(ret, format.substr(closeBracket + 2),
            std::forward<Args>(args)...);
    }
    throw InternalException("Unescaped '}'");
}

template<typename Arg, typename... Args>
inline void stringFormatHelper(std::string& ret, std::string_view format, Arg&& arg,
    Args&&... args) {
    size_t len = format.size();
    size_t openBracket = len;
    size_t closeBracket = len;

    for (size_t i = 0; i < len; ++i) {
        if (format[i] == '{') {
            openBracket = i;
            break;
        }
        if (format[i] == '}') {
            closeBracket = i;
            break;
        }
    }

    // No '{' or '}' found.
    if (openBracket == len && closeBracket == len) {
        throw InternalException("Too many values for stringFormat");
    }

    if (openBracket < closeBracket) {
        // Process '{'.
        ret += format.substr(0, openBracket);
        if (format.substr(openBracket, 2) == "{{") {
            // Escaped {.
            ret += "{";
            return stringFormatHelper(ret, format.substr(openBracket + 2), std::forward<Arg>(arg),
                std::forward<Args>(args)...);
        }
        if (format.substr(openBracket, 2) == "{}") {
            // Formatted {}.
            ret += map(arg);
            return stringFormatHelper(ret, format.substr(openBracket + 2),
                std::forward<Args>(args)...);
        }
        throw InternalException("Unescaped '{'");
    }

    // Process '}'.
    ret += format.substr(0, closeBracket);
    if (format.substr(closeBracket, 2) == "}}") {
        // Escaped }.
        ret += "}";
        return stringFormatHelper(ret, format.substr(closeBracket + 2), std::forward<Arg>(arg),
            std::forward<Args>(args)...);
    }
    throw InternalException("Unescaped '}'");
}

} // namespace string_format_detail

// Formats `args` according to `format`. Accepts {} for formatting the argument and {{}} for
// a literal {}. Formatting is done with std::ostream::operator<<.
template<typename... Args>
inline std::string stringFormat(std::string_view format, Args&&... args) {
    std::string ret;
    ret.reserve(32); // Optimistic pre-allocation.
    string_format_detail::stringFormatHelper(ret, format, std::forward<Args>(args)...);
    return ret;
}

// Needed only to mirror the `USE_STD_FORMAT` API.
template<typename... Args>
inline std::string vStringFormat(std::string_view format, Args&&... args) {
    return stringFormat(format, std::forward<Args>(args)...);
}

#endif

} // namespace common
} // namespace kuzu
