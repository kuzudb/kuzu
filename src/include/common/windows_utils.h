#pragma once

#if defined(_WIN32)
#include <string>

#include "windows.h"

namespace kuzu {
namespace common {

struct WindowsUtils {
    static std::wstring utf8ToUnicode(const char* input);
    static std::string unicodeToUTF8(LPCWSTR input);
};

} // namespace common
} // namespace kuzu
#endif
