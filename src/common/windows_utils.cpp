#if defined(_WIN32)
#include "common/windows_utils.h"

#include <memory>

#include "common/exception/io.h"

namespace kuzu {
namespace common {

std::wstring WindowsUtils::utf8ToUnicode(const char* input) {
    uint32_t result;

    result = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
    if (result == 0) {
        throw IOException("Failure in MultiByteToWideChar");
    }
    auto buffer = std::make_unique<wchar_t[]>(result);
    result = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result);
    if (result == 0) {
        throw IOException("Failure in MultiByteToWideChar");
    }
    return std::wstring(buffer.get(), result);
}

std::string WindowsUtils::unicodeToUTF8(LPCWSTR input) {
    uint64_t result_size;

    result_size = WideCharToMultiByte(CP_UTF8, 0, input, -1, 0, 0, 0, 0);
    if (result_size == 0) {
        throw IOException("Failure in WideCharToMultiByte");
    }
    auto buffer = std::make_unique<char>(result_size);
    result_size = WideCharToMultiByte(CP_UTF8, 0, input, -1, buffer.get(), result_size, 0, 0);
    if (result_size == 0) {
        throw IOException("Failure in WideCharToMultiByte");
    }
    return std::string(buffer.get(), result_size - 1);
}

} // namespace common
} // namespace kuzu
#endif
