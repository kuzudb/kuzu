#include "c_api/helpers.h"

#include <cstring>

char* convertToOwnedCString(const std::string& str) {
    size_t src_len = str.size();
    auto* c_str = (char*)malloc(sizeof(char) * (src_len + 1));
    memcpy(c_str, str.c_str(), src_len);
    c_str[src_len] = '\0';
    return c_str;
}
