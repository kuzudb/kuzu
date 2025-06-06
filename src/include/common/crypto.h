#pragma once

#include <string>
#include "common/api.h"

namespace kuzu {
namespace common {

typedef unsigned char hash_bytes[32];
typedef unsigned char hash_str[64];

KUZU_API void sha256(const char* in, size_t inLen, hash_bytes& out);

KUZU_API void hmac256(const std::string& message, const char* secret, size_t secretLen, hash_bytes& out);

KUZU_API void hmac256(std::string message, hash_bytes secret, hash_bytes& out);

KUZU_API void hex256(hash_bytes& in, hash_str& out);

} // namespace common
} // namespace kuzu
