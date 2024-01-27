#pragma once

#include <string>

namespace kuzu {
namespace httpfs {

typedef unsigned char hash_bytes[32];
typedef unsigned char hash_str[64];

void sha256(const char* in, size_t inLen, hash_bytes& out);

void hmac256(const std::string& message, const char* secret, size_t secretLen, hash_bytes& out);

void hmac256(std::string message, hash_bytes secret, hash_bytes& out);

void hex256(hash_bytes& in, hash_str& out);

} // namespace httpfs
} // namespace kuzu
