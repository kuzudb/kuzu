#pragma once

#include <string>

namespace kuzu {
namespace httpfs {

typedef unsigned char hashBytes[32];
typedef unsigned char hashStr[64];

void sha256(const char* in, size_t inLen, hashBytes& out);

void hmac256(const std::string& message, const char* secret, size_t secretLen, hashBytes& out);

void hmac256(std::string message, hashBytes secret, hashBytes& out);

void hex256(hashBytes& in, hashStr& out);

} // namespace httpfs
} // namespace kuzu
