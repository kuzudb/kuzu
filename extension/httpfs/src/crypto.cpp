#include "crypto.h"

#include "common/exception/runtime.h"
#include "mbedtls/md.h"
#include "mbedtls/sha256.h"

namespace kuzu {
namespace httpfs {

void sha256(const char* in, size_t inLen, hash_bytes& out) {
    mbedtls_sha256_context shaContext;
    mbedtls_sha256_init(&shaContext);
    if (mbedtls_sha256_starts(&shaContext, false) ||
        mbedtls_sha256_update(&shaContext, reinterpret_cast<const unsigned char*>(in), inLen) ||
        mbedtls_sha256_finish(&shaContext, reinterpret_cast<unsigned char*>(out))) {
        throw common::RuntimeException{"SHA256 Error"};
    }
    mbedtls_sha256_free(&shaContext);
}

void hmac256(const std::string& message, const char* secret, size_t secretLen, hash_bytes& out) {
    mbedtls_md_context_t hmacCtx;
    const mbedtls_md_info_t* mdType = mbedtls_md_info_from_type(MBEDTLS_MD_SHA256);
    if (!mdType) {
        throw common::RuntimeException("failed to init hmac");
    }

    if (mbedtls_md_setup(&hmacCtx, mdType, 1) ||
        mbedtls_md_hmac_starts(
            &hmacCtx, reinterpret_cast<const unsigned char*>(secret), secretLen) ||
        mbedtls_md_hmac_update(
            &hmacCtx, reinterpret_cast<const unsigned char*>(message.c_str()), message.length()) ||
        mbedtls_md_hmac_finish(&hmacCtx, reinterpret_cast<unsigned char*>(out))) {
        throw common::RuntimeException("HMAC256 Error");
    }
    mbedtls_md_free(&hmacCtx);
}

void hmac256(std::string message, hash_bytes secret, hash_bytes& out) {
    hmac256(message, (char*)secret, sizeof(hash_bytes), out);
}

void hex256(hash_bytes& in, hash_str& out) {
    const char* hex = "0123456789abcdef";
    unsigned char* pin = in;
    unsigned char* pout = out;
    for (; pin < in + sizeof(in); pout += 2, pin++) {
        pout[0] = hex[(*pin >> 4) & 0xF];
        pout[1] = hex[*pin & 0xF];
    }
}

} // namespace httpfs
} // namespace kuzu
