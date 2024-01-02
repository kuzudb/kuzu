#include "common/types/uuid.h"

namespace kuzu {
namespace common {

void UUID::byteToHex(char byteVal, char* buf, uint64_t& pos) {
    buf[pos++] = HEX_DIGITS[(byteVal >> 4) & 0xf];
    buf[pos++] = HEX_DIGITS[byteVal & 0xf];
}

unsigned char UUID::hex2Char(char ch) {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f') {
        return 10 + ch - 'a';
    }
    if (ch >= 'A' && ch <= 'F') {
        return 10 + ch - 'A';
    }
    return 0;
};

bool UUID::fromString(std::string str, int128_t& result) {
    if (str.empty()) {
        return false;
    }
    uint32_t numBrackets = 0;
    if (str.front() == '{') {
        numBrackets = 1;
    }
    // LCOV_EXCL_START
    if (numBrackets && str.back() != '}') {
        return false;
    }
    // LCOV_EXCL_STOP

    result.low = 0;
    result.high = 0;
    uint32_t count = 0;
    for (uint32_t i = numBrackets; i < str.size() - numBrackets; ++i) {
        if (str[i] == '-') {
            continue;
        }
        if (count >= 32 || !isHex(str[i])) {
            return false;
        }
        if (count >= 16) {
            result.low = (result.low << 4) | hex2Char(str[i]);
        } else {
            result.high = (result.high << 4) | hex2Char(str[i]);
        }
        count++;
    }
    // Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
    result.high ^= (int64_t(1) << 63);
    return count == 32;
}

void UUID::toString(int128_t input, char* buf) {
    // Flip back before convert to string
    int64_t high = input.high ^ (int64_t(1) << 63);
    uint64_t pos = 0;
    byteToHex(high >> 56 & 0xFF, buf, pos);
    byteToHex(high >> 48 & 0xFF, buf, pos);
    byteToHex(high >> 40 & 0xFF, buf, pos);
    byteToHex(high >> 32 & 0xFF, buf, pos);
    buf[pos++] = '-';
    byteToHex(high >> 24 & 0xFF, buf, pos);
    byteToHex(high >> 16 & 0xFF, buf, pos);
    buf[pos++] = '-';
    byteToHex(high >> 8 & 0xFF, buf, pos);
    byteToHex(high & 0xFF, buf, pos);
    buf[pos++] = '-';
    byteToHex(input.low >> 56 & 0xFF, buf, pos);
    byteToHex(input.low >> 48 & 0xFF, buf, pos);
    buf[pos++] = '-';
    byteToHex(input.low >> 40 & 0xFF, buf, pos);
    byteToHex(input.low >> 32 & 0xFF, buf, pos);
    byteToHex(input.low >> 24 & 0xFF, buf, pos);
    byteToHex(input.low >> 16 & 0xFF, buf, pos);
    byteToHex(input.low >> 8 & 0xFF, buf, pos);
    byteToHex(input.low & 0xFF, buf, pos);
}

} // namespace common
} // namespace kuzu
