// Adapted from
// https://github.com/duckdb/duckdb/blob/main/src/storage/compression/bitpacking_hugeint.cpp

#include "storage/compression/bitpacking_int128.h"

#include "common/utils.h"
#include "storage/compression/compression.h"

namespace kuzu::storage {

//===--------------------------------------------------------------------===//
// Unpacking
//===--------------------------------------------------------------------===//

static void unpackSingle(const uint32_t* __restrict& in, common::int128_t* __restrict out,
    uint16_t delta, uint16_t shiftRight) {
    if (delta + shiftRight < 32) {
        *out =
            ((static_cast<common::int128_t>(in[0])) >> shiftRight) % (common::int128_t(1) << delta);
    }

    else if (delta + shiftRight >= 32 && delta + shiftRight < 64) {
        *out = static_cast<common::int128_t>(in[0]) >> shiftRight;
        ++in;

        if (delta + shiftRight > 32) {
            const uint16_t NEXT_SHR = shiftRight + delta - 32;
            *out |= static_cast<common::int128_t>((*in) % (1U << NEXT_SHR)) << (32 - shiftRight);
        }
    }

    else if (delta + shiftRight >= 64 && delta + shiftRight < 96) {
        *out = static_cast<common::int128_t>(in[0]) >> shiftRight;
        *out |= static_cast<common::int128_t>(in[1]) << (32 - shiftRight);
        in += 2;

        if (delta + shiftRight > 64) {
            const uint16_t NEXT_SHR = delta + shiftRight - 64;
            *out |= static_cast<common::int128_t>((*in) % (1U << NEXT_SHR)) << (64 - shiftRight);
        }
    }

    else if (delta + shiftRight >= 96 && delta + shiftRight < 128) {
        *out = static_cast<common::int128_t>(in[0]) >> shiftRight;
        *out |= static_cast<common::int128_t>(in[1]) << (32 - shiftRight);
        *out |= static_cast<common::int128_t>(in[2]) << (64 - shiftRight);
        in += 3;

        if (delta + shiftRight > 96) {
            const uint16_t NEXT_SHR = delta + shiftRight - 96;
            *out |= static_cast<common::int128_t>((*in) % (1U << NEXT_SHR)) << (96 - shiftRight);
        }
    }

    else if (delta + shiftRight >= 128) {
        *out = static_cast<common::int128_t>(in[0]) >> shiftRight;
        *out |= static_cast<common::int128_t>(in[1]) << (32 - shiftRight);
        *out |= static_cast<common::int128_t>(in[2]) << (64 - shiftRight);
        *out |= static_cast<common::int128_t>(in[3]) << (96 - shiftRight);
        in += 4;

        if (delta + shiftRight > 128) {
            const uint16_t NEXT_SHR = delta + shiftRight - 128;
            *out |= static_cast<common::int128_t>((*in) % (1U << NEXT_SHR)) << (128 - shiftRight);
        }
    }
}

static void unpackLast(const uint32_t* __restrict& in, common::int128_t* __restrict out,
    uint16_t delta) {
    const uint8_t LAST_IDX = 31;
    const uint16_t SHIFT = (delta * 31) % 32;
    out[LAST_IDX] = in[0] >> SHIFT;
    if (delta > 32) {
        out[LAST_IDX] |= static_cast<common::int128_t>(in[1]) << (32 - SHIFT);
    }
    if (delta > 64) {
        out[LAST_IDX] |= static_cast<common::int128_t>(in[2]) << (64 - SHIFT);
    }
    if (delta > 96) {
        out[LAST_IDX] |= static_cast<common::int128_t>(in[3]) << (96 - SHIFT);
    }
}

// Unpacks for specific deltas
static void unpackDelta0(common::int128_t* __restrict out) {
    for (uint8_t i = 0; i < 32; ++i) {
        out[i] = 0;
    }
}

static void unpackDelta32(const uint32_t* __restrict in, common::int128_t* __restrict out) {
    for (uint8_t k = 0; k < 32; ++k) {
        out[k] = static_cast<common::int128_t>(in[k]);
    }
}

static void unpackDelta64(const uint32_t* __restrict in, common::int128_t* __restrict out) {
    for (uint8_t i = 0; i < 32; ++i) {
        const uint8_t OFFSET = i * 2;
        out[i] = in[OFFSET];
        out[i] |= static_cast<common::int128_t>(in[OFFSET + 1]) << 32;
    }
}

static void unpackDelta96(const uint32_t* __restrict in, common::int128_t* __restrict out) {
    for (uint8_t i = 0; i < 32; ++i) {
        const uint8_t OFFSET = i * 3;
        out[i] = in[OFFSET];
        out[i] |= static_cast<common::int128_t>(in[OFFSET + 1]) << 32;
        out[i] |= static_cast<common::int128_t>(in[OFFSET + 2]) << 64;
    }
}

static void unpackDelta128(const uint32_t* __restrict in, common::int128_t* __restrict out) {
    for (uint8_t i = 0; i < 32; ++i) {
        const uint8_t OFFSET = i * 4;
        out[i] = in[OFFSET];
        out[i] |= static_cast<common::int128_t>(in[OFFSET + 1]) << 32;
        out[i] |= static_cast<common::int128_t>(in[OFFSET + 2]) << 64;
        out[i] |= static_cast<common::int128_t>(in[OFFSET + 3]) << 96;
    }
}

//===--------------------------------------------------------------------===//
// Packing
//===--------------------------------------------------------------------===//

static void packSingle(const common::int128_t in, uint32_t* __restrict& out, uint16_t delta,
    uint16_t shiftLeft, common::int128_t mask) {
    if (delta + shiftLeft < 32) {

        if (shiftLeft == 0) {
            out[0] = static_cast<uint32_t>(in & mask);
        } else {
            out[0] |= static_cast<uint32_t>((in & mask) << shiftLeft);
        }

    } else if (delta + shiftLeft >= 32 && delta + shiftLeft < 64) {

        if (shiftLeft == 0) {
            out[0] = static_cast<uint32_t>(in & mask);
        } else {
            out[0] |= static_cast<uint32_t>((in & mask) << shiftLeft);
        }
        ++out;

        if (delta + shiftLeft > 32) {
            *out = static_cast<uint32_t>((in & mask) >> (32 - shiftLeft));
        }
    }

    else if (delta + shiftLeft >= 64 && delta + shiftLeft < 96) {

        if (shiftLeft == 0) {
            out[0] = static_cast<uint32_t>(in & mask);
        } else {
            out[0] |= static_cast<uint32_t>(in << shiftLeft);
        }

        out[1] = static_cast<uint32_t>((in & mask) >> (32 - shiftLeft));
        out += 2;

        if (delta + shiftLeft > 64) {
            *out = static_cast<uint32_t>((in & mask) >> (64 - shiftLeft));
        }
    }

    else if (delta + shiftLeft >= 96 && delta + shiftLeft < 128) {
        if (shiftLeft == 0) {
            out[0] = static_cast<uint32_t>(in & mask);
        } else {
            out[0] |= static_cast<uint32_t>(in << shiftLeft);
        }

        out[1] = static_cast<uint32_t>((in & mask) >> (32 - shiftLeft));
        out[2] = static_cast<uint32_t>((in & mask) >> (64 - shiftLeft));
        out += 3;

        if (delta + shiftLeft > 96) {
            *out = static_cast<uint32_t>((in & mask) >> (96 - shiftLeft));
        }
    }

    else if (delta + shiftLeft >= 128) {
        // shl == 0 won't ever happen here considering a delta of 128 calls PackDelta128
        out[0] |= static_cast<uint32_t>(in << shiftLeft);
        out[1] = static_cast<uint32_t>((in & mask) >> (32 - shiftLeft));
        out[2] = static_cast<uint32_t>((in & mask) >> (64 - shiftLeft));
        out[3] = static_cast<uint32_t>((in & mask) >> (96 - shiftLeft));
        out += 4;

        if (delta + shiftLeft > 128) {
            *out = static_cast<uint32_t>((in & mask) >> (128 - shiftLeft));
        }
    }
}

static void packLast(const common::int128_t* __restrict in, uint32_t* __restrict out,
    uint16_t delta) {
    const uint8_t LAST_IDX = 31;
    const uint16_t SHIFT = (delta * 31) % 32;
    out[0] |= static_cast<uint32_t>(in[LAST_IDX] << SHIFT);
    if (delta > 32) {
        out[1] = static_cast<uint32_t>(in[LAST_IDX] >> (32 - SHIFT));
    }
    if (delta > 64) {
        out[2] = static_cast<uint32_t>(in[LAST_IDX] >> (64 - SHIFT));
    }
    if (delta > 96) {
        out[3] = static_cast<uint32_t>(in[LAST_IDX] >> (96 - SHIFT));
    }

    // note that SHIFT + delta <= 128 for delta <= 128
    // thus we never need to check for overflow into out[4]
}

// Packs for specific deltas
static void packDelta32(const common::int128_t* __restrict in, uint32_t* __restrict out) {
    for (uint8_t i = 0; i < 32; ++i) {
        out[i] = static_cast<uint32_t>(in[i]);
    }
}

static void packDelta64(const common::int128_t* __restrict in, uint32_t* __restrict out) {
    for (uint8_t i = 0; i < 32; ++i) {
        const uint8_t OFFSET = 2 * i;
        out[OFFSET] = static_cast<uint32_t>(in[i]);
        out[OFFSET + 1] = static_cast<uint32_t>(in[i] >> 32);
    }
}

static void packDelta96(const common::int128_t* __restrict in, uint32_t* __restrict out) {
    for (uint8_t i = 0; i < 32; ++i) {
        const uint8_t OFFSET = 3 * i;
        out[OFFSET] = static_cast<uint32_t>(in[i]);
        out[OFFSET + 1] = static_cast<uint32_t>(in[i] >> 32);
        out[OFFSET + 2] = static_cast<uint32_t>(in[i] >> 64);
    }
}

static void packDelta128(const common::int128_t* __restrict in, uint32_t* __restrict out) {
    for (uint8_t i = 0; i < 32; ++i) {
        const uint8_t OFFSET = 4 * i;
        out[OFFSET] = static_cast<uint32_t>(in[i]);
        out[OFFSET + 1] = static_cast<uint32_t>(in[i] >> 32);
        out[OFFSET + 2] = static_cast<uint32_t>(in[i] >> 64);
        out[OFFSET + 3] = static_cast<uint32_t>(in[i] >> 96);
    }
}

//===--------------------------------------------------------------------===//
// HugeIntPacker
//===--------------------------------------------------------------------===//

void Int128Packer::pack(const common::int128_t* __restrict in, uint32_t* __restrict out,
    uint8_t width) {
    KU_ASSERT(width <= 128);
    switch (width) {
    case 0:
        break;
    case 32:
        packDelta32(in, out);
        break;
    case 64:
        packDelta64(in, out);
        break;
    case 96:
        packDelta96(in, out);
        break;
    case 128:
        packDelta128(in, out);
        break;
    default:
        for (common::idx_t oindex = 0; oindex < IntegerBitpacking<common::int128_t>::CHUNK_SIZE - 1;
             ++oindex) {
            packSingle(in[oindex], out, width,
                (width * oindex) % IntegerBitpacking<common::int128_t>::CHUNK_SIZE,
                common::BitmaskUtils::all1sMaskForLeastSignificantBits<common::int128_t>(width));
        }
        packLast(in, out, width);
    }
}

void Int128Packer::unpack(const uint32_t* __restrict in, common::int128_t* __restrict out,
    uint8_t width) {
    KU_ASSERT(width <= 128);
    switch (width) {
    case 0:
        unpackDelta0(out);
        break;
    case 32:
        unpackDelta32(in, out);
        break;
    case 64:
        unpackDelta64(in, out);
        break;
    case 96:
        unpackDelta96(in, out);
        break;
    case 128:
        unpackDelta128(in, out);
        break;
    default:
        for (common::idx_t oindex = 0; oindex < IntegerBitpacking<common::int128_t>::CHUNK_SIZE - 1;
             ++oindex) {
            unpackSingle(in, out + oindex, width,
                (width * oindex) % IntegerBitpacking<common::int128_t>::CHUNK_SIZE);
        }
        unpackLast(in, out, width);
    }
}

} // namespace kuzu::storage
