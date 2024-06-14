#include <algorithm>

#include "common/numeric_utils.h"
#include "gmock/gmock-matchers.h"
#include "gtest/gtest.h"
#include "storage/compression/bitpacking_int128.h"
#include "storage/compression/compression.h"

using namespace kuzu::common;
using namespace kuzu::storage;

TEST(BitpackingTest, Int128PackSanityTest) {
    std::vector<int128_t> inputBuffer = {1, 2, 3, 4, 5, 6, 7, 8};
    inputBuffer.resize(IntegerBitpacking<uint32_t>::CHUNK_SIZE);

    std::vector<uint32_t> compressedBuffer(inputBuffer.size(), 0);

    const size_t inputBitWidth =
        NumericUtils::BitWidth(*std::max_element(inputBuffer.begin(), inputBuffer.end()));
    Int128Packer::pack(inputBuffer.data(), compressedBuffer.data(), inputBitWidth);

    std::vector<int128_t> decompressedBuffer(inputBuffer.size(), 0);
    Int128Packer::unpack(compressedBuffer.data(), decompressedBuffer.data(), inputBitWidth);

    ASSERT_THAT(decompressedBuffer, ::testing::ContainerEq(inputBuffer));
}

TEST(BitpackingTest, Int128PackFullChunk) {
    std::vector<int128_t> inputBuffer(IntegerBitpacking<uint32_t>::CHUNK_SIZE,
        (int128_t{1} << 126) | 0b111);

    std::vector<uint32_t> compressedBuffer(inputBuffer.size() * sizeof(int128_t) / sizeof(uint32_t),
        0);

    const size_t inputBitWidth =
        NumericUtils::BitWidth(*std::max_element(inputBuffer.begin(), inputBuffer.end()));
    Int128Packer::pack(inputBuffer.data(), compressedBuffer.data(), inputBitWidth);

    std::vector<int128_t> decompressedBuffer(inputBuffer.size(), 0);
    Int128Packer::unpack(compressedBuffer.data(), decompressedBuffer.data(), inputBitWidth);

    ASSERT_THAT(decompressedBuffer, ::testing::ContainerEq(inputBuffer));
}

TEST(BitpackingTest, Int128PackFullChunkNoCompress) {
    std::vector<int128_t> inputBuffer(IntegerBitpacking<uint32_t>::CHUNK_SIZE,
        (int128_t{1} << 127) | 0b111);

    std::vector<uint32_t> compressedBuffer(inputBuffer.size() * sizeof(int128_t) / sizeof(uint32_t),
        0);

    const size_t inputBitWidth =
        NumericUtils::BitWidth(*std::max_element(inputBuffer.begin(), inputBuffer.end()));
    Int128Packer::pack(inputBuffer.data(), compressedBuffer.data(), inputBitWidth);

    std::vector<int128_t> decompressedBuffer(inputBuffer.size(), 0);
    Int128Packer::unpack(compressedBuffer.data(), decompressedBuffer.data(), inputBitWidth);

    ASSERT_THAT(decompressedBuffer, ::testing::ContainerEq(inputBuffer));
}
