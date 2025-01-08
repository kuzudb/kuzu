#include <algorithm>
#include <numeric>

#include "common/constants.h"
#include "common/exception/not_implemented.h"
#include "common/exception/storage.h"
#include "common/serializer/buffered_reader.h"
#include "common/serializer/buffered_serializer.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/reader.h"
#include "common/serializer/serializer.h"
#include "gmock/gmock-matchers.h"
#include "gtest/gtest.h"
#include "storage/compression/compression.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;

/*
 * StorageValue Tests
 */

TEST(CompressionTests, TestStorageValueEquality) {
    KU_ASSERT(StorageValue{-1} == StorageValue{-2 + 1});
    KU_ASSERT(StorageValue{5} == StorageValue{5U});
    KU_ASSERT(
        StorageValue{1} == StorageValue{StorageValue{(int128_t{1} << 100)}.get<int128_t>() >> 100});
}

/*
 * CompressionMetadata Tests
 */

bool operator==(const ALPMetadata& a, const ALPMetadata& b) {
    return (a.exceptionCapacity == b.exceptionCapacity) && (a.exceptionCount == b.exceptionCount) &&
           (a.exp == b.exp) && (a.fac == b.fac);
}

bool operator!=(const ALPMetadata& a, const ALPMetadata& b) {
    return !(a == b);
}

bool operator==(const CompressionMetadata& a, const CompressionMetadata& b) {
    if (a.min != b.min)
        return false;
    if (a.max != b.max)
        return false;
    if (a.extraMetadata.has_value() != b.extraMetadata.has_value())
        return false;
    if (a.extraMetadata.has_value() &&
        *reinterpret_cast<ALPMetadata*>(a.extraMetadata.value().get()) !=
            *reinterpret_cast<ALPMetadata*>(b.extraMetadata.value().get())) {
        return false;
    }
    if (a.children.size() != b.children.size())
        return false;
    for (size_t i = 0; i < a.children.size(); ++i) {
        if (a.getChild(i) != b.getChild(i))
            return false;
    }
    return true;
}

void testSerializeThenDeserialize(const CompressionMetadata& orig) {
    // test serializing/deserializing twice

    const auto writer = std::make_shared<BufferedSerializer>();
    Serializer ser{writer};
    orig.serialize(ser);
    orig.serialize(ser);

    Deserializer deser{std::make_unique<BufferReader>(writer->getBlobData(), writer->getSize())};
    const auto deserialized1 = CompressionMetadata::deserialize(deser);
    EXPECT_TRUE(orig == deserialized1);

    const auto deserialized2 = CompressionMetadata::deserialize(deser);
    EXPECT_TRUE(orig == deserialized2);
}

TEST(CompressionTests, DoubleMetadataSerializeThenDeserialize) {
    alp::state alpState{};
    alpState.exceptions_count = 1 << 17;
    alpState.exp = 10;
    alpState.fac = 5;

    const CompressionMetadata orig{StorageValue{-1.01}, StorageValue{1.01}, CompressionType::ALP,
        alpState, StorageValue{0}, StorageValue{1}, PhysicalTypeID::DOUBLE};

    testSerializeThenDeserialize(orig);
}

TEST(CompressionTests, DoubleUncompressedMetadataSerializeThenDeserialize) {
    alp::state alpState{};
    alpState.exceptions_count = 1 << 17;
    alpState.exp = 10;
    alpState.fac = 5;

    CompressionMetadata orig{StorageValue{-1.01}, StorageValue{1.01},
        CompressionType::UNCOMPRESSED};

    testSerializeThenDeserialize(orig);
}

TEST(CompressionTests, IntMetadataSerializeThenDeserialize) {
    const CompressionMetadata orig{StorageValue{-10}, StorageValue{-5},
        CompressionType::INTEGER_BITPACKING};

    testSerializeThenDeserialize(orig);
}

TEST(CompressionTests, IntegerBitpackingMetadataInvalidPhysicalType) {
    const CompressionMetadata metadata{StorageValue{-10}, StorageValue{-5},
        CompressionType::INTEGER_BITPACKING};
    uint8_t data = 0;

    kuzu::storage::InPlaceUpdateLocalState localUpdateState{};
    EXPECT_THROW(metadata.canUpdateInPlace(&data, 0, 1, PhysicalTypeID::ARRAY, localUpdateState),
        StorageException);

    EXPECT_THROW(metadata.numValues(KUZU_PAGE_SIZE, LogicalType::STRING()), StorageException);

    EXPECT_THROW(metadata.toString(PhysicalTypeID::FLOAT), InternalException);
    EXPECT_THROW(metadata.toString(PhysicalTypeID::BOOL), InternalException);

    uint8_t result = 0;
    PageCursor cursor;
    EXPECT_THROW(ReadCompressedValuesFromPage{LogicalType::DOUBLE()}.operator()(&data, cursor,
                     &result, 0, 1, metadata),
        NotImplementedException);
    EXPECT_THROW(WriteCompressedValuesToPage{LogicalType::DOUBLE()}.operator()(&data, 0, &result, 0,
                     1, metadata),
        NotImplementedException);
}

TEST(CompressionTests, FloatCompressionMetadataInvalidPhysicalType) {
    const CompressionMetadata metadata{StorageValue{-10}, StorageValue{-5}, CompressionType::ALP};
    uint8_t data = 0;

    kuzu::storage::InPlaceUpdateLocalState localUpdateState{};
    EXPECT_THROW(metadata.canUpdateInPlace(&data, 0, 1, PhysicalTypeID::INT32, localUpdateState),
        StorageException);

    EXPECT_THROW(metadata.numValues(KUZU_PAGE_SIZE, LogicalType::UINT64()), StorageException);

    EXPECT_THROW(metadata.toString(PhysicalTypeID::STRUCT), InternalException);

    uint8_t result = 0;
    PageCursor cursor;
    EXPECT_THROW(ReadCompressedValuesFromPage{LogicalType::DATE()}.operator()(&data, cursor,
                     &result, 0, 1, metadata),
        NotImplementedException);
    EXPECT_THROW(WriteCompressedValuesToPage{LogicalType::DATE()}.operator()(&data, 0, &result, 0,
                     1, metadata),
        NotImplementedException);
}

/*
 * Compression Tests
 */

template<typename T>
void test_compression(CompressionAlg& alg, std::vector<T> src, bool force_offset_zero = true) {
    if (force_offset_zero) {
        // Force offset of 0 for bitpacking
        src[0] = 0;
    }
    auto pageSize = 4096;
    std::vector<uint8_t> dest(pageSize);

    const auto& [min, max] = std::minmax_element(src.begin(), src.end());
    auto metadata =
        CompressionMetadata(StorageValue(*min), StorageValue(*max), alg.getCompressionType());
    // For simplicity, we'll ignore the possibility of it requiring multiple pages
    // That's tested separately

    auto numValuesRemaining = src.size();
    const uint8_t* srcCursor = (uint8_t*)src.data();
    alg.compressNextPage(srcCursor, numValuesRemaining, dest.data(), pageSize, metadata);
    std::vector<T> decompressed(src.size());
    alg.decompressFromPage(dest.data(), 0 /*srcOffset*/, (uint8_t*)decompressed.data(),
        0 /*dstOffset*/, src.size(), metadata);
    EXPECT_EQ(src, decompressed);
    // works with all bit widths (but not all offsets)
    T value = 0;
    if (!force_offset_zero) {
        // make sure we can update in place to not fail runtime assertions
        value = *std::min_element(src.begin(), src.end());
    }

    alg.setValuesFromUncompressed((uint8_t*)&value, 0 /*srcOffset*/, (uint8_t*)dest.data(),
        1 /*dstOffset*/, 1 /*numValues*/, metadata, nullptr /*nullMask*/);
    alg.decompressFromPage(dest.data(), 0 /*srcOffset*/, (uint8_t*)decompressed.data(),
        0 /*dstOffset*/, src.size(), metadata);
    src[1] = value;
    EXPECT_EQ(decompressed, src);
    EXPECT_EQ(decompressed[1], value);

    for (auto i = 0u; i < src.size(); i++) {
        alg.decompressFromPage(dest.data(), i, (uint8_t*)decompressed.data(), i, 1 /*numValues*/,
            metadata);
        EXPECT_EQ(decompressed[i], src[i]);
    }
    EXPECT_EQ(decompressed, src);

    // Decompress part of a page
    decompressed.clear();
    decompressed.resize(src.size() / 2);
    alg.decompressFromPage(dest.data(), src.size() / 3 /*srcOffset*/, (uint8_t*)decompressed.data(),
        0 /*dstOffset*/, src.size() / 2 /*numValues*/, metadata);
    auto expected = std::vector(src);
    expected.erase(expected.begin(), expected.begin() + src.size() / 3);
    expected.resize(src.size() / 2);
    EXPECT_EQ(decompressed, expected);

    decompressed.clear();
    decompressed.resize(src.size() / 2);
    alg.decompressFromPage(dest.data(), src.size() / 7 /*srcOffset*/, (uint8_t*)decompressed.data(),
        0 /*dstOffset*/, src.size() / 2 /*numValues*/, metadata);
    expected = std::vector(src);
    expected.erase(expected.begin(), expected.begin() + src.size() / 7);
    expected.resize(src.size() / 2);
    EXPECT_EQ(decompressed, expected);
}

TEST(CompressionTests, BooleanBitpackingTest) {
    std::vector<uint8_t> src{true, false, true, true, false, true, false};
    auto alg = BooleanBitpacking();
    test_compression(alg, src);
}

TEST(CompressionTests, UncompressedTest) {
    std::vector<uint8_t> src{true, false, true, true, false, true, false};
    auto alg = Uncompressed(LogicalType(LogicalTypeID::BOOL));
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTest32) {
    auto length = 128;
    std::vector<int32_t> src(length, 0);
    for (int i = 0; i < length; i++) {
        src[i] = i;
    }
    auto alg = IntegerBitpacking<int32_t>();
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTest32Small) {
    auto length = 7;
    std::vector<int32_t> src(length, 0);
    for (int i = 0; i < length; i++) {
        src[i] = i;
    }
    auto alg = IntegerBitpacking<int32_t>();
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTest64) {
    std::vector<int64_t> src(128, 6);
    auto alg = IntegerBitpacking<int64_t>();
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTest64SetValuesFromUncompressed) {
    std::vector<int64_t> src(128, 51);
    src[0] = 0;
    src[100] = 1LL << 61;
    auto alg = IntegerBitpacking<int64_t>();
    std::vector<int64_t> dest(src.size());

    const auto& [min, max] = std::minmax_element(src.begin(), src.end());
    auto metadata =
        CompressionMetadata(StorageValue(*min), StorageValue(*max), alg.getCompressionType());

    {
        alg.setValuesFromUncompressed((uint8_t*)src.data(), 0, (uint8_t*)dest.data(), 0, src.size(),
            metadata, nullptr);

        std::vector<int64_t> decompressed(src.size());
        alg.decompressFromPage((uint8_t*)dest.data(), 0, (uint8_t*)decompressed.data(), 0,
            decompressed.size(), metadata);

        EXPECT_THAT(decompressed, ::testing::ContainerEq(src));
    }

    {
        static constexpr offset_t startUpdateIdx = 30;
        static constexpr offset_t endUpdateIdx = 70;
        for (offset_t i = startUpdateIdx; i < endUpdateIdx; ++i) {
            src[i] = src[i - 1] * 2 - 1;
        }
        const auto updatedSrc = std::span(src.begin(), src.begin() + endUpdateIdx);
        alg.setValuesFromUncompressed((uint8_t*)updatedSrc.data(), startUpdateIdx,
            (uint8_t*)dest.data(), startUpdateIdx, endUpdateIdx - startUpdateIdx, metadata,
            nullptr);

        std::vector<int64_t> decompressed(src.size());
        alg.decompressFromPage((uint8_t*)dest.data(), 0, (uint8_t*)decompressed.data(), 0,
            decompressed.size(), metadata);

        EXPECT_THAT(decompressed, ::testing::ContainerEq(src));
    }
}

TEST(CompressionTests, IntegerPackingTest128WorksOnNonZeroBuffer) {
    std::vector<int128_t> src(128);
    for (size_t i = 0; i < src.size(); ++i) {
        src[i] = (int128_t(1) << 125) + int128_t{(uint32_t)i};
    }
    auto alg = IntegerBitpacking<int128_t>();
    std::vector<uint8_t> dest(sizeof(int128_t) * src.size(), 0xff);

    const auto& [min, max] = std::minmax_element(src.begin(), src.end());
    auto metadata =
        CompressionMetadata(StorageValue(*min), StorageValue(*max), alg.getCompressionType());

    const auto* srcCursor = (const uint8_t*)src.data();
    alg.compressNextPage(srcCursor, src.size(), dest.data(), dest.size(), metadata);

    std::vector<int128_t> decompressed(src.size());
    alg.decompressFromPage((uint8_t*)dest.data(), 0, (uint8_t*)decompressed.data(), 0,
        decompressed.size(), metadata);

    EXPECT_THAT(decompressed, ::testing::ContainerEq(src));
}

TEST(CompressionTests, IntegerPackingTest128AllPositive) {
    std::vector<kuzu::common::int128_t> src(101);

    {
        kuzu::common::int128_t cur = 1;
        kuzu::common::int128_t diff = 1;
        std::ranges::generate(src.begin(), src.end(), [&diff, &cur] {
            diff *= 2;
            cur += diff;
            return cur;
        });
    }

    auto alg = IntegerBitpacking<kuzu::common::int128_t>();
    test_compression(alg, src, false);
}

TEST(CompressionTests, IntegerPackingTest128SignBitFillingDoesNotBreakUnpacking) {
    std::vector<kuzu::common::int128_t> src(128, 0b1111);

    auto alg = IntegerBitpacking<kuzu::common::int128_t>();
    test_compression(alg, src, false);
}

TEST(CompressionTests, IntegerPackingTest128Negative) {
    std::vector<kuzu::common::int128_t> src(101);

    {
        auto cur = -(kuzu::common::int128_t(1) << 120);
        kuzu::common::int128_t diff = 1;
        std::ranges::generate(src.begin(), src.end(), [&diff, &cur] {
            diff *= 2;
            cur += diff;
            return cur;
        });
    }

    auto alg = IntegerBitpacking<kuzu::common::int128_t>();
    test_compression(alg, src, false);
}

TEST(CompressionTests, IntegerPackingTest128NegativeSimple) {
    std::vector<kuzu::common::int128_t> src{-1024, -1027, -1023};

    auto alg = IntegerBitpacking<kuzu::common::int128_t>();
    test_compression(alg, src, false);
}

TEST(CompressionTests, IntegerPackingTest128CompressFullChunkLargeWidth) {
    std::vector<kuzu::common::int128_t> src{IntegerBitpacking<int128_t>::CHUNK_SIZE,
        (int128_t{1} << 126) | 0b111};

    auto alg = IntegerBitpacking<kuzu::common::int128_t>();
    test_compression(alg, src, false);
}

TEST(CompressionTests, IntegerPackingTestNegative32) {
    std::vector<int32_t> src(128, -6);
    src[5] = 20;
    auto alg = IntegerBitpacking<int32_t>();
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTestNegative64) {
    std::vector<int64_t> src(128, -6);
    src[5] = 20;
    auto alg = IntegerBitpacking<int64_t>();
    test_compression(alg, src);
}

TEST(CompressionTests, CopyMultiPage) {
    int64_t numValues = 512;
    std::vector<int64_t> src(numValues, -6);

    auto alg = Uncompressed(LogicalType(LogicalTypeID::INT64));
    auto pageSize = 64;
    const auto& [min, max] = std::minmax_element(src.begin(), src.end());
    auto metadata =
        CompressionMetadata(StorageValue(*min), StorageValue(*max), alg.getCompressionType());
    auto numValuesRemaining = numValues;
    auto numValuesPerPage = metadata.numValues(pageSize, LogicalType(LogicalTypeID::INT64));
    const uint8_t* srcCursor = (uint8_t*)src.data();
    // TODO: accumulate output and then decompress
    while (numValuesRemaining > 0) {
        std::vector<uint8_t> dest(pageSize);
        auto compressedSize =
            alg.compressNextPage(srcCursor, numValuesRemaining, dest.data(), pageSize, metadata);
        numValuesRemaining -= numValuesPerPage;
        ASSERT_EQ(compressedSize, pageSize);
    }
    ASSERT_EQ((int64_t*)srcCursor - src.data(), numValues);
}

template<typename T>
void integerPackingMultiPage(const std::vector<T>& src) {
    auto alg = IntegerBitpacking<T>();
    auto pageSize = 4096;
    const auto& [min, max] = std::minmax_element(src.begin(), src.end());
    auto metadata =
        CompressionMetadata(StorageValue(*min), StorageValue(*max), alg.getCompressionType());
    auto numValuesPerPage = metadata.numValues(pageSize, LogicalType(LogicalTypeID::INT64));
    int64_t numValuesRemaining = src.size();
    const uint8_t* srcCursor = (uint8_t*)src.data();
    auto pages = src.size() / numValuesPerPage + 1;
    std::vector<std::vector<uint8_t>> dest(pages, std::vector<uint8_t>(pageSize));
    size_t pageNum = 0;
    while (numValuesRemaining > 0) {
        ASSERT_LT(pageNum, pages);
        alg.compressNextPage(srcCursor, numValuesRemaining, dest[pageNum++].data(), pageSize,
            metadata);
        numValuesRemaining -= numValuesPerPage;
    }
    ASSERT_EQ(srcCursor, (uint8_t*)(src.data() + src.size()));
    for (auto i = 0u; i < src.size(); i++) {
        auto page = i / numValuesPerPage;
        auto indexInPage = i % numValuesPerPage;
        T value;
        alg.decompressFromPage(dest[page].data(), indexInPage, (uint8_t*)&value, 0, 1 /*numValues*/,
            metadata);
        EXPECT_EQ(src[i] - value, 0);
        EXPECT_EQ(src[i], value);
    }
    std::vector<T> decompressed(src.size());
    for (auto i = 0u; i < src.size(); i += numValuesPerPage) {
        auto page = i / numValuesPerPage;
        alg.decompressFromPage(dest[page].data(), 0, (uint8_t*)decompressed.data(), i,
            std::min(numValuesPerPage, (uint64_t)src.size() - i), metadata);
    }
    ASSERT_EQ(decompressed, src);
}

TEST(CompressionTests, IntegerPackingMultiPage64) {
    int64_t numValues = 10000;
    std::vector<kuzu::common::int128_t> src(numValues);
    const auto M = (kuzu::common::int128_t(1) << 126) - 1;
    for (int i = 0; i < numValues; i++) {
        src[i] = (i * i) % M;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageNegative64) {
    int64_t numValues = 10000;
    std::vector<kuzu::common::int128_t> src(numValues);

    const auto M = (kuzu::common::int128_t(1) << 126) - 1;
    for (int i = 0; i < numValues; i++) {
        src[i] = -((i * i) % M);
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPage128) {
    int64_t numValues = 10000;
    std::vector<int64_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageNegative128) {
    int64_t numValues = 10000;
    std::vector<int64_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = -i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPage32) {
    int64_t numValues = 10000;
    std::vector<int32_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageNegative32) {
    int64_t numValues = 10000;
    std::vector<int32_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = -i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageUnsigned32) {
    int64_t numValues = 10000;
    std::vector<uint32_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageUnsigned64) {
    int64_t numValues = 10000;
    std::vector<uint64_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageUnsigned16) {
    int64_t numValues = 10000;
    std::vector<uint16_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPage16) {
    int64_t numValues = 10000;
    std::vector<int16_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPage8) {
    int64_t numValues = 10000;
    std::vector<int8_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i % 30 - 15;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageUnsigned8) {
    int64_t numValues = 10000;
    std::vector<uint8_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i % 30;
    }

    integerPackingMultiPage(src);
}

// Tests with a large fixed offset
TEST(CompressionTests, OffsetIntegerPackingMultiPageUnsigned64) {
    int64_t numValues = 100;
    std::vector<uint64_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = 10000000 + i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, OffsetIntegerPackingMultiPageSigned64) {
    int64_t numValues = 100;
    std::vector<int64_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = -10000000 - i;
    }

    integerPackingMultiPage(src);
}
