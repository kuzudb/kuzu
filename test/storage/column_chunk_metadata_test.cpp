#include "common/serializer/buffered_serializer.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "gmock/gmock-matchers.h"
#include "gtest/gtest.h"
#include "storage/store/column_chunk_metadata.h"

using namespace kuzu::common;
using namespace kuzu::storage;

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

bool operator==(const ColumnChunkMetadata& a, const ColumnChunkMetadata& b) {
    return (a.compMeta == b.compMeta) && (a.numPages == b.numPages) &&
           (a.numValues == b.numValues) && (a.pageIdx == b.pageIdx);
}

struct BufferReader : Reader {
    BufferReader(uint8_t* data, size_t dataSize) : data(data), dataSize(dataSize), readSize(0) {}

    void read(uint8_t* outputData, uint64_t size) final {
        memcpy(outputData, data + readSize, size);
        readSize += size;
    }

    bool finished() final { return (readSize >= dataSize); }

    uint8_t* data;
    size_t dataSize;
    size_t readSize;
};

void testSerializeThenDeserialize(const ColumnChunkMetadata& orig) {
    const auto writer = std::make_shared<BufferedSerializer>();
    Serializer ser{writer};
    orig.serialize(ser);

    Deserializer deser{std::make_unique<BufferReader>(writer->getBlobData(), writer->getSize())};
    const auto deserialized = ColumnChunkMetadata::deserialize(deser);
    EXPECT_TRUE(orig == deserialized);
}

TEST(ColumnChunkMetadataTests, DoubleChunkMetadataSerializeThenDeserialize) {
    alp::state alpState{};
    alpState.exceptions_count = 1 << 17;
    alpState.exp = 10;
    alpState.fac = 5;

    const CompressionMetadata origCompMeta{StorageValue{-1.01}, StorageValue{1.01},
        CompressionType::ALP, alpState, StorageValue{0}, StorageValue{1}, PhysicalTypeID::DOUBLE};
    const ColumnChunkMetadata orig{1, 2, 3, origCompMeta};

    testSerializeThenDeserialize(orig);
}
