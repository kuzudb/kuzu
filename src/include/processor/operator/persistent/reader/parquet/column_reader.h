#pragma once

#include <bitset>

#include "common/constants.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "parquet/parquet_types.h"
#include "parquet_dbp_decoder.h"
#include "parquet_rle_bp_decoder.h"
#include "resizable_buffer.h"
#include "thrift_tools.h"

namespace kuzu {
namespace processor {
class ParquetReader;

typedef std::bitset<common::DEFAULT_VECTOR_CAPACITY> parquet_filter_t;

class ColumnReader {
public:
    ColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
        const kuzu_parquet::format::SchemaElement& schema, uint64_t fileIdx, uint64_t maxDefinition,
        uint64_t maxRepeat);
    virtual ~ColumnReader() = default;
    inline common::LogicalType* getDataType() const { return type.get(); }
    inline bool hasDefines() { return maxDefine > 0; }
    inline bool hasRepeats() { return maxRepeat > 0; }
    virtual inline void skip(uint64_t numValues) { pendingSkips += numValues; }
    virtual inline void dictionary(
        std::shared_ptr<ResizeableBuffer> /*data*/, uint64_t /*num_entries*/) {
        throw common::NotImplementedException{"Dictionary"};
    }
    virtual inline void offsets(uint32_t* /*offsets*/, uint8_t* /*defines*/, uint64_t /*numValues*/,
        parquet_filter_t& /*filter*/, uint64_t /*resultOffset*/, common::ValueVector* /*result*/) {
        throw common::NotImplementedException{"ColumnReader::offsets"};
    }
    virtual inline void plain(std::shared_ptr<ByteBuffer> /*plainData*/, uint8_t* /*defines*/,
        uint64_t /*numValues*/, parquet_filter_t& /*filter*/, uint64_t /*resultOffset*/,
        common::ValueVector* /*result*/) {
        throw common::NotImplementedException{"ColumnReader::plain"};
    }
    virtual inline void resetPage() {}
    virtual inline uint64_t getGroupRowsAvailable() { return groupRowsAvailable; }
    virtual void initializeRead(uint64_t rowGroupIdx,
        const std::vector<kuzu_parquet::format::ColumnChunk>& columns,
        kuzu_apache::thrift::protocol::TProtocol& protocol);
    virtual uint64_t getTotalCompressedSize();
    virtual void registerPrefetch(ThriftFileTransport& transport, bool allowMerge);
    virtual uint64_t fileOffset() const;
    virtual void applyPendingSkips(uint64_t numValues);
    virtual uint64_t read(uint64_t numValues, parquet_filter_t& filter, uint8_t* defineOut,
        uint8_t* repeatOut, common::ValueVector* resultOut);
    static std::unique_ptr<ColumnReader> createReader(ParquetReader& reader,
        std::unique_ptr<common::LogicalType> type,
        const kuzu_parquet::format::SchemaElement& schema, uint64_t fileIdx, uint64_t maxDefine,
        uint64_t maxRepeat);
    void prepareRead(parquet_filter_t& filter);
    void allocateBlock(uint64_t size);
    void allocateCompressed(uint64_t size);
    void decompressInternal(kuzu_parquet::format::CompressionCodec::type codec, const uint8_t* src,
        uint64_t srcSize, uint8_t* dst, uint64_t dstSize);
    void preparePageV2(kuzu_parquet::format::PageHeader& pageHdr);
    void preparePage(kuzu_parquet::format::PageHeader& pageHdr);
    void prepareDataPage(kuzu_parquet::format::PageHeader& pageHdr);
    template<class VALUE_TYPE, class CONVERSION>
    void plainTemplated(std::shared_ptr<ByteBuffer> plainData, uint8_t* defines, uint64_t numValues,
        parquet_filter_t& filter, uint64_t resultOffset, common::ValueVector* result) {
        for (auto i = 0u; i < numValues; i++) {
            if (hasDefines() && defines[i + resultOffset] != maxDefine) {
                result->setNull(i + resultOffset, true);
                continue;
            }
            if (filter[i + resultOffset]) {
                VALUE_TYPE val = CONVERSION::plainRead(*plainData, *this);
                result->setValue(i + resultOffset, val);
            } else { // there is still some data there that we have to skip over
                CONVERSION::plainSkip(*plainData, *this);
            }
        }
    }

protected:
    const kuzu_parquet::format::SchemaElement& schema;

    uint64_t fileIdx;
    uint64_t maxDefine;
    uint64_t maxRepeat;

    ParquetReader& reader;
    std::unique_ptr<common::LogicalType> type;

    uint64_t pendingSkips = 0;

    const kuzu_parquet::format::ColumnChunk* chunk = nullptr;

    kuzu_apache::thrift::protocol::TProtocol* protocol;
    uint64_t pageRowsAvailable;
    uint64_t groupRowsAvailable;
    uint64_t chunkReadOffset;

    std::shared_ptr<ResizeableBuffer> block;

    ResizeableBuffer compressedBuffer;
    ResizeableBuffer offsetBuffer;

    std::unique_ptr<RleBpDecoder> dictDecoder;
    std::unique_ptr<RleBpDecoder> defineDecoder;
    std::unique_ptr<RleBpDecoder> repeatedDecoder;
    std::unique_ptr<DbpDecoder> dbpDecoder;
    std::unique_ptr<RleBpDecoder> rleDecoder;

    // dummies for Skip()
    parquet_filter_t noneFilter;
    ResizeableBuffer dummyDefine;
    ResizeableBuffer dummyRepeat;
};

} // namespace processor
} // namespace kuzu
