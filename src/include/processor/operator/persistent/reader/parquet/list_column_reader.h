#pragma once

#include "column_reader.h"
#include "templated_column_reader.h"

namespace kuzu {
namespace processor {

class ListColumnReader : public ColumnReader {
public:
    static constexpr const common::PhysicalTypeID TYPE = common::PhysicalTypeID::VAR_LIST;

public:
    ListColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
        const kuzu_parquet::format::SchemaElement& schema, uint64_t schemaIdx, uint64_t maxDefine,
        uint64_t maxRepeat, std::unique_ptr<ColumnReader> childColumnReader,
        storage::MemoryManager* memoryManager);

    inline void initializeRead(uint64_t rowGroupIdx,
        const std::vector<kuzu_parquet::format::ColumnChunk>& columns,
        kuzu_apache::thrift::protocol::TProtocol& protocol) override {
        childColumnReader->initializeRead(rowGroupIdx, columns, protocol);
    }

    uint64_t read(uint64_t numValues, parquet_filter_t& filter, uint8_t* defineOut,
        uint8_t* repeatOut, common::ValueVector* resultOut) override;

    void applyPendingSkips(uint64_t numValues) override;

private:
    inline uint64_t getGroupRowsAvailable() override {
        return childColumnReader->getGroupRowsAvailable() + overflowChildCount;
    }

    inline uint64_t getTotalCompressedSize() override {
        return childColumnReader->getTotalCompressedSize();
    }

    inline void registerPrefetch(ThriftFileTransport& transport, bool allow_merge) override {
        childColumnReader->registerPrefetch(transport, allow_merge);
    }

private:
    std::unique_ptr<ColumnReader> childColumnReader;
    ResizeableBuffer childDefines;
    ResizeableBuffer childRepeats;
    uint8_t* childDefinesPtr;
    uint8_t* childRepeatsPtr;
    parquet_filter_t childFilter;
    uint64_t overflowChildCount;
    std::unique_ptr<common::ValueVector> vectorToRead;
    storage::MemoryManager* memoryManager;
};

} // namespace processor
} // namespace kuzu
