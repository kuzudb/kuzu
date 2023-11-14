#pragma once

#include "column_reader.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/types.h"
#include "function/scalar_function.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "function/table_functions/scan_functions.h"
#include "parquet/parquet_types.h"
#include "resizable_buffer.h"
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift_tools.h"

namespace kuzu {
namespace processor {

struct ParquetReaderPrefetchConfig {
    // Percentage of data in a row group span that should be scanned for enabling whole group
    // prefetch
    static constexpr double WHOLE_GROUP_PREFETCH_MINIMUM_SCAN = 0.95;
};

struct ParquetReaderScanState {
    std::vector<uint64_t> groupIdxList;
    int64_t currentGroup = -1;
    uint64_t groupOffset;
    std::unique_ptr<common::FileInfo> fileInfo;
    std::unique_ptr<ColumnReader> rootReader;
    std::unique_ptr<kuzu_apache::thrift::protocol::TProtocol> thriftFileProto;

    bool finished;

    ResizeableBuffer defineBuf;
    ResizeableBuffer repeatBuf;

    // TODO(Ziyi): We currently only support reading from local file system, thus the prefetch
    // mode is disabled by default. Add this back when we support remote file system.
    bool prefetchMode = false;
    bool currentGroupPrefetched = false;
};

class ParquetReader {
public:
    ParquetReader(const std::string& filePath, storage::MemoryManager* memoryManager);
    ~ParquetReader() = default;

    void initializeScan(ParquetReaderScanState& state, std::vector<uint64_t> groups_to_read);
    bool scanInternal(ParquetReaderScanState& state, common::DataChunk& result);
    void scan(ParquetReaderScanState& state, common::DataChunk& result);
    inline uint64_t getNumRowsGroups() { return metadata->row_groups.size(); }

    inline uint32_t getNumColumns() const { return columnNames.size(); }
    inline std::string getColumnName(uint32_t idx) const { return columnNames[idx]; }
    inline common::LogicalType* getColumnType(uint32_t idx) const { return columnTypes[idx].get(); }

    inline kuzu_parquet::format::FileMetaData* getMetadata() const { return metadata.get(); }

private:
    inline std::unique_ptr<kuzu_apache::thrift::protocol::TProtocol> createThriftProtocol(
        common::FileInfo* fileInfo_, bool prefetch_mode) {
        return std::make_unique<
            kuzu_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(
            std::make_shared<ThriftFileTransport>(fileInfo_, prefetch_mode));
    }
    inline const kuzu_parquet::format::RowGroup& getGroup(ParquetReaderScanState& state) {
        KU_ASSERT(
            state.currentGroup >= 0 && (int64_t)state.currentGroup < state.groupIdxList.size());
        KU_ASSERT(state.groupIdxList[state.currentGroup] >= 0 &&
                  state.groupIdxList[state.currentGroup] < metadata->row_groups.size());
        return metadata->row_groups[state.groupIdxList[state.currentGroup]];
    }
    static std::unique_ptr<common::LogicalType> deriveLogicalType(
        const kuzu_parquet::format::SchemaElement& s_ele);
    void initMetadata();
    std::unique_ptr<ColumnReader> createReader();
    std::unique_ptr<ColumnReader> createReaderRecursive(uint64_t depth, uint64_t maxDefine,
        uint64_t maxRepeat, uint64_t& nextSchemaIdx, uint64_t& nextFileIdx);
    void prepareRowGroupBuffer(ParquetReaderScanState& state, uint64_t colIdx);
    // Group span is the distance between the min page offset and the max page offset plus the max
    // page compressed size
    uint64_t getGroupSpan(ParquetReaderScanState& state);
    uint64_t getGroupCompressedSize(ParquetReaderScanState& state);
    uint64_t getGroupOffset(ParquetReaderScanState& state);

private:
    std::unique_ptr<common::FileInfo> fileInfo;
    std::string filePath;
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    std::unique_ptr<kuzu_parquet::format::FileMetaData> metadata;
    storage::MemoryManager* memoryManager;
};

struct ParquetScanSharedState final : public function::ScanSharedState {
    explicit ParquetScanSharedState(const common::ReaderConfig readerConfig,
        storage::MemoryManager* memoryManager, uint64_t numRows);

    std::vector<std::unique_ptr<ParquetReader>> readers;
    storage::MemoryManager* memoryManager;
};

struct ParquetScanLocalState final : public function::TableFuncLocalState {
    ParquetScanLocalState() { state = std::make_unique<ParquetReaderScanState>(); }

    ParquetReader* reader;
    std::unique_ptr<ParquetReaderScanState> state;
};

struct ParquetScanFunction {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::CatalogContent* catalog);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& input, function::TableFuncSharedState* state);

    static void bindColumns(const common::ReaderConfig& readerConfig, storage::MemoryManager* mm,
        std::vector<std::string>& columnNames,
        std::vector<std::unique_ptr<common::LogicalType>>& columnTypes);
    static void bindColumns(const common::ReaderConfig& readerConfig, uint32_t fileIdx,
        storage::MemoryManager* mm, std::vector<std::string>& columnNames,
        std::vector<std::unique_ptr<common::LogicalType>>& columnTypes);
};

} // namespace processor
} // namespace kuzu
