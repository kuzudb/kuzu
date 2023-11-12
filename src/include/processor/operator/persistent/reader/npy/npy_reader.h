#pragma once

#include <string>
#include <vector>

#include "common/data_chunk/data_chunk.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "function/scalar_function.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "function/table_functions/scan_functions.h"

namespace kuzu {
namespace processor {

class NpyReader {
public:
    explicit NpyReader(const std::string& filePath);

    ~NpyReader();

    size_t getNumElementsPerRow() const;

    uint8_t* getPointerToRow(size_t row) const;

    inline size_t getNumRows() const { return shape[0]; }

    void readBlock(common::block_idx_t blockIdx, common::ValueVector* vectorToRead) const;

    // Used in tests only.
    inline common::LogicalTypeID getType() const { return type; }
    inline std::vector<size_t> getShape() const { return shape; }

    void validate(const common::LogicalType& type_, common::offset_t numRows);

private:
    void parseHeader();
    void parseType(std::string descr);

private:
    std::string filePath;
    int fd;
    size_t fileSize;
    void* mmapRegion;
    size_t dataOffset;
    std::vector<size_t> shape;
    common::LogicalTypeID type;
    static inline const std::string defaultFieldName = "NPY_FIELD";
};

class NpyMultiFileReader {
public:
    explicit NpyMultiFileReader(const std::vector<std::string>& filePaths);

    void readBlock(common::block_idx_t blockIdx, common::DataChunk& dataChunkToRead) const;

private:
    std::vector<std::unique_ptr<NpyReader>> fileReaders;
};

struct NpyScanSharedState final : public function::ScanSharedState {
    explicit NpyScanSharedState(const common::ReaderConfig readerConfig, uint64_t numRows);

    std::unique_ptr<NpyMultiFileReader> npyMultiFileReader;
};

struct NpyScanFunction {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::CatalogContent* catalog);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/);

    static void bindColumns(const common::ReaderConfig& readerConfig,
        std::vector<std::string>& columnNames,
        std::vector<std::unique_ptr<common::LogicalType>>& columnTypes);
    static void bindColumns(const common::ReaderConfig& readerConfig, uint32_t fileIdx,
        std::vector<std::string>& columnNames,
        std::vector<std::unique_ptr<common::LogicalType>>& columnTypes);
};

} // namespace processor
} // namespace kuzu
