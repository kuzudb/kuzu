#pragma once

#include "function/scalar_function.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/scan_functions.h"
#include "rdf_reader.h"

namespace kuzu {
namespace processor {

struct RdfScanSharedState final : public function::ScanSharedState {
    std::unique_ptr<RdfReader> reader;
    common::RdfReaderMode mode;
    std::shared_ptr<RdfStore> store;

    RdfScanSharedState(common::ReaderConfig readerConfig, common::RdfReaderMode mode,
        std::shared_ptr<RdfStore> store)
        : ScanSharedState{std::move(readerConfig), 0}, mode{mode}, store{std::move(store)} {
        initReader();
    }
    RdfScanSharedState(common::ReaderConfig readerConfig, common::RdfReaderMode mode)
        : RdfScanSharedState{std::move(readerConfig), mode, nullptr} {}

    void read(common::DataChunk& dataChunk);

    void initReader();
};

struct RdfScanBindData final : public function::ScanBindData {
    std::shared_ptr<RdfStore> store;

    RdfScanBindData(common::logical_types_t columnTypes, std::vector<std::string> columnNames,
        storage::MemoryManager* mm, common::ReaderConfig config, common::VirtualFileSystem* vfs,
        std::shared_ptr<RdfStore> store)
        : function::ScanBindData{std::move(columnTypes), std::move(columnNames), mm,
              std::move(config), vfs},
          store{std::move(store)} {}
    RdfScanBindData(const RdfScanBindData& other)
        : function::ScanBindData{other}, store{other.store} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<RdfScanBindData>(*this);
    }
};

struct RdfInMemScanSharedState : public function::BaseScanSharedState {
    std::shared_ptr<RdfStore> store;
    uint64_t resourceTripleCursor = 0;
    uint64_t literalTripleCursor = 0;
    // Each triple can be read as at most 3 rows (resources). For simplicity, we read 500 triples
    // per batch to avoid exceeding DEFAULT_VECTOR_CAPACITY.
    static constexpr uint64_t batchSize = 500;

    explicit RdfInMemScanSharedState(std::shared_ptr<RdfStore> store)
        : function::BaseScanSharedState{0}, store{std::move(store)} {}

    std::pair<uint64_t, uint64_t> getResourceTripleRange() {
        return getRange(store->resourceTripleStore, resourceTripleCursor);
    }
    std::pair<uint64_t, uint64_t> getLiteralTripleRange() {
        return getRange(store->literalTripleStore, literalTripleCursor);
    }

private:
    std::pair<uint64_t, uint64_t> getRange(const TripleStore& triples, uint64_t& cursor);
};

struct RdfResourceScan {
    static function::function_set getFunctionSet();

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);
};

struct RdfLiteralScan {
    static function::function_set getFunctionSet();

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);
};

struct RdfResourceTripleScan {
    static function::function_set getFunctionSet();

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);
};

struct RdfLiteralTripleScan {
    static function::function_set getFunctionSet();

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);
};

struct RdfAllTripleScan {
    static function::function_set getFunctionSet();

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);
};

struct RdfResourceInMemScan {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);
};

struct RdfLiteralInMemScan {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);
};

struct RdfResourceTripleInMemScan {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);
};

struct RdfLiteralTripleInMemScan {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);
};

} // namespace processor
} // namespace kuzu
