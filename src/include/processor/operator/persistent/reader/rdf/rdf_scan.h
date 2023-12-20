#pragma once

#include "function/scalar_function.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "function/table_functions/scan_functions.h"
#include "rdf_reader.h"
#include "rdf_reader_mode.h"

namespace kuzu {
namespace processor {

struct RdfScanSharedState final : public function::ScanSharedState {
    std::unique_ptr<RdfReader> reader;
    RdfReaderMode mode;

    RdfScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows, RdfReaderMode mode)
        : ScanSharedState{readerConfig, numRows}, mode{mode} {
        initReader();
    }

    void read(common::DataChunk& dataChunk);

    void initReader();
};

struct RdfScanBindInput : public function::TableFuncBindInput {
    RdfReaderMode mode;
    std::unique_ptr<common::ReaderConfig> config;
};

struct RdfScanBindData : public function::TableFuncBindData {
    RdfReaderMode mode;
    std::unique_ptr<common::ReaderConfig> config;

    RdfScanBindData(RdfReaderMode mode, std::unique_ptr<common::ReaderConfig> config)
        : function::TableFuncBindData{}, mode{mode}, config{std::move(config)} {}
    RdfScanBindData(const RdfScanBindData& other)
        : function::TableFuncBindData{other}, mode{other.mode}, config{other.config->copy()} {}

    std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<RdfScanBindData>(*this);
    }
};

struct RdfScan {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::Catalog* /*catalog*/,
        storage::StorageManager* /*storageManager*/);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& input, function::TableFuncSharedState* /*state*/,
        storage::MemoryManager* /*mm*/);
};

} // namespace processor
} // namespace kuzu
