#include "processor/operator/persistent/reader/rdf/rdf_scan.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

void RdfScanSharedState::read(common::DataChunk& dataChunk) {
    std::lock_guard<std::mutex> mtx{lock};
    do {
        if (fileIdx > readerConfig.getNumFiles()) {
            return;
        }
        if (reader->read(&dataChunk) > 0) {
            return;
        }
        fileIdx++;
        initReader();
    } while (true);
}

void RdfScanSharedState::initReader() {
    if (fileIdx >= readerConfig.getNumFiles()) {
        return;
    }
    auto path = readerConfig.filePaths[fileIdx];
    reader = std::make_unique<RdfReader>(path, readerConfig.fileType, mode);
    reader->initReader();
}

function::function_set RdfScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(READ_RDF_FUNC_NAME, tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    functionSet.push_back(std::move(func));
    return functionSet;
}

void RdfScan::tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk) {
    auto sharedState = reinterpret_cast<RdfScanSharedState*>(input.sharedState);
    sharedState->read(outputChunk);
}

std::unique_ptr<function::TableFuncBindData> RdfScan::bindFunc(main::ClientContext* /*context*/,
    function::TableFuncBindInput* input_, catalog::Catalog* /*catalog*/,
    storage::StorageManager* /*storageManager*/) {
    auto input = ku_dynamic_cast<function::TableFuncBindInput*, RdfScanBindInput*>(input_);
    return std::make_unique<RdfScanBindData>(input->mode, input->config->copy());
}

std::unique_ptr<function::TableFuncSharedState> RdfScan::initSharedState(
    function::TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, RdfScanBindData*>(input.bindData);
    return std::make_unique<RdfScanSharedState>(*bindData->config, 0 /* numRows */, bindData->mode);
}

std::unique_ptr<function::TableFuncLocalState> RdfScan::initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/,
    storage::MemoryManager* /*mm*/) {
    return std::make_unique<TableFuncLocalState>();
}

} // namespace processor
} // namespace kuzu
