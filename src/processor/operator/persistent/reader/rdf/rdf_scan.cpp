#include "processor/operator/persistent/reader/rdf/rdf_scan.h"

#include "function/table_functions/bind_input.h"
#include "processor/operator/persistent/reader/rdf/rdf_utils.h"

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
    reader = std::make_unique<RdfReader>(path, readerConfig.fileType, mode, store.get());
    reader->initReader();
}

std::pair<uint64_t, uint64_t> RdfInMemScanSharedState::getRange(
    const TripleStore& triples, uint64_t& cursor) {
    std::unique_lock lck{lock};
    auto numTuplesToScan = std::min(batchSize, triples.subjects.size() - cursor);
    auto startIdx = cursor;
    cursor += numTuplesToScan;
    return {startIdx, numTuplesToScan};
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    main::ClientContext*, TableFuncBindInput* input_, catalog::Catalog*, storage::StorageManager*) {
    auto input = ku_dynamic_cast<TableFuncBindInput*, ScanTableFuncBindInput*>(input_);
    return std::make_unique<RdfScanBindData>(common::logical_types_t{}, std::vector<std::string>{},
        input->mm, input->config.copy(), input->vfs, std::make_shared<RdfStore>());
}

static void scanTableFunc(TableFunctionInput& input, common::DataChunk& outputChunk) {
    auto sharedState = reinterpret_cast<RdfScanSharedState*>(input.sharedState);
    sharedState->read(outputChunk);
}

static std::unique_ptr<TableFuncSharedState> inMemScanInitSharedState(
    TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, RdfScanBindData*>(input.bindData);
    return std::make_unique<RdfInMemScanSharedState>(bindData->store);
}

static std::unique_ptr<TableFuncLocalState> initLocalState(
    TableFunctionInitInput&, TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

function_set RdfResourceScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(READ_RDF_RESOURCE_FUNC_NAME, scanTableFunc, nullptr,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{});
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfLiteralScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(READ_RDF_LITERAL_FUNC_NAME, scanTableFunc, nullptr,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{});
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfResourceTripleScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(READ_RDF_RESOURCE_TRIPLE_FUNC_NAME, scanTableFunc,
        nullptr, initSharedState, initLocalState, std::vector<LogicalTypeID>{});
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfLiteralTripleScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(READ_RDF_LITERAL_TRIPLE_FUNC_NAME, scanTableFunc,
        nullptr, initSharedState, initLocalState, std::vector<LogicalTypeID>{});
    functionSet.push_back(std::move(func));
    return functionSet;
}

function::function_set RdfAllTripleScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(READ_RDF_ALL_TRIPLE_FUNC_NAME, scanTableFunc,
        bindFunc, initSharedState, initLocalState, std::vector<LogicalTypeID>{});
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfResourceInMemScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(IN_MEM_READ_RDF_RESOURCE_FUNC_NAME, tableFunc,
        nullptr, inMemScanInitSharedState, initLocalState, std::vector<LogicalTypeID>{});
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfLiteralInMemScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(IN_MEM_READ_RDF_LITERAL_FUNC_NAME, tableFunc,
        nullptr, inMemScanInitSharedState, initLocalState, std::vector<LogicalTypeID>{});
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfResourceTripleInMemScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(IN_MEM_READ_RDF_RESOURCE_TRIPLE_FUNC_NAME,
        tableFunc, nullptr, inMemScanInitSharedState, initLocalState, std::vector<LogicalTypeID>{});
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfLiteralTripleInMemScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(IN_MEM_READ_RDF_LITERAL_TRIPLE_FUNC_NAME, tableFunc,
        nullptr, inMemScanInitSharedState, initLocalState, std::vector<LogicalTypeID>{});
    functionSet.push_back(std::move(func));
    return functionSet;
}

void RdfResourceInMemScan::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, RdfInMemScanSharedState*>(input.sharedState);
    auto sVector = outputChunk.getValueVector(0).get();
    auto vectorPos = 0u;
    auto [rStartIdx, rNumTuplesToScan] = sharedState->getResourceTripleRange();
    if (rNumTuplesToScan == 0) {
        // scan literal triples
        auto triples = &sharedState->store->literalTripleStore;
        auto [lStartIdx, lNumTuplesToScan] = sharedState->getLiteralTripleRange();
        for (auto i = 0u; i < lNumTuplesToScan; ++i) {
            StringVector::addString(sVector, vectorPos++, triples->subjects[lStartIdx + i]);
            StringVector::addString(sVector, vectorPos++, triples->predicates[lStartIdx + i]);
        }
        outputChunk.state->selVector->selectedSize = vectorPos;
        return;
    }
    auto triples = &sharedState->store->resourceTripleStore;
    for (auto i = 0u; i < rNumTuplesToScan; ++i) {
        StringVector::addString(sVector, vectorPos++, triples->subjects[rStartIdx + i]);
        StringVector::addString(sVector, vectorPos++, triples->predicates[rStartIdx + i]);
        StringVector::addString(sVector, vectorPos++, triples->objects[rStartIdx + i]);
    }
    outputChunk.state->selVector->selectedSize = vectorPos;
}

void RdfLiteralInMemScan::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, RdfInMemScanSharedState*>(input.sharedState);
    auto oVector = outputChunk.getValueVector(0).get();
    auto triples = &sharedState->store->literalTripleStore;
    auto [startIdx, numTuplesToScan] = sharedState->getLiteralTripleRange();
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        auto& object = triples->objects[startIdx + i];
        auto& objectType = triples->objectType[startIdx + i];
        if (objectType.empty()) {
            RdfUtils::addRdfLiteral(oVector, i, object.data(), object.size());
        } else {
            RdfUtils::addRdfLiteral(
                oVector, i, object.data(), object.size(), objectType.data(), objectType.size());
        }
    }
    outputChunk.state->selVector->selectedSize = numTuplesToScan;
}

void RdfResourceTripleInMemScan::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, RdfInMemScanSharedState*>(input.sharedState);
    auto [startIdx, numTuplesToScan] = sharedState->getResourceTripleRange();
    auto sVector = outputChunk.getValueVector(0).get();
    auto pVector = outputChunk.getValueVector(1).get();
    auto oVector = outputChunk.getValueVector(2).get();
    auto triples = &sharedState->store->resourceTripleStore;
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(sVector, i, triples->subjects[startIdx + i]);
        StringVector::addString(pVector, i, triples->predicates[startIdx + i]);
        StringVector::addString(oVector, i, triples->objects[startIdx + i]);
    }
    outputChunk.state->selVector->selectedSize = numTuplesToScan;
}

void RdfLiteralTripleInMemScan::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, RdfInMemScanSharedState*>(input.sharedState);
    auto [startIdx, numTuplesToScan] = sharedState->getLiteralTripleRange();
    auto sVector = outputChunk.getValueVector(0).get();
    auto pVector = outputChunk.getValueVector(1).get();
    auto oVector = outputChunk.getValueVector(2).get();
    auto triples = &sharedState->store->literalTripleStore;
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(sVector, i, triples->subjects[startIdx + i]);
        StringVector::addString(pVector, i, triples->predicates[startIdx + i]);
        oVector->setValue(i, startIdx + i);
    }
    outputChunk.state->selVector->selectedSize = numTuplesToScan;
}

std::unique_ptr<TableFuncSharedState> RdfResourceScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    return std::make_unique<RdfScanSharedState>(bindData->config.copy(), RdfReaderMode::RESOURCE);
}

std::unique_ptr<TableFuncSharedState> RdfLiteralScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    return std::make_unique<RdfScanSharedState>(bindData->config.copy(), RdfReaderMode::LITERAL);
}

std::unique_ptr<TableFuncSharedState> RdfResourceTripleScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    return std::make_unique<RdfScanSharedState>(
        bindData->config.copy(), RdfReaderMode::RESOURCE_TRIPLE);
}

std::unique_ptr<TableFuncSharedState> RdfLiteralTripleScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    return std::make_unique<RdfScanSharedState>(
        bindData->config.copy(), RdfReaderMode::LITERAL_TRIPLE);
}

std::unique_ptr<TableFuncSharedState> RdfAllTripleScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, RdfScanBindData*>(input.bindData);
    return std::make_unique<RdfScanSharedState>(
        bindData->config.copy(), RdfReaderMode::ALL, bindData->store);
}

} // namespace processor
} // namespace kuzu
