#include "processor/operator/persistent/reader/rdf/rdf_scan.h"

#include "function/table_functions/bind_input.h"
#include "processor/operator/persistent/reader/rdf/rdf_utils.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

void RdfScanSharedState::read(common::DataChunk& dataChunk) {
    std::lock_guard<std::mutex> mtx{lock};
    do {
        if (fileIdx > readerConfig.getNumFiles()) {
            return;
        }
        if (reader->readChunk(&dataChunk) > 0) {
            return;
        }
        // No more to read in current file, move to next.
        numLiteralTriplesScanned += reader->getNumLiteralTriplesScanned();
        fileIdx++;
        initReader();
        store->clear();
    } while (true);
}

void RdfScanSharedState::readAll() {
    std::lock_guard<std::mutex> mtx{lock};
    do {
        if (fileIdx > readerConfig.getNumFiles()) {
            return;
        }
        reader->readAll();
        fileIdx++;
        initReader();
    } while (true);
}

void RdfScanSharedState::initReader() {
    if (fileIdx >= readerConfig.getNumFiles()) {
        return;
    }
    auto path = readerConfig.filePaths[fileIdx];
    createReader(fileIdx, path, numLiteralTriplesScanned);
    reader->init();
}

std::pair<uint64_t, uint64_t> RdfInMemScanSharedState::getRange(
    const RdfStore& store_, uint64_t& cursor) {
    std::unique_lock lck{lock};
    auto numTuplesToScan = std::min(batchSize, store_.size() - cursor);
    auto startIdx = cursor;
    cursor += numTuplesToScan;
    return {startIdx, numTuplesToScan};
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
    auto func = std::make_unique<TableFunction>(READ_RDF_ALL_TRIPLE_FUNC_NAME, tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{});
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

void RdfAllTripleScan::tableFunc(TableFunctionInput& input, DataChunk&) {
    auto sharedState = reinterpret_cast<RdfScanSharedState*>(input.sharedState);
    sharedState->readAll();
}

std::unique_ptr<function::TableFuncBindData> RdfAllTripleScan::bindFunc(main::ClientContext*,
    function::TableFuncBindInput* input_, catalog::Catalog*, storage::StorageManager*) {
    auto input = ku_dynamic_cast<TableFuncBindInput*, ScanTableFuncBindInput*>(input_);
    return std::make_unique<RdfScanBindData>(std::vector<common::LogicalType>{},
        std::vector<std::string>{}, input->config.copy(), input->context,
        std::make_shared<TripleStore>());
}

void RdfResourceInMemScan::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, RdfInMemScanSharedState*>(input.sharedState);
    auto sVector = outputChunk.getValueVector(0).get();
    auto vectorPos = 0u;
    // Scan resource triples
    auto& store = ku_dynamic_cast<RdfStore&, TripleStore&>(*sharedState->store);
    auto [rStartIdx, rNumTuplesToScan] = sharedState->getResourceTripleRange();
    if (rNumTuplesToScan == 0) {
        // scan literal triples
        auto [lStartIdx, lNumTuplesToScan] = sharedState->getLiteralTripleRange();
        for (auto i = 0u; i < lNumTuplesToScan; ++i) {
            StringVector::addString(sVector, vectorPos++, store.ltStore.subjects[lStartIdx + i]);
            StringVector::addString(sVector, vectorPos++, store.ltStore.predicates[lStartIdx + i]);
        }
        outputChunk.state->selVector->selectedSize = vectorPos;
        return;
    }
    for (auto i = 0u; i < rNumTuplesToScan; ++i) {
        StringVector::addString(sVector, vectorPos++, store.rtStore.subjects[rStartIdx + i]);
        StringVector::addString(sVector, vectorPos++, store.rtStore.predicates[rStartIdx + i]);
        StringVector::addString(sVector, vectorPos++, store.rtStore.objects[rStartIdx + i]);
    }
    outputChunk.state->selVector->selectedSize = vectorPos;
}

void RdfLiteralInMemScan::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, RdfInMemScanSharedState*>(input.sharedState);
    auto oVector = outputChunk.getValueVector(0).get();
    auto langVector = outputChunk.getValueVector(1).get();
    auto& store = ku_dynamic_cast<RdfStore&, TripleStore&>(*sharedState->store);
    auto [startIdx, numTuplesToScan] = sharedState->getLiteralTripleRange();
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        auto& object = store.ltStore.objects[startIdx + i];
        auto& objectType = store.ltStore.objectTypes[startIdx + i];
        auto& lang = store.ltStore.langs[startIdx + i];
        RdfUtils::addRdfLiteral(oVector, i, object, objectType);
        StringVector::addString(langVector, i, lang);
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
    auto& store = ku_dynamic_cast<RdfStore&, TripleStore&>(*sharedState->store);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(sVector, i, store.rtStore.subjects[startIdx + i]);
        StringVector::addString(pVector, i, store.rtStore.predicates[startIdx + i]);
        StringVector::addString(oVector, i, store.rtStore.objects[startIdx + i]);
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
    auto& store = ku_dynamic_cast<RdfStore&, TripleStore&>(*sharedState->store);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(sVector, i, store.ltStore.subjects[startIdx + i]);
        StringVector::addString(pVector, i, store.ltStore.predicates[startIdx + i]);
        oVector->setValue(i, startIdx + i);
    }
    outputChunk.state->selVector->selectedSize = numTuplesToScan;
}

std::unique_ptr<TableFuncSharedState> RdfResourceScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfResourceScanSharedState>(
        bindData->config.copy(), std::move(rdfConfig));
}

std::unique_ptr<TableFuncSharedState> RdfLiteralScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfLiteralScanSharedState>(
        bindData->config.copy(), std::move(rdfConfig));
}

std::unique_ptr<TableFuncSharedState> RdfResourceTripleScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfResourceTripleScanSharedState>(
        bindData->config.copy(), std::move(rdfConfig));
}

std::unique_ptr<TableFuncSharedState> RdfLiteralTripleScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfLiteralTripleScanSharedState>(
        bindData->config.copy(), std::move(rdfConfig));
}

std::unique_ptr<TableFuncSharedState> RdfAllTripleScan::initSharedState(
    TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfTripleScanSharedState>(
        bindData->config.copy(), std::move(rdfConfig), bindData->store);
}

} // namespace processor
} // namespace kuzu
