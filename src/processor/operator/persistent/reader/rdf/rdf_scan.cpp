#include "processor/operator/persistent/reader/rdf/rdf_scan.h"

#include "function/table/bind_input.h"
#include "processor/execution_context.h"
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

std::pair<uint64_t, uint64_t> RdfInMemScanSharedState::getRange(const RdfStore& store_,
    uint64_t& cursor) {
    std::unique_lock lck{lock};
    auto numTuplesToScan = std::min(batchSize, store_.size() - cursor);
    auto startIdx = cursor;
    cursor += numTuplesToScan;
    return {startIdx, numTuplesToScan};
}

static common::offset_t scanTableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = reinterpret_cast<RdfScanSharedState*>(input.sharedState);
    sharedState->read(output.dataChunk);
    return output.dataChunk.state->getSelVector().getSelSize();
}

static std::unique_ptr<TableFuncSharedState> inMemScanInitSharedState(
    TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<RdfScanBindData*>(input.bindData);
    return std::make_unique<RdfInMemScanSharedState>(bindData->store);
}

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput&,
    TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

static offset_t RdfAllTripleScanTableFunc(TableFuncInput& input, TableFuncOutput&) {
    auto sharedState = reinterpret_cast<RdfScanSharedState*>(input.sharedState);
    sharedState->readAll();
    return 0;
}

static std::unique_ptr<function::TableFuncBindData> RdfAllTripleScanBindFunc(main::ClientContext*,
    function::ScanTableFuncBindInput* input) {
    return std::make_unique<RdfScanBindData>(std::vector<common::LogicalType>{},
        std::vector<std::string>{}, input->config.copy(), input->context,
        std::make_shared<TripleStore>());
}

static offset_t RdfResourceInMemScanTableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = ku_dynamic_cast<RdfInMemScanSharedState*>(input.sharedState);
    auto& sVector = output.dataChunk.getValueVectorMutable(0);
    auto vectorPos = 0u;
    // Scan resource triples
    auto& store = ku_dynamic_cast<TripleStore&>(*sharedState->store);
    auto [rStartIdx, rNumTuplesToScan] = sharedState->getResourceTripleRange();
    if (rNumTuplesToScan == 0) {
        // scan literal triples
        auto [lStartIdx, lNumTuplesToScan] = sharedState->getLiteralTripleRange();
        for (auto i = 0u; i < lNumTuplesToScan; ++i) {
            StringVector::addString(&sVector, vectorPos++, store.ltStore.subjects[lStartIdx + i]);
            StringVector::addString(&sVector, vectorPos++, store.ltStore.predicates[lStartIdx + i]);
        }
        return vectorPos;
    }
    for (auto i = 0u; i < rNumTuplesToScan; ++i) {
        StringVector::addString(&sVector, vectorPos++, store.rtStore.subjects[rStartIdx + i]);
        StringVector::addString(&sVector, vectorPos++, store.rtStore.predicates[rStartIdx + i]);
        StringVector::addString(&sVector, vectorPos++, store.rtStore.objects[rStartIdx + i]);
    }
    return vectorPos;
}

static offset_t RdfLiteralInMemScanTableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = ku_dynamic_cast<RdfInMemScanSharedState*>(input.sharedState);
    auto& oVector = output.dataChunk.getValueVectorMutable(0);
    auto& langVector = output.dataChunk.getValueVectorMutable(1);
    auto& store = ku_dynamic_cast<TripleStore&>(*sharedState->store);
    auto [startIdx, numTuplesToScan] = sharedState->getLiteralTripleRange();
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        auto& object = store.ltStore.objects[startIdx + i];
        auto& objectType = store.ltStore.objectTypes[startIdx + i];
        auto& lang = store.ltStore.langs[startIdx + i];
        RdfUtils::addRdfLiteral(&oVector, i, object, objectType);
        StringVector::addString(&langVector, i, lang);
    }
    return numTuplesToScan;
}

static offset_t RdfResourceTripleInMemScanTableFunc(TableFuncInput& input,
    TableFuncOutput& output) {
    auto sharedState = ku_dynamic_cast<RdfInMemScanSharedState*>(input.sharedState);
    auto [startIdx, numTuplesToScan] = sharedState->getResourceTripleRange();
    auto& sVector = output.dataChunk.getValueVectorMutable(0);
    auto& pVector = output.dataChunk.getValueVectorMutable(1);
    auto& oVector = output.dataChunk.getValueVectorMutable(2);
    auto& store = ku_dynamic_cast<TripleStore&>(*sharedState->store);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(&sVector, i, store.rtStore.subjects[startIdx + i]);
        StringVector::addString(&pVector, i, store.rtStore.predicates[startIdx + i]);
        StringVector::addString(&oVector, i, store.rtStore.objects[startIdx + i]);
    }
    return numTuplesToScan;
}

static offset_t RdfLiteralTripleInMemScanTableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = ku_dynamic_cast<RdfInMemScanSharedState*>(input.sharedState);
    auto [startIdx, numTuplesToScan] = sharedState->getLiteralTripleRange();
    auto& sVector = output.dataChunk.getValueVectorMutable(0);
    auto& pVector = output.dataChunk.getValueVectorMutable(1);
    auto& oVector = output.dataChunk.getValueVectorMutable(2);
    auto& store = ku_dynamic_cast<TripleStore&>(*sharedState->store);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(&sVector, i, store.ltStore.subjects[startIdx + i]);
        StringVector::addString(&pVector, i, store.ltStore.predicates[startIdx + i]);
        oVector.setValue(i, startIdx + i);
    }
    return numTuplesToScan;
}

static std::unique_ptr<TableFuncSharedState> RdfResourceScanInitSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfResourceScanSharedState>(bindData->config.copy(),
        std::move(rdfConfig));
}

static std::unique_ptr<TableFuncSharedState> RdfLiteralScanInitSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfLiteralScanSharedState>(bindData->config.copy(),
        std::move(rdfConfig));
}

static std::unique_ptr<TableFuncSharedState> RdfResourceTripleScanInitSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfResourceTripleScanSharedState>(bindData->config.copy(),
        std::move(rdfConfig));
}

static std::unique_ptr<TableFuncSharedState> RdfLiteralTripleScanInitSharedState(
    TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfLiteralTripleScanSharedState>(bindData->config.copy(),
        std::move(rdfConfig));
}

static std::unique_ptr<TableFuncSharedState> RdfAllTripleScanInitSharedState(
    TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<RdfScanBindData*>(input.bindData);
    auto rdfConfig = RdfReaderConfig::construct(bindData->config.options);
    return std::make_unique<RdfTripleScanSharedState>(bindData->config.copy(), std::move(rdfConfig),
        bindData->store);
}

static double RdfResourceInMemScanProgressFunc(TableFuncSharedState* sharedState) {
    auto rdfSharedState = ku_dynamic_cast<RdfInMemScanSharedState*>(sharedState);
    uint64_t rtSize = ku_dynamic_cast<TripleStore*>(rdfSharedState->store.get())->rtStore.size();
    if (rtSize == 0) {
        return 0.0;
    }
    return static_cast<double>(rdfSharedState->rtCursor) / rtSize;
}

static double RdfLiteralInMemScanProgressFunc(TableFuncSharedState* sharedState) {
    auto rdfSharedState = ku_dynamic_cast<RdfInMemScanSharedState*>(sharedState);
    uint64_t ltSize = ku_dynamic_cast<TripleStore*>(rdfSharedState->store.get())->ltStore.size();
    if (ltSize == 0) {
        return 0.0;
    }
    return static_cast<double>(rdfSharedState->ltCursor) / ltSize;
}

static double RdfResourceTripleInMemScanProgressFunc(TableFuncSharedState* sharedState) {
    auto rdfSharedState = ku_dynamic_cast<RdfInMemScanSharedState*>(sharedState);
    TripleStore* store = ku_dynamic_cast<TripleStore*>(rdfSharedState->store.get());
    uint64_t size = store->rtStore.size() + store->ltStore.size();
    if (size == 0) {
        return 0.0;
    }
    return static_cast<double>(rdfSharedState->rtCursor + rdfSharedState->ltCursor) / size;
}

static void finalizeFunc(ExecutionContext* ctx, TableFuncSharedState* sharedState,
    TableFuncLocalState*) {
    auto state = ku_dynamic_cast<RdfScanSharedState*>(sharedState);
    for (idx_t i = 0; i < state->readerConfig.getNumFiles(); ++i) {
        ctx->clientContext->getWarningContextUnsafe().defaultPopulateAllWarnings(ctx->queryID);
    }
}

static void finalizeFuncInMem(ExecutionContext* ctx, TableFuncSharedState*, TableFuncLocalState*) {
    // file idx doesn't apply here
    ctx->clientContext->getWarningContextUnsafe().defaultPopulateAllWarnings(ctx->queryID);
}

function_set RdfResourceScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, scanTableFunc, nullptr,
        RdfResourceScanInitSharedState, initLocalState, std::vector<LogicalTypeID>{}, finalizeFunc);
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfLiteralScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, scanTableFunc, nullptr,
        RdfLiteralScanInitSharedState, initLocalState, std::vector<LogicalTypeID>{}, finalizeFunc);
    func->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfResourceTripleScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, scanTableFunc, nullptr,
        RdfResourceTripleScanInitSharedState, initLocalState, std::vector<LogicalTypeID>{},
        finalizeFunc);
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfLiteralTripleScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, scanTableFunc, nullptr,
        RdfLiteralTripleScanInitSharedState, initLocalState, std::vector<LogicalTypeID>{},
        finalizeFunc);
    functionSet.push_back(std::move(func));
    return functionSet;
}

function::function_set RdfAllTripleScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, RdfAllTripleScanTableFunc,
        RdfAllTripleScanBindFunc, RdfAllTripleScanInitSharedState, initLocalState,
        std::vector<LogicalTypeID>{}, finalizeFunc);
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfResourceInMemScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, RdfResourceInMemScanTableFunc, nullptr,
        inMemScanInitSharedState, initLocalState, RdfResourceInMemScanProgressFunc,
        std::vector<LogicalTypeID>{}, finalizeFuncInMem);
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfLiteralInMemScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, RdfLiteralInMemScanTableFunc, nullptr,
        inMemScanInitSharedState, initLocalState, RdfLiteralInMemScanProgressFunc,
        std::vector<LogicalTypeID>{}, finalizeFuncInMem);
    func->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfResourceTripleInMemScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, RdfResourceTripleInMemScanTableFunc, nullptr,
        inMemScanInitSharedState, initLocalState, RdfResourceTripleInMemScanProgressFunc,
        std::vector<LogicalTypeID>{}, finalizeFuncInMem);
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set RdfLiteralTripleInMemScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, RdfLiteralTripleInMemScanTableFunc, nullptr,
        inMemScanInitSharedState, initLocalState, RdfLiteralInMemScanProgressFunc,
        std::vector<LogicalTypeID>{}, finalizeFuncInMem);
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace processor
} // namespace kuzu
