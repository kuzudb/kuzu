#include "printer/json_printer.h"

#include "json_utils.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"
#include "yyjson.h"

namespace kuzu {
namespace main {

using namespace common;
using namespace storage;

JsonPrinter::JsonPrinter(PrinterType type) : Printer{type} {
    resultVectorState = std::make_shared<DataChunkState>(DEFAULT_VECTOR_CAPACITY);
    resultVectorState->getSelVectorUnsafe().setToUnfiltered(DEFAULT_VECTOR_CAPACITY);
}

std::string JsonPrinter::print(QueryResult& queryResult, storage::MemoryManager& mm) const {
    std::string result;
    result += printHeader();
    result += printBody(queryResult, mm);
    result += printFooter();
    return result;
}

std::string JsonPrinter::printBody(QueryResult& queryResult, MemoryManager& mm) const {
    std::string result;
    std::vector<std::shared_ptr<common::ValueVector>> resultVectors;
    std::vector<common::ValueVector*> scanVectors;
    for (auto& type : queryResult.getColumnDataTypes()) {
        auto resultVector = std::make_shared<ValueVector>(type.copy(), &mm, resultVectorState);
        resultVectors.push_back(resultVector);
        scanVectors.push_back(resultVector.get());
    }
    std::span<ValueVector*> vectorsToScan{scanVectors};

    uint64_t numTuplesScanned = 0;
    auto maxNumTuplesToScanInBatch =
        queryResult.getTable()->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    auto totalNumTuplesToScan = queryResult.getTable()->getNumTuples();
    while (numTuplesScanned < totalNumTuplesToScan) {
        auto numTuplesToScanInBatch =
            std::min<uint64_t>(maxNumTuplesToScanInBatch, totalNumTuplesToScan - numTuplesScanned);
        queryResult.getTable()->scan(vectorsToScan, numTuplesScanned, numTuplesToScanInBatch);
        numTuplesScanned += numTuplesToScanInBatch;
        auto queryResultsInJson =
            json_extension::jsonifyQueryResult(resultVectors, queryResult.getColumnNames());
        for (auto i = 0u; i < queryResultsInJson.size() - 1; i++) {
            result += jsonToString(queryResultsInJson[i]);
            result += printDelim();
            result += JsonPrinter::NEW_LINE;
        }
        // Only print the delimiter when it is not the last record.
        result += jsonToString(queryResultsInJson[queryResultsInJson.size() - 1]);
        if (numTuplesScanned != totalNumTuplesToScan) {
            result += printDelim();
        }
        result += JsonPrinter::NEW_LINE;
    }
    return result;
}

} // namespace main
} // namespace kuzu
