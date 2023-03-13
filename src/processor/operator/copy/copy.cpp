#include "processor/operator/copy/copy.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Copy::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    inputVector = resultSet->getValueVector(inputPos).get();
    outputVector = resultSet->getValueVector(outputPos).get();
    numThreads = context->numThreads;
}

bool Copy::getNextTuplesInternal() {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    children[0]->getNextTuple();
    metrics->executionTime.start();
    if (!allowCopyCSV()) {
        throw CopyException("COPY commands can only be executed once on a table.");
    }
    auto copyDescription = getCopyDescription();
    auto numTuplesCopied = copy(copyDescription, numThreads);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(1);
    outputVector->setValue<std::string>(0, getOutputMsg(numTuplesCopied));
    return true;
}

/*!
 * @brief: We currently support two ways to specify the input file path:
 * 1. String Literal: "path/to/file.csv".
 * 2. List Literal: ["path/to/file1.csv", "path/to/file2.csv"].
 * Note: we also support wild card pattern matching for file paths.
 */
CopyDescription Copy::getCopyDescription() const {
    std::vector<std::string> filePaths;
    if (inputVector->dataType.typeID == DataTypeID::STRING) {
        auto rawPath =
            inputVector
                ->getValue<common::ku_string_t>(inputVector->state->selVector->selectedPositions[0])
                .getAsString();
        globPath(filePaths, rawPath);
    } else if (inputVector->dataType.typeID == DataTypeID::VAR_LIST) {
        auto lstOfRawPaths = inputVector->getValue<common::ku_list_t>(
            inputVector->state->selVector->selectedPositions[0]);
        for (auto i = 0; i < lstOfRawPaths.size; i++) {
            auto rawPath = ((common::ku_string_t*)lstOfRawPaths.overflowPtr)[i].getAsString();
            globPath(filePaths, rawPath);
        }
    }
    if (filePaths.empty()) {
        throw CopyException("No valid file is specified in CopyCSV.");
    }
    return {filePaths, csvReaderConfig};
}

} // namespace processor
} // namespace kuzu
