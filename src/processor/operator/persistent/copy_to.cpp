#include "processor/operator/persistent/copy_to.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CopyTo::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->csvFileWriter->open(getCopyDescription().filePaths[0]);
    sharedState->csvFileWriter->writeHeader(getCopyDescription().columnNames);
}

void CopyTo::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    outputVectors.reserve(vectorsToCopyPos.size());
    for (auto& pos : vectorsToCopyPos) {
        outputVectors.push_back(resultSet->getValueVector(pos).get());
    }
}

bool CopyTo::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    sharedState->csvFileWriter->writeValues(outputVectors);
    return true;
}

} // namespace processor
} // namespace kuzu
