#include "src/processor/include/physical_plan/operator/result/result_set.h"

namespace graphflow {
namespace processor {

uint64_t ResultSet::getNumTuples() {
    uint64_t numTuples = 1;
    for (auto& dataChunk : dataChunks) {
        numTuples *= dataChunk->state->getNumSelectedValues();
    }
    return numTuples * multiplicity;
}

unique_ptr<ResultSet> ResultSet::clone() {
    auto clonedResultSet = make_unique<ResultSet>();
    for (auto& dataChunk : dataChunks) {
        clonedResultSet->dataChunks.push_back(dataChunk->clone());
    }
    clonedResultSet->multiplicity = multiplicity;
    return clonedResultSet;
}

} // namespace processor
} // namespace graphflow
