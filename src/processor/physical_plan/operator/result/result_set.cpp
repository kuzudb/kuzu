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
    auto clonedResultSet = make_unique<ResultSet>(dataChunks.size());
    for (auto i = 0u; i < dataChunks.size(); ++i) {
        clonedResultSet->insert(i, dataChunks[i]->clone());
    }
    clonedResultSet->multiplicity = multiplicity;
    return clonedResultSet;
}

} // namespace processor
} // namespace graphflow
