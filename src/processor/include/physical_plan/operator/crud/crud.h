#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/storage/include/graph.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::transaction;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class CRUDOperator : public PhysicalOperator {

public:
    CRUDOperator(uint64_t dataChunkPos, Transaction* transactionPtr, PhysicalOperatorType type,
        unique_ptr<PhysicalOperator> prevOperator);

    void getNextTuples() override;

protected:
    Transaction* transactionPtr;
    const Graph& graph;
    uint64_t dataChunkPos;
    shared_ptr<DataChunk> inDataChunk;
};

} // namespace processor
} // namespace graphflow
