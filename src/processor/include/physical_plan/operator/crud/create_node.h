#pragma once

#include "src/processor/include/physical_plan/operator/crud/crud.h"
#include "src/storage/include/data_structure/column.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class CreateNode : public CRUDOperator {

public:
    CreateNode(uint64_t dataChunkPos, Transaction* transactionPtr,
        vector<uint32_t> propertyKeyVectorPos, label_t nodeLabel,
        vector<BaseColumn*> nodePropertyColumns, uint64_t numNodes,
        unique_ptr<PhysicalOperator> prevOperator)
        : CRUDOperator{dataChunkPos, transactionPtr, CREATE_NODE, move(prevOperator)},
          propertyKeyVectorPos{move(propertyKeyVectorPos)}, nodeLabel{nodeLabel},
          nodePropertyColumns{nodePropertyColumns}, numNodes{numNodes} {};

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateNode>(dataChunkPos, transactionPtr, propertyKeyVectorPos,
            nodeLabel, nodePropertyColumns, numNodes, prevOperator->clone());
    }

private:
    vector<uint32_t> propertyKeyVectorPos;

    // current state of the graph that will be updated.
    label_t nodeLabel;
    vector<BaseColumn*> nodePropertyColumns;
    uint64_t numNodes;
};

} // namespace processor
} // namespace graphflow
