#pragma once

#include "src/processor/include/physical_plan/operator/crud/crud_node.h"
#include "src/storage/include/data_structure/column.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class DeleteNode : public CRUDNode {

public:
    DeleteNode(uint64_t dataChunkPos, Transaction* transactionPtr,
        vector<uint32_t> propertyKeyVectorPos, label_t nodeLabel,
        vector<BaseColumn*> nodePropertyColumns, uint64_t numNodes,
        unique_ptr<PhysicalOperator> prevOperator)
        : CRUDNode{DELETE_NODE, dataChunkPos, transactionPtr, move(propertyKeyVectorPos), nodeLabel,
              move(nodePropertyColumns), numNodes, move(prevOperator)} {};

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DeleteNode>(dataChunkPos, transactionPtr, propertyKeyVectorPos,
            nodeLabel, nodePropertyColumns, numNodes, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
