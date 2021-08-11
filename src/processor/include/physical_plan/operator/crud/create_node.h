#pragma once

#include "src/processor/include/physical_plan/operator/crud/crud_node.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class CreateNode : public CRUDNode {

public:
    CreateNode(uint64_t dataChunkPos, vector<uint32_t> propertyKeyVectorPos, label_t nodeLabel,
        vector<Column*> nodePropertyColumns, uint64_t numNodes,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : CRUDNode{CREATE_NODE, dataChunkPos, move(propertyKeyVectorPos), nodeLabel,
              move(nodePropertyColumns), numNodes, move(prevOperator), context, id} {};

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateNode>(dataChunkPos, propertyKeyVectorPos, nodeLabel,
            nodePropertyColumns, numNodes, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
