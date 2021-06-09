#pragma once

#include "src/processor/include/physical_plan/operator/crud/crud.h"
#include "src/storage/include/data_structure/column.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class CRUDNode : public CRUDOperator {

public:
    CRUDNode(PhysicalOperatorType type, uint64_t dataChunkPos,
        vector<uint32_t> propertyKeyVectorPos, label_t nodeLabel,
        vector<BaseColumn*> nodePropertyColumns, uint64_t numNodes,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : CRUDOperator{dataChunkPos, type, move(prevOperator), context, id},
          propertyKeyVectorPos{move(propertyKeyVectorPos)}, nodeLabel{nodeLabel},
          nodePropertyColumns{move(nodePropertyColumns)}, numNodes{numNodes} {};

protected:
    vector<uint32_t> propertyKeyVectorPos;

    // current state of the graph that will be updated.
    label_t nodeLabel;
    vector<BaseColumn*> nodePropertyColumns;
    uint64_t numNodes;
};

} // namespace processor
} // namespace graphflow
