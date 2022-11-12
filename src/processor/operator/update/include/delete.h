#pragma once

#include "src/processor/operator/include/physical_operator.h"
#include "src/storage/store/include/node_table.h"

namespace kuzu {
namespace processor {

class DeleteNodeStructuredProperty : public PhysicalOperator {
public:
    DeleteNodeStructuredProperty(vector<DataPos> nodeIDVectorPositions,
        vector<DataPos> primaryKeyVectorPositions, vector<NodeTable*> nodeTables,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, nodeIDVectorPositions{move(
                                                               nodeIDVectorPositions)},
          primaryKeyVectorPositions{move(primaryKeyVectorPositions)}, nodeTables{move(nodeTables)} {
    }

    PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::DELETE; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DeleteNodeStructuredProperty>(nodeIDVectorPositions,
            primaryKeyVectorPositions, nodeTables, children[0]->clone(), id, paramsString);
    }

private:
    vector<DataPos> nodeIDVectorPositions;
    vector<DataPos> primaryKeyVectorPositions;
    vector<NodeTable*> nodeTables;

    vector<ValueVector*> nodeIDVectors;
    vector<ValueVector*> primaryKeyVectors;
};

} // namespace processor
} // namespace kuzu
