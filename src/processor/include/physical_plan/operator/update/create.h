#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/storage/store/include/node_table.h"

namespace graphflow {
namespace processor {

class CreateNode : public PhysicalOperator {

public:
    CreateNode(vector<NodeTable*> nodeTables, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, nodeTables{move(nodeTables)} {}

    inline PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::CREATE; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateNode>(nodeTables, children[0]->clone(), id, paramsString);
    }

private:
    vector<NodeTable*> nodeTables;
};

} // namespace processor
} // namespace graphflow
