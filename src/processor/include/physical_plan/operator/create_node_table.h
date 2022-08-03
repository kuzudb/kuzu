#pragma once

#include "src/catalog/include/catalog.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/storage/store/include/nodes_metadata.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::catalog;

namespace graphflow {
namespace processor {

class CreateNodeTable : public PhysicalOperator, public SourceOperator {

public:
    CreateNodeTable(Catalog* catalog, string labelName,
        vector<PropertyNameDataType> propertyNameDataTypes, uint32_t primaryKeyIdx, uint32_t id,
        const string& paramsString, NodesMetadata* nodesMetadata)
        : PhysicalOperator{id, paramsString}, SourceOperator{nullptr}, catalog{catalog},
          labelName{move(labelName)}, propertyNameDataTypes{move(propertyNameDataTypes)},
          primaryKeyIdx{move(primaryKeyIdx)}, nodesMetadata{nodesMetadata} {}

    PhysicalOperatorType getOperatorType() override { return CREATE_NODE_TABLE; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override { return nullptr; };

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateNodeTable>(catalog, labelName, propertyNameDataTypes,
            primaryKeyIdx, id, paramsString, nodesMetadata);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

private:
    Catalog* catalog;
    string labelName;
    vector<PropertyNameDataType> propertyNameDataTypes;
    uint32_t primaryKeyIdx;
    NodesMetadata* nodesMetadata;
};

} // namespace processor
} // namespace graphflow
