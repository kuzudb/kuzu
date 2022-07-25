#pragma once

#include "src/catalog/include/catalog.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::catalog;

namespace graphflow {
namespace processor {

class CreateNodeTable : public PhysicalOperator, public SourceOperator {

public:
    CreateNodeTable(Catalog* catalog, string labelName,
        vector<PropertyNameDataType> propertyNameDataTypes, string primaryKey, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{id, paramsString},
          SourceOperator{nullptr}, catalog{catalog}, labelName{move(labelName)},
          propertyNameDataTypes{move(propertyNameDataTypes)}, primaryKey{move(primaryKey)} {}

    PhysicalOperatorType getOperatorType() override { return CREATE_NODE_TABLE; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override { return nullptr; };

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateNodeTable>(
            catalog, labelName, propertyNameDataTypes, primaryKey, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

private:
    Catalog* catalog;
    string labelName;
    string primaryKey;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace processor
} // namespace graphflow
