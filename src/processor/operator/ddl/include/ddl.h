#pragma once

#include "src/catalog/include/catalog.h"
#include "src/processor/operator/include/physical_operator.h"
#include "src/processor/operator/include/source_operator.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::catalog;

namespace graphflow {
namespace processor {

class DDL : public PhysicalOperator, public SourceOperator {

public:
    DDL(Catalog* catalog, string tableName, vector<PropertyNameDataType> propertyNameDataTypes,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{nullptr}, catalog{catalog},
          tableName{move(tableName)}, propertyNameDataTypes{move(propertyNameDataTypes)} {}

    shared_ptr<ResultSet> init(ExecutionContext* context) override { return nullptr; };

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

protected:
    Catalog* catalog;
    string tableName;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace processor
} // namespace graphflow
