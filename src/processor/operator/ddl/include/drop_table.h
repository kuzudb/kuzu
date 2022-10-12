#pragma once

#include "src/catalog/include/catalog.h"
#include "src/processor/operator/include/physical_operator.h"
#include "src/processor/operator/include/source_operator.h"
#include "src/storage/include/storage_manager.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace processor {

class DropTable : public PhysicalOperator, public SourceOperator {

public:
    DropTable(Catalog* catalog, TableSchema* tableSchema, StorageManager& storageManager,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{nullptr}, catalog{catalog},
          tableSchema{tableSchema}, storageManager{storageManager} {}

    PhysicalOperatorType getOperatorType() override { return DROP_TABLE; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override { return nullptr; };

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

    bool getNextTuples() override {
        catalog->removeTableSchema(tableSchema);
        return false;
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropTable>(catalog, tableSchema, storageManager, id, paramsString);
    }

protected:
    Catalog* catalog;
    TableSchema* tableSchema;
    StorageManager& storageManager;
};

} // namespace processor
} // namespace graphflow
