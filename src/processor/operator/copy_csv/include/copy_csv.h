#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/processor/operator/include/physical_operator.h"
#include "src/processor/operator/include/source_operator.h"
#include "src/storage/store/include/nodes_statistics_and_deleted_ids.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace processor {

class CopyCSV : public PhysicalOperator {

public:
    CopyCSV(Catalog* catalog, CSVDescription csvDescription, TableSchema tableSchema, WAL* wal,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, catalog{catalog},
          csvDescription{move(csvDescription)}, tableSchema{move(tableSchema)}, wal{wal} {}

    virtual void execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) = 0;

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

    bool getNextTuples() override { return false; }

protected:
    Catalog* catalog;
    CSVDescription csvDescription;
    TableSchema tableSchema;
    WAL* wal;
};

} // namespace processor
} // namespace graphflow
