#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"
#include "src/storage/store/include/nodes_metadata.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace processor {

class CopyNodeCSV : public PhysicalOperator, public SourceOperator {

public:
    CopyNodeCSV(Catalog* catalog, const CSVDescription csvDescription, const string outputDirectory,
        NodesMetadata* nodesMetadata, BufferManager* bufferManager, WAL* wal, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{nullptr}, catalog{catalog},
          csvDescription{move(csvDescription)}, outputDirectory{move(outputDirectory)},
          nodesMetadata{nodesMetadata}, bufferManager{bufferManager}, wal{wal} {}

    void execute(TaskScheduler& taskScheduler);

    shared_ptr<ResultSet> init(ExecutionContext* context) override { return nullptr; };

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

    PhysicalOperatorType getOperatorType() override { return COPY_NODE_CSV; }

    bool getNextTuples() override { return false; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyNodeCSV>(catalog, csvDescription, outputDirectory, nodesMetadata,
            bufferManager, wal, id, paramsString);
    }

private:
    Catalog* catalog;
    const CSVDescription csvDescription;
    const string outputDirectory;
    NodesMetadata* nodesMetadata;
    BufferManager* bufferManager;
    WAL* wal;
};

} // namespace processor
} // namespace graphflow
