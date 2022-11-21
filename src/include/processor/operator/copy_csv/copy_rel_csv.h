#pragma once

#include "processor/operator/copy_csv/copy_csv.h"
#include "storage/store/rels_store.h"

namespace kuzu {
namespace processor {

class CopyRelCSV : public CopyCSV {

public:
    CopyRelCSV(Catalog* catalog, CSVDescription csvDescription, TableSchema tableSchema, WAL* wal,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs, uint32_t id,
        const string& paramsString, RelsStatistics* relsStatistics)
        : CopyCSV(catalog, move(csvDescription), move(tableSchema), wal, id, paramsString),
          nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs}, relsStatistics{
                                                                          relsStatistics} {}

    string execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext) override;

    PhysicalOperatorType getOperatorType() override { return COPY_REL_CSV; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyRelCSV>(catalog, csvDescription, tableSchema, wal,
            nodesStatisticsAndDeletedIDs, id, paramsString, relsStatistics);
    }

private:
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    RelsStatistics* relsStatistics;
};

} // namespace processor
} // namespace kuzu
