#pragma once

#include "processor/operator/copy_csv/copy_csv.h"
#include "storage/store/nodes_store.h"

namespace kuzu {
namespace processor {

class CopyNodeCSV : public CopyCSV {
public:
    CopyNodeCSV(Catalog* catalog, CSVDescription csvDescription, table_id_t tableID, WAL* wal,
        NodesStatisticsAndDeletedIDs* nodesStatistics, uint32_t id, const string& paramsString)
        : CopyCSV{PhysicalOperatorType::COPY_NODE_CSV, catalog, std::move(csvDescription), tableID,
              wal, id, paramsString},
          nodesStatistics{nodesStatistics} {}

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyNodeCSV>(
            catalog, csvDescription, tableID, wal, nodesStatistics, id, paramsString);
    }

protected:
    uint64_t executeInternal(
        common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) override;

    uint64_t getNumTuplesInTable() override;

private:
    NodesStatisticsAndDeletedIDs* nodesStatistics;
};

} // namespace processor
} // namespace kuzu
