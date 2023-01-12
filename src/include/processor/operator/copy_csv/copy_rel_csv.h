#pragma once

#include "processor/operator/copy_csv/copy_csv.h"
#include "storage/store/rels_store.h"

namespace kuzu {
namespace processor {

class CopyRelCSV : public CopyCSV {
public:
    CopyRelCSV(Catalog* catalog, CSVDescription csvDescription, table_id_t tableID, WAL* wal,
        NodesStatisticsAndDeletedIDs* nodesStatistics, RelsStatistics* relsStatistics, uint32_t id,
        const string& paramsString)
        : CopyCSV{PhysicalOperatorType::COPY_REL_CSV, catalog, std::move(csvDescription), tableID,
              wal, id, paramsString},
          nodesStatistics{nodesStatistics}, relsStatistics{relsStatistics} {}

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyRelCSV>(catalog, csvDescription, tableID, wal, nodesStatistics,
            relsStatistics, id, paramsString);
    }

protected:
    uint64_t executeInternal(
        TaskScheduler* taskScheduler, ExecutionContext* executionContext) override;

    uint64_t getNumTuplesInTable() override;

private:
    NodesStatisticsAndDeletedIDs* nodesStatistics;
    RelsStatistics* relsStatistics;
};

} // namespace processor
} // namespace kuzu
