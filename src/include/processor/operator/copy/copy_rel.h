#pragma once

#include "processor/operator/copy/copy.h"
#include "storage/store/rels_store.h"

namespace kuzu {
namespace processor {

class CopyRel : public Copy {
public:
    CopyRel(Catalog* catalog, CopyDescription copyDescription, table_id_t tableID, WAL* wal,
        NodesStatisticsAndDeletedIDs* nodesStatistics, RelsStatistics* relsStatistics, uint32_t id,
        const string& paramsString)
        : Copy{PhysicalOperatorType::COPY_REL, catalog, std::move(copyDescription), tableID, wal,
              id, paramsString},
          nodesStatistics{nodesStatistics}, relsStatistics{relsStatistics} {}

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyRel>(catalog, copyDescription, tableID, wal, nodesStatistics,
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
