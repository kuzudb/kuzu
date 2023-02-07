#pragma once

#include "processor/operator/copy/copy.h"
#include "storage/store/rels_store.h"

namespace kuzu {
namespace processor {

class CopyRel : public Copy {
public:
    CopyRel(catalog::Catalog* catalog, common::CopyDescription copyDescription,
        common::table_id_t tableID, storage::WAL* wal,
        storage::NodesStatisticsAndDeletedIDs* nodesStatistics,
        storage::RelsStatistics* relsStatistics, uint32_t id, const std::string& paramsString)
        : Copy{PhysicalOperatorType::COPY_REL, catalog, std::move(copyDescription), tableID, wal,
              id, paramsString},
          nodesStatistics{nodesStatistics}, relsStatistics{relsStatistics} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyRel>(catalog, copyDescription, tableID, wal, nodesStatistics,
            relsStatistics, id, paramsString);
    }

protected:
    uint64_t executeInternal(
        common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) override;

    uint64_t getNumTuplesInTable() override;

private:
    storage::NodesStatisticsAndDeletedIDs* nodesStatistics;
    storage::RelsStatistics* relsStatistics;
};

} // namespace processor
} // namespace kuzu
