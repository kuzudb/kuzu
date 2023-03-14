#pragma once

#include "processor/operator/copy/copy.h"
#include "storage/store/nodes_store.h"

namespace kuzu {
namespace processor {

class CopyNode : public Copy {
public:
    CopyNode(catalog::Catalog* catalog, common::CopyDescription copyDescription,
        common::table_id_t tableID, storage::WAL* wal,
        storage::NodesStatisticsAndDeletedIDs* nodesStatistics, storage::RelsStore& relsStore,
        uint32_t id, const std::string& paramsString)
        : Copy{PhysicalOperatorType::COPY_NODE, catalog, std::move(copyDescription), tableID, wal,
              id, paramsString},
          nodesStatistics{nodesStatistics}, relsStore{relsStore} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyNode>(
            catalog, copyDescription, tableID, wal, nodesStatistics, relsStore, id, paramsString);
    }

protected:
    uint64_t executeInternal(
        common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) override;

private:
    inline bool allowCopyCSV() override {
        return nodesStatistics->getNodeStatisticsAndDeletedIDs(tableID)->getNumTuples() == 0;
    }

private:
    storage::NodesStatisticsAndDeletedIDs* nodesStatistics;
    storage::RelsStore& relsStore;
};

} // namespace processor
} // namespace kuzu
