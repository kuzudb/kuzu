#pragma once

#include "processor/operator/copy/copy.h"
#include "storage/store/nodes_store.h"

namespace kuzu {
namespace processor {

class CopyNPY : public Copy {
public:
    CopyNPY(catalog::Catalog* catalog, const common::CopyDescription& copyDescription,
        storage::NodeTable* table, storage::WAL* wal,
        storage::NodesStatisticsAndDeletedIDs* nodesStatistics, storage::RelsStore& relsStore,
        uint32_t id, const std::string& paramsString)
        : Copy{PhysicalOperatorType::COPY_NPY, catalog, copyDescription, table->getTableID(), wal,
              id, paramsString},
          table{table}, nodesStatistics{nodesStatistics}, relsStore{relsStore} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyNPY>(
            catalog, copyDescription, table, wal, nodesStatistics, relsStore, id, paramsString);
    }

protected:
    uint64_t executeInternal(
        common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) override;

private:
    inline bool isCopyAllowed() override {
        return nodesStatistics->getNodeStatisticsAndDeletedIDs(tableID)->getNumTuples() == 0;
    }

private:
    storage::NodeTable* table;
    storage::NodesStatisticsAndDeletedIDs* nodesStatistics;
    storage::RelsStore& relsStore;
};

} // namespace processor
} // namespace kuzu
