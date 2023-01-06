#pragma once

#include "processor/operator/copy/copy.h"
#include "storage/store/nodes_store.h"

namespace kuzu {
namespace processor {

class CopyNode : public Copy {
public:
    CopyNode(Catalog* catalog, CopyDescription copyDescription, table_id_t tableID, WAL* wal,
        NodesStatisticsAndDeletedIDs* nodesStatistics, uint32_t id, const string& paramsString)
        : Copy{PhysicalOperatorType::COPY_NODE, catalog, std::move(copyDescription), tableID, wal,
              id, paramsString},
          nodesStatistics{nodesStatistics} {}

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyNode>(
            catalog, copyDescription, tableID, wal, nodesStatistics, id, paramsString);
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
