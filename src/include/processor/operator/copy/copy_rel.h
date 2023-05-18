#pragma once

#include "processor/operator/copy/copy.h"
#include "storage/store/nodes_store.h"
#include "storage/store/rels_store.h"

namespace kuzu {
namespace processor {

class CopyRel : public Copy {
public:
    CopyRel(catalog::Catalog* catalog, const common::CopyDescription& copyDescription,
        storage::RelTable* table, storage::WAL* wal, storage::RelsStatistics* relsStatistics,
        storage::NodesStore& nodesStore, uint32_t id, const std::string& paramsString)
        : Copy{PhysicalOperatorType::COPY_REL, catalog, copyDescription, table->getRelTableID(),
              wal, id, paramsString},
          table{table}, relsStatistics{relsStatistics}, nodesStore{nodesStore} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyRel>(
            catalog, copyDescription, table, wal, relsStatistics, nodesStore, id, paramsString);
    }

protected:
    uint64_t executeInternal(
        common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) override;

private:
    inline bool isCopyAllowed() override {
        return relsStatistics->getRelStatistics(tableID)->getNextRelOffset() == 0;
    }

private:
    storage::RelTable* table;
    storage::RelsStatistics* relsStatistics;
    storage::NodesStore& nodesStore;
};

} // namespace processor
} // namespace kuzu
