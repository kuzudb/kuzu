#pragma once

#include "processor/operator/copy_csv/copy_csv.h"
#include "storage/store/nodes_store.h"

namespace kuzu {
namespace processor {

class CopyNodeCSV : public CopyCSV {

public:
    CopyNodeCSV(Catalog* catalog, CSVDescription csvDescription, TableSchema tableSchema, WAL* wal,
        uint32_t id, const string& paramsString, NodesStore* nodesStore)
        : CopyCSV(catalog, move(csvDescription), move(tableSchema), wal, id, paramsString),
          nodesStore{nodesStore} {}

    string execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext) override;

    PhysicalOperatorType getOperatorType() override { return COPY_NODE_CSV; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyNodeCSV>(
            catalog, csvDescription, tableSchema, wal, id, paramsString, nodesStore);
    }

private:
    NodesStore* nodesStore;
};

} // namespace processor
} // namespace kuzu
