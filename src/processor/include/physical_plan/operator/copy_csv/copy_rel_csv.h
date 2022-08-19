#pragma once

#include "copy_csv.h"

#include "src/storage/store/include/rels_store.h"

namespace graphflow {
namespace processor {

class CopyRelCSV : public CopyCSV {

public:
    CopyRelCSV(Catalog* catalog, CSVDescription csvDescription, Label label, WAL* wal,
        NodesMetadata* nodesMetadata, uint32_t id, const string& paramsString, RelsStore* relsStore)
        : CopyCSV(catalog, move(csvDescription), move(label), wal, id, paramsString),
          nodesMetadata{nodesMetadata}, relsStore{relsStore} {}

    void execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) override;

    PhysicalOperatorType getOperatorType() override { return COPY_REL_CSV; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyRelCSV>(
            catalog, csvDescription, label, wal, nodesMetadata, id, paramsString, relsStore);
    }

private:
    NodesMetadata* nodesMetadata;
    RelsStore* relsStore;
};

} // namespace processor
} // namespace graphflow
