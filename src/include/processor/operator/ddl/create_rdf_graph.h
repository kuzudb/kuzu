#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

class CreateRdfGraph final : public DDL {
public:
    CreateRdfGraph(storage::StorageManager* storageManager, binder::BoundCreateTableInfo info,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::CREATE_RDF_GRAPH, outputPos, id, paramsString},
          storageManager{storageManager},
          nodesStatistics{storageManager->getNodesStatisticsAndDeletedIDs()},
          relsStatistics{storageManager->getRelsStatistics()}, info{std::move(info)} {}

    void executeDDLInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CreateRdfGraph>(
            storageManager, info.copy(), outputPos, id, paramsString);
    }

private:
    storage::StorageManager* storageManager;
    storage::NodesStoreStatsAndDeletedIDs* nodesStatistics;
    storage::RelsStoreStats* relsStatistics;
    binder::BoundCreateTableInfo info;
};

} // namespace processor
} // namespace kuzu
