#pragma once

#include "processor/operator/copy/copy.h"
#include "storage/store/nodes_store.h"

namespace kuzu {
namespace processor {

class CopyNode : public Copy {
public:
    CopyNode(catalog::Catalog* catalog, common::CSVReaderConfig csvReaderConfig,
        common::table_id_t tableID, storage::WAL* wal,
        storage::NodesStatisticsAndDeletedIDs* nodesStatistics, storage::RelsStore& relsStore,
        const DataPos& inputPos, const DataPos& outputPos, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : Copy{PhysicalOperatorType::COPY_NODE, catalog, std::move(csvReaderConfig), tableID, wal,
              inputPos, outputPos, std::move(child), id, paramsString},
          nodesStatistics{nodesStatistics}, relsStore{relsStore} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyNode>(catalog, csvReaderConfig, tableID, wal, nodesStatistics,
            relsStore, inputPos, outputPos, children[0]->clone(), id, paramsString);
    }

protected:
    uint64_t copy(common::CopyDescription& copyDescription, uint64_t numThreads) override;

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
