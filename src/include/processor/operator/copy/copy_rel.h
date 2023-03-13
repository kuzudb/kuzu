#pragma once

#include "processor/operator/copy/copy.h"
#include "storage/store/rels_store.h"

namespace kuzu {
namespace processor {

class CopyRel : public Copy {
public:
    CopyRel(catalog::Catalog* catalog, common::CSVReaderConfig csvReaderConfig,
        common::table_id_t tableID, storage::WAL* wal,
        storage::NodesStatisticsAndDeletedIDs* nodesStatistics,
        storage::RelsStatistics* relsStatistics, const DataPos& inputPos, const DataPos& outputPos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Copy{PhysicalOperatorType::COPY_REL, catalog, std::move(csvReaderConfig), tableID, wal,
              inputPos, outputPos, std::move(child), id, paramsString},
          nodesStatistics{nodesStatistics}, relsStatistics{relsStatistics} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyRel>(catalog, csvReaderConfig, tableID, wal, nodesStatistics,
            relsStatistics, inputPos, outputPos, children[0]->clone(), id, paramsString);
    }

protected:
    uint64_t copy(common::CopyDescription& copyDescription, uint64_t numThreads) override;

private:
    inline bool allowCopyCSV() override {
        return relsStatistics->getRelStatistics(tableID)->getNextRelOffset() == 0;
    }

    inline void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override {
        Copy::initLocalStateInternal(resultSet_, context);
        bufferManager = context->bufferManager;
    }

private:
    storage::NodesStatisticsAndDeletedIDs* nodesStatistics;
    storage::RelsStatistics* relsStatistics;
    storage::BufferManager* bufferManager;
};

} // namespace processor
} // namespace kuzu
