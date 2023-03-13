#pragma once

#include "common/copier_config/copier_config.h"
#include "common/task_system/task_scheduler.h"
#include "processor/operator/physical_operator.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace processor {

class Copy : public PhysicalOperator {
public:
    Copy(PhysicalOperatorType operatorType, catalog::Catalog* catalog,
        common::CSVReaderConfig csvReaderConfig, common::table_id_t tableID, storage::WAL* wal,
        const DataPos& inputPos, const DataPos& outputPos, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, std::move(child), id, paramsString}, catalog{catalog},
          csvReaderConfig{std::move(csvReaderConfig)}, tableID{tableID}, wal{wal},
          inputPos{inputPos}, outputPos{outputPos}, hasExecuted{false} {}

    inline bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

protected:
    virtual uint64_t copy(common::CopyDescription& copyDescription, uint64_t numThreads) = 0;

    virtual bool allowCopyCSV() = 0;

    inline std::string getOutputMsg(uint64_t numTuplesCopied) {
        return common::StringUtils::string_format(
            "{} number of tuples has been copied to table: {}.", numTuplesCopied,
            catalog->getReadOnlyVersion()->getTableName(tableID).c_str());
    }

private:
    static inline void globPath(std::vector<std::string>& filePaths, const std::string& rawPath) {
        auto expandedPaths = storage::StorageUtils::globFilePath(rawPath);
        filePaths.insert(filePaths.end(), expandedPaths.begin(), expandedPaths.end());
    }

    common::CopyDescription getCopyDescription() const;

protected:
    catalog::Catalog* catalog;
    common::CSVReaderConfig csvReaderConfig;
    common::table_id_t tableID;
    storage::WAL* wal;
    DataPos inputPos;
    DataPos outputPos;
    common::ValueVector* inputVector;
    common::ValueVector* outputVector;
    bool hasExecuted;
    uint64_t numThreads;
};

} // namespace processor
} // namespace kuzu
