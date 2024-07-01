#pragma once

#include "storage/store/chunked_node_group.h"
#include "storage/store/column.h"

namespace kuzu {
namespace catalog {
class TableCatalogEntry;
class Property;
} // namespace catalog
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator

namespace storage {
struct TableScanState;

class DiskArrayCollection;
template<typename T>
class DiskArray;

struct TableDataScanState {
    std::vector<common::column_id_t> columnIDs;
    std::vector<Column::ChunkState> chunkStates;

    explicit TableDataScanState(const std::vector<common::column_id_t>& columnIDs)
        : columnIDs{columnIDs} {
        chunkStates.resize(this->columnIDs.size());
    };
    virtual ~TableDataScanState() = default;
    DELETE_COPY_DEFAULT_MOVE(TableDataScanState);

    // TODO(Guodong): If we reset the state, we should be able to use the same object to scan
    // different table. This should simplify the multi-label scan.
    virtual void resetState() {
        for (auto& state : chunkStates) {
            state.resetState();
        }
    }

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TableDataScanState&, TARGET&>(*this);
    }
};

class LocalTableData;
class TableData {
public:
    virtual ~TableData() = default;

    virtual void initializeScanState(transaction::Transaction* transaction,
        TableScanState& scanState) const = 0;
    virtual void scan(transaction::Transaction* transaction, TableDataScanState& readState,
        common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) = 0;
    virtual void lookup(transaction::Transaction* transaction, TableDataScanState& readState,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) = 0;

    virtual void append(transaction::Transaction* transaction, ChunkedNodeGroup* nodeGroup) = 0;

    void dropColumn(common::column_id_t columnID) { columns.erase(columns.begin() + columnID); }
    void addColumn(transaction::Transaction* transaction, const std::string& colNamePrefix,
        DiskArray<ColumnChunkMetadata>* metadataDA, const MetadataDAHInfo& metadataDahInfo,
        const catalog::Property& property, evaluator::ExpressionEvaluator& defaultEvaluator);

    common::idx_t getNumColumns() const { return columns.size(); }
    Column* getColumn(common::column_id_t columnID) const {
        KU_ASSERT(columnID < columns.size() && columnID != common::INVALID_COLUMN_ID);
        return columns[columnID].get();
    }
    const std::vector<std::unique_ptr<Column>>& getColumns() const { return columns; }

    virtual void prepareLocalTableToCommit(transaction::Transaction* transaction,
        LocalTableData* localTable) = 0;
    virtual void checkpointInMemory();
    virtual void rollbackInMemory();
    virtual void prepareCommit();

    virtual common::node_group_idx_t getNumCommittedNodeGroups() const = 0;

protected:
    TableData(BMFileHandle* dataFH, DiskArrayCollection* metadataDAC,
        catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
        bool enableCompression);

protected:
    BMFileHandle* dataFH;
    DiskArrayCollection* metadataDAC;
    common::table_id_t tableID;
    std::string tableName;
    BufferManager* bufferManager;
    WAL* wal;
    bool enableCompression;
    std::vector<std::unique_ptr<Column>> columns;
};

} // namespace storage
} // namespace kuzu
