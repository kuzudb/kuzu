#pragma once

#include "catalog/rel_table_schema.h"
#include "common/column_data_format.h"
#include "storage/stats/rel_table_statistics.h"
#include "storage/store/rel_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace storage {

class RelTable : public Table {
public:
    RelTable(BMFileHandle* dataFH, BMFileHandle* metadataFH, RelsStoreStats* relsStoreStats,
        MemoryManager* memoryManager, catalog::RelTableSchema* schema, WAL* wal,
        bool enableCompression);

    inline void initializeReadState(transaction::Transaction* transaction,
        common::RelDataDirection direction, std::vector<common::column_id_t> columnIDs,
        common::ValueVector* inNodeIDVector, RelDataReadState* readState) {
        return direction == common::RelDataDirection::FWD ?
                   fwdRelTableData->initializeReadState(
                       transaction, columnIDs, inNodeIDVector, readState) :
                   bwdRelTableData->initializeReadState(
                       transaction, columnIDs, inNodeIDVector, readState);
    }
    void read(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) final;

    void insert(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector, common::ValueVector* propertyVector);
    void delete_(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* relIDVector);

    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) final;
    inline void dropColumn(common::column_id_t columnID) final {
        fwdRelTableData->dropColumn(columnID);
        bwdRelTableData->dropColumn(columnID);
    }

    inline common::ColumnDataFormat getTableDataFormat(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getDataFormat() :
                                                            bwdRelTableData->getDataFormat();
    }
    inline void append(NodeGroup* nodeGroup, common::RelDataDirection direction) {
        direction == common::RelDataDirection::FWD ? fwdRelTableData->append(nodeGroup) :
                                                     bwdRelTableData->append(nodeGroup);
    }

    void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) final;
    void prepareRollback(LocalTableData* localTable) final;
    void checkpointInMemory() final;
    void rollbackInMemory() final;

private:
    void scan(transaction::Transaction* transaction, RelDataReadState& scanState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(transaction::Transaction* transaction, RelDataReadState& scanState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

    inline RelTableData* getDirectedTableData(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData.get() :
                                                            bwdRelTableData.get();
    }

private:
    std::unique_ptr<RelTableData> fwdRelTableData;
    std::unique_ptr<RelTableData> bwdRelTableData;
};

} // namespace storage
} // namespace kuzu
