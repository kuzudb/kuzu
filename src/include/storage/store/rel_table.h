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
        BufferManager* bufferManager, catalog::RelTableSchema* schema, WAL* wal,
        bool enableCompression);

    inline void initializeReadState(transaction::Transaction* transaction,
        common::RelDataDirection direction, std::vector<common::column_id_t> columnIDs,
        common::ValueVector* inNodeIDVector, RelDataReadState* readState) {
        return direction == common::RelDataDirection::FWD ?
                   fwdRelTableData->initializeReadState(
                       transaction, direction, columnIDs, inNodeIDVector, readState) :
                   bwdRelTableData->initializeReadState(
                       transaction, direction, columnIDs, inNodeIDVector, readState);
    }
    void read(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) final;

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

    void prepareCommit() final;
    void prepareRollback() final;
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
