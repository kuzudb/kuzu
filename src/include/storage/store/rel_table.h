#pragma once

#include "catalog/rel_table_schema.h"
#include "common/column_data_format.h"
#include "storage/stats/rel_table_statistics.h"
#include "storage/storage_utils.h"
#include "storage/store/rel_table_data.h"

namespace kuzu {
namespace storage {

class RelTable {
public:
    RelTable(BMFileHandle* dataFH, BMFileHandle* metadataFH, RelsStoreStats* relsStoreStats,
        BufferManager* bufferManager, catalog::RelTableSchema* schema, WAL* wal,
        bool enableCompression);

    inline common::table_id_t getTableID() const { return tableID; }

    inline void initializeReadState(transaction::Transaction* transaction,
        common::RelDataDirection direction, common::ValueVector* inNodeIDVector,
        RelDataReadState* readState) {
        return direction == common::FWD ?
                   fwdRelTableData->initializeReadState(transaction, inNodeIDVector, readState) :
                   bwdRelTableData->initializeReadState(transaction, inNodeIDVector, readState);
    }
    void read(transaction::Transaction* transaction, RelDataReadState& scanState,
        common::RelDataDirection direction, common::ValueVector* inNodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);

    inline void dropColumn(common::column_id_t columnID) {
        fwdRelTableData->dropColumn(columnID);
        bwdRelTableData->dropColumn(columnID);
    }
    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector);

    inline common::ColumnDataFormat getTableDataFormat(common::RelDataDirection direction) {
        return direction == common::FWD ? fwdRelTableData->getDataFormat() :
                                          bwdRelTableData->getDataFormat();
    }

    inline void append(NodeGroup* nodeGroup, common::RelDataDirection direction) {
        direction == common::FWD ? fwdRelTableData->append(nodeGroup) :
                                   bwdRelTableData->append(nodeGroup);
    }

    void checkpointInMemory();
    void rollbackInMemory();

    inline bool compressionEnabled() const { return fwdRelTableData->compressionEnabled(); }

private:
    void scan(transaction::Transaction* transaction, RelDataReadState& scanState,
        common::RelDataDirection direction, common::ValueVector* inNodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(transaction::Transaction* transaction, common::RelDataDirection direction,
        common::ValueVector* inNodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);

    inline RelTableData* getDirectedTableData(common::RelDataDirection direction) {
        return direction == common::FWD ? fwdRelTableData.get() : bwdRelTableData.get();
    }

private:
    common::table_id_t tableID;
    RelsStoreStats* relsStoreStats;
    std::unique_ptr<RelTableData> fwdRelTableData;
    std::unique_ptr<RelTableData> bwdRelTableData;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
