#include "storage/wal/wal_record.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

bool DBFileID::operator==(const DBFileID& rhs) const {
    if (dbFileType != rhs.dbFileType || isOverflow != rhs.isOverflow) {
        return false;
    }
    switch (dbFileType) {
    case DBFileType::NODE_INDEX: {
        return nodeIndexID == rhs.nodeIndexID;
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::string dbFileTypeToString(DBFileType dbFileType) {
    switch (dbFileType) {
    case DBFileType::DATA: {
        return "DATA";
    }
    case DBFileType::METADATA: {
        return "METADATA";
    }
    case DBFileType::NODE_INDEX: {
        return "NODE_INDEX";
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

DBFileID DBFileID::newDataFileID() {
    DBFileID retVal{DBFileType::DATA, false};
    return retVal;
}

DBFileID DBFileID::newMetadataFileID() {
    DBFileID retVal{DBFileType::METADATA, false};
    return retVal;
}

DBFileID DBFileID::newPKIndexFileID(common::table_id_t tableID) {
    DBFileID retVal{DBFileType::NODE_INDEX};
    retVal.nodeIndexID = NodeIndexID(tableID);
    return retVal;
}

bool WALRecord::operator==(const WALRecord& rhs) const {
    if (recordType != rhs.recordType) {
        return false;
    }
    switch (recordType) {
    case WALRecordType::PAGE_UPDATE_OR_INSERT_RECORD: {
        return pageInsertOrUpdateRecord == rhs.pageInsertOrUpdateRecord;
    }
    case WALRecordType::TABLE_STATISTICS_RECORD: {
        return tableStatisticsRecord == rhs.tableStatisticsRecord;
    }
    case WALRecordType::COMMIT_RECORD: {
        return commitRecord == rhs.commitRecord;
    }
    case WALRecordType::CATALOG_RECORD: {
        // CatalogRecords are empty so are always equal
        return true;
    }
    case WALRecordType::CREATE_TABLE_RECORD: {
        return createTableRecord == rhs.createTableRecord;
    }
    case WALRecordType::CREATE_RDF_GRAPH_RECORD: {
        return rdfGraphRecord == rhs.rdfGraphRecord;
    }
    case WALRecordType::OVERFLOW_FILE_NEXT_BYTE_POS_RECORD: {
        return diskOverflowFileNextBytePosRecord == rhs.diskOverflowFileNextBytePosRecord;
    }
    case WALRecordType::COPY_TABLE_RECORD: {
        return copyTableRecord == rhs.copyTableRecord;
    }
    case WALRecordType::DROP_TABLE_RECORD: {
        return dropTableRecord == rhs.dropTableRecord;
    }
    case WALRecordType::DROP_PROPERTY_RECORD: {
        return dropPropertyRecord == rhs.dropPropertyRecord;
    }
    case WALRecordType::ADD_PROPERTY_RECORD: {
        return addPropertyRecord == rhs.addPropertyRecord;
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::string walRecordTypeToString(WALRecordType walRecordType) {
    switch (walRecordType) {
    case WALRecordType::PAGE_UPDATE_OR_INSERT_RECORD: {
        return "PAGE_UPDATE_OR_INSERT_RECORD";
    }
    case WALRecordType::TABLE_STATISTICS_RECORD: {
        return "TABLE_STATISTICS_RECORD";
    }
    case WALRecordType::COMMIT_RECORD: {
        return "COMMIT_RECORD";
    }
    case WALRecordType::CATALOG_RECORD: {
        return "CATALOG_RECORD";
    }
    case WALRecordType::CREATE_TABLE_RECORD: {
        return "CREATE_TABLE_RECORD";
    }
    case WALRecordType::CREATE_REL_TABLE_GROUP_RECORD: {
        return "REL_TABLE_GROUP_RECORD";
    }
    case WALRecordType::CREATE_RDF_GRAPH_RECORD: {
        return "CREATE_RDF_GRAPH_RECORD";
    }
    case WALRecordType::OVERFLOW_FILE_NEXT_BYTE_POS_RECORD: {
        return "OVERFLOW_FILE_NEXT_BYTE_POS_RECORD";
    }
    case WALRecordType::COPY_TABLE_RECORD: {
        return "COPY_TABLE_RECORD";
    }
    case WALRecordType::DROP_TABLE_RECORD: {
        return "DROP_TABLE_RECORD";
    }
    case WALRecordType::ADD_PROPERTY_RECORD: {
        return "ADD_PROPERTY_RECORD";
    }
    case WALRecordType::DROP_PROPERTY_RECORD: {
        return "DROP_PROPERTY_RECORD";
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

WALRecord WALRecord::newPageInsertOrUpdateRecord(
    DBFileID dbFileID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL, bool isInsert) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::PAGE_UPDATE_OR_INSERT_RECORD;
    retVal.pageInsertOrUpdateRecord =
        PageUpdateOrInsertRecord(dbFileID, pageIdxInOriginalFile, pageIdxInWAL, isInsert);
    return retVal;
}

WALRecord WALRecord::newPageUpdateRecord(
    DBFileID dbFileID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    return WALRecord::newPageInsertOrUpdateRecord(
        dbFileID, pageIdxInOriginalFile, pageIdxInWAL, false /* is update */);
}

WALRecord WALRecord::newPageInsertRecord(
    DBFileID dbFileID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    return WALRecord::newPageInsertOrUpdateRecord(
        dbFileID, pageIdxInOriginalFile, pageIdxInWAL, true /* is insert */);
}

WALRecord WALRecord::newCommitRecord(uint64_t transactionID) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::COMMIT_RECORD;
    retVal.commitRecord = CommitRecord(transactionID);
    return retVal;
}

WALRecord WALRecord::newTableStatisticsRecord(bool isNodeTable) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::TABLE_STATISTICS_RECORD;
    retVal.tableStatisticsRecord = TableStatisticsRecord(isNodeTable);
    return retVal;
}

WALRecord WALRecord::newCatalogRecord() {
    WALRecord retVal;
    retVal.recordType = WALRecordType::CATALOG_RECORD;
    return retVal;
}

WALRecord WALRecord::newCreateTableRecord(table_id_t tableID, TableType tableType) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::CREATE_TABLE_RECORD;
    retVal.createTableRecord = CreateTableRecord(tableID, tableType);
    return retVal;
}

WALRecord WALRecord::newRdfGraphRecord(table_id_t rdfGraphID, table_id_t resourceTableID,
    table_id_t literalTableID, table_id_t resourceTripleTableID, table_id_t literalTripleTableID) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::CREATE_RDF_GRAPH_RECORD;
    retVal.rdfGraphRecord =
        RdfGraphRecord(rdfGraphID, CreateTableRecord(resourceTableID, TableType::NODE),
            CreateTableRecord(literalTableID, TableType::NODE),
            CreateTableRecord(resourceTripleTableID, TableType::REL),
            CreateTableRecord(literalTripleTableID, TableType::REL));
    return retVal;
}

WALRecord WALRecord::newOverflowFileNextBytePosRecord(
    DBFileID dbFileID_, uint64_t prevNextByteToWriteTo_) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::OVERFLOW_FILE_NEXT_BYTE_POS_RECORD;
    retVal.diskOverflowFileNextBytePosRecord =
        DiskOverflowFileNextBytePosRecord(dbFileID_, prevNextByteToWriteTo_);
    return retVal;
}

WALRecord WALRecord::newCopyTableRecord(table_id_t tableID, TableType tableType) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::COPY_TABLE_RECORD;
    retVal.copyTableRecord = CopyTableRecord(tableID, tableType);
    return retVal;
}

WALRecord WALRecord::newDropTableRecord(table_id_t tableID) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::DROP_TABLE_RECORD;
    retVal.dropTableRecord = DropTableRecord(tableID);
    return retVal;
}

WALRecord WALRecord::newDropPropertyRecord(table_id_t tableID, property_id_t propertyID) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::DROP_PROPERTY_RECORD;
    retVal.dropPropertyRecord = DropPropertyRecord(tableID, propertyID);
    return retVal;
}

WALRecord WALRecord::newAddPropertyRecord(table_id_t tableID, property_id_t propertyID) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::ADD_PROPERTY_RECORD;
    retVal.addPropertyRecord = AddPropertyRecord(tableID, propertyID);
    return retVal;
}

void WALRecord::constructWALRecordFromBytes(WALRecord& retVal, uint8_t* bytes, uint64_t& offset) {
    ((WALRecord*)&retVal)[0] = ((WALRecord*)(bytes + offset))[0];
    offset += sizeof(WALRecord);
}

void WALRecord::writeWALRecordToBytes(uint8_t* bytes, uint64_t& offset) {
    ((WALRecord*)(bytes + offset))[0] = *this;
    offset += sizeof(WALRecord);
}

} // namespace storage
} // namespace kuzu
