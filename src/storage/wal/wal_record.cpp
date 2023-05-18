#include "storage/wal/wal_record.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::string storageStructureTypeToString(StorageStructureType storageStructureType) {
    switch (storageStructureType) {
    case StorageStructureType::COLUMN: {
        return "COLUMN";
    } break;
    case StorageStructureType::LISTS: {
        return "LISTS";
    } break;
    case StorageStructureType::NODE_INDEX: {
        return "NODE_INDEX";
    } break;
    default: {
        assert(false);
    }
    }
}

StorageStructureID StorageStructureID::newNodePropertyColumnID(
    table_id_t tableID, property_id_t propertyID) {
    StorageStructureID retVal;
    retVal.storageStructureType = StorageStructureType::COLUMN;
    retVal.isOverflow = false;
    retVal.isNullBits = false;
    retVal.columnFileID = ColumnFileID(NodePropertyColumnID(tableID, propertyID));
    return retVal;
}

StorageStructureID StorageStructureID::newNodeIndexID(table_id_t tableID) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.isNullBits = false;
    retVal.storageStructureType = StorageStructureType::NODE_INDEX;
    retVal.nodeIndexID = NodeIndexID(tableID);
    return retVal;
}

StorageStructureID StorageStructureID::newAdjListsID(
    table_id_t relTableID, RelDataDirection dir, ListFileType listFileType) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.isNullBits = false;
    retVal.storageStructureType = StorageStructureType::LISTS;
    retVal.listFileID = ListFileID(listFileType, AdjListsID(RelNodeTableAndDir(relTableID, dir)));
    return retVal;
}

StorageStructureID StorageStructureID::newRelPropertyListsID(table_id_t relTableID,
    RelDataDirection dir, property_id_t propertyID, ListFileType listFileType) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.isNullBits = false;
    retVal.storageStructureType = StorageStructureType::LISTS;
    retVal.listFileID = ListFileID(
        listFileType, RelPropertyListsID(RelNodeTableAndDir(relTableID, dir), propertyID));
    return retVal;
}

StorageStructureID StorageStructureID::newRelPropertyColumnID(
    table_id_t relTableID, RelDataDirection dir, property_id_t propertyID) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.isNullBits = false;
    retVal.storageStructureType = StorageStructureType::COLUMN;
    retVal.columnFileID =
        ColumnFileID(RelPropertyColumnID(RelNodeTableAndDir(relTableID, dir), propertyID));
    return retVal;
}

StorageStructureID StorageStructureID::newAdjColumnID(table_id_t relTableID, RelDataDirection dir) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.isNullBits = false;
    retVal.storageStructureType = StorageStructureType::COLUMN;
    retVal.columnFileID = ColumnFileID(AdjColumnID(RelNodeTableAndDir(relTableID, dir)));
    return retVal;
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
    case WALRecordType::NODE_TABLE_RECORD: {
        return "NODE_TABLE_RECORD";
    }
    case WALRecordType::REL_TABLE_RECORD: {
        return "REL_TABLE_RECORD";
    }
    case WALRecordType::OVERFLOW_FILE_NEXT_BYTE_POS_RECORD: {
        return "OVERFLOW_FILE_NEXT_BYTE_POS_RECORD";
    }
    case WALRecordType::COPY_NODE_RECORD: {
        return "COPY_NODE_RECORD";
    }
    case WALRecordType::COPY_REL_RECORD: {
        return "COPY_REL_RECORD";
    }
    case WALRecordType::DROP_TABLE_RECORD: {
        return "DROP_TABLE_RECORD";
    }
    case WALRecordType::DROP_PROPERTY_RECORD: {
        return "DROP_PROPERTY_RECORD";
    }
    default: {
        assert(false);
    }
    }
}

WALRecord WALRecord::newPageInsertOrUpdateRecord(StorageStructureID storageStructureID_,
    uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL, bool isInsert) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::PAGE_UPDATE_OR_INSERT_RECORD;
    retVal.pageInsertOrUpdateRecord = PageUpdateOrInsertRecord(
        storageStructureID_, pageIdxInOriginalFile, pageIdxInWAL, isInsert);
    return retVal;
}

WALRecord WALRecord::newPageUpdateRecord(
    StorageStructureID storageStructureID_, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    return WALRecord::newPageInsertOrUpdateRecord(
        storageStructureID_, pageIdxInOriginalFile, pageIdxInWAL, false /* is update */);
}

WALRecord WALRecord::newPageInsertRecord(
    StorageStructureID storageStructureID_, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    return WALRecord::newPageInsertOrUpdateRecord(
        storageStructureID_, pageIdxInOriginalFile, pageIdxInWAL, true /* is insert */);
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

WALRecord WALRecord::newNodeTableRecord(table_id_t tableID) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::NODE_TABLE_RECORD;
    retVal.nodeTableRecord = NodeTableRecord(tableID);
    return retVal;
}

WALRecord WALRecord::newRelTableRecord(table_id_t tableID) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::REL_TABLE_RECORD;
    retVal.relTableRecord = RelTableRecord(tableID);
    return retVal;
}

WALRecord WALRecord::newOverflowFileNextBytePosRecord(
    StorageStructureID storageStructureID_, uint64_t prevNextByteToWriteTo_) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::OVERFLOW_FILE_NEXT_BYTE_POS_RECORD;
    retVal.diskOverflowFileNextBytePosRecord =
        DiskOverflowFileNextBytePosRecord(storageStructureID_, prevNextByteToWriteTo_);
    return retVal;
}

WALRecord WALRecord::newCopyNodeRecord(table_id_t tableID) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::COPY_NODE_RECORD;
    retVal.copyNodeRecord = CopyNodeRecord(tableID);
    return retVal;
}

WALRecord WALRecord::newCopyRelRecord(table_id_t tableID) {
    WALRecord retVal;
    retVal.recordType = WALRecordType::COPY_REL_RECORD;
    retVal.copyRelRecord = CopyRelRecord(tableID);
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
