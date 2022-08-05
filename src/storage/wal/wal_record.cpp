#include "src/storage/wal/include/wal_record.h"

namespace graphflow {
namespace storage {

StorageStructureID StorageStructureID::newStructuredNodePropertyColumnID(
    label_t nodeLabel, uint32_t propertyID, bool isOverflow) {
    StorageStructureID retVal;
    retVal.storageStructureType = STRUCTURED_NODE_PROPERTY_COLUMN;
    retVal.isOverflow = isOverflow;
    retVal.structuredNodePropertyColumnID = StructuredNodePropertyColumnID(nodeLabel, propertyID);
    return retVal;
}

StorageStructureID StorageStructureID::newNodeIndexID(label_t nodeLabel) {
    StorageStructureID retVal;
    retVal.storageStructureType = NODE_INDEX;
    retVal.nodeIndexID = NodeIndexID(nodeLabel);
    return retVal;
}

StorageStructureID StorageStructureID::newUnstructuredNodePropertyListsID(
    label_t nodeLabel, ListFileType listFileType) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.storageStructureType = LISTS;
    retVal.listFileID = ListFileID(listFileType, UnstructuredNodePropertyListsID(nodeLabel));
    return retVal;
}

StorageStructureID StorageStructureID::newAdjListsID(
    label_t relLabel, label_t srcNodeLabel, RelDirection dir, ListFileType listFileType) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.storageStructureType = LISTS;
    retVal.listFileID =
        ListFileID(listFileType, AdjListsID(RelNodeLabelAndDir(relLabel, srcNodeLabel, dir)));
    return retVal;
}

StorageStructureID StorageStructureID::newRelPropertyListsID(label_t relLabel, label_t srcNodeLabel,
    RelDirection dir, uint32_t propertyID, ListFileType listFileType) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.storageStructureType = LISTS;
    retVal.listFileID = ListFileID(listFileType,
        RelPropertyListID(RelNodeLabelAndDir(relLabel, srcNodeLabel, dir), propertyID));
    return retVal;
}

WALRecord WALRecord::newPageInsertOrUpdateRecord(StorageStructureID storageStructureID_,
    uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL, bool isInsert) {
    WALRecord retVal;
    retVal.recordType = PAGE_UPDATE_OR_INSERT_RECORD;
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
    retVal.recordType = COMMIT_RECORD;
    retVal.commitRecord = CommitRecord(transactionID);
    return retVal;
}

WALRecord WALRecord::newNodeMetadataRecord() {
    WALRecord retVal;
    retVal.recordType = NODES_METADATA_RECORD;
    return retVal;
}

WALRecord WALRecord::newCatalogRecord() {
    WALRecord retVal;
    retVal.recordType = CATALOG_RECORD;
    return retVal;
}

WALRecord WALRecord::newNodeTableRecord(label_t labelID) {
    WALRecord retVal;
    retVal.recordType = NODE_TABLE_RECORD;
    retVal.nodeTableRecord = NodeTableRecord(labelID);
    return retVal;
}

WALRecord WALRecord::newRelTableRecord(label_t labelID) {
    WALRecord retVal;
    retVal.recordType = REL_TABLE_RECORD;
    retVal.relTableRecord = RelTableRecord(labelID);
    return retVal;
}

WALRecord WALRecord::newOverflowFileNextBytePosRecord(
    StorageStructureID storageStructureID_, uint64_t prevNextByteToWriteTo_) {
    WALRecord retVal;
    retVal.recordType = OVERFLOW_FILE_NEXT_BYTE_POS_RECORD;
    retVal.overflowFileNextBytePosRecord =
        OverflowFileNextBytePosRecord(storageStructureID_, prevNextByteToWriteTo_);
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
} // namespace graphflow
