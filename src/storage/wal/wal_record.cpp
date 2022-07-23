#include "src/storage/wal/include/wal_record.h"

namespace graphflow {
namespace storage {

std::size_t ListFileIDHasher::operator()(const ListFileID& key) const {
    size_t listTypeHash;
    switch (key.listType) {
    case UNSTRUCTURED_NODE_PROPERTY_LISTS: {
        listTypeHash += key.unstructuredNodePropertyListsID.nodeLabel << 3;
    } break;
    case ADJ_LISTS: {
        listTypeHash += (key.adjListsID.relLabelAndDir.relLabel << 5) +
                        (key.adjListsID.relLabelAndDir.srcNodeLabel << 4) +
                        (key.adjListsID.relLabelAndDir.dir << 3);

    } break;
    case REL_PROPERTY_LISTS: {
        listTypeHash += (key.relPropertyListID.relLabelAndDir.relLabel << 6) +
                        (key.relPropertyListID.relLabelAndDir.srcNodeLabel << 5) +
                        (key.relPropertyListID.relLabelAndDir.dir << 4) +
                        (key.relPropertyListID.propertyID << 3);

    } break;
    default:
        throw RuntimeException("Unrecognized ListType: " + to_string(key.listType) +
                               " in ListFileIDHasher. This should never happen.");
    }
    return listTypeHash + (hash<uint8_t>()(key.listType) << 2) + (hash<uint8_t>()(key.listFileType))
           << 1;
}

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
    retVal.storageStructureType = LISTS;
    retVal.listFileID = ListFileID(listFileType, UnstructuredNodePropertyListsID(nodeLabel));
    return retVal;
}

StorageStructureID StorageStructureID::newAdjListsID(
    label_t relLabel, label_t srcNodeLabel, RelDirection dir, ListFileType listFileType) {
    StorageStructureID retVal;
    retVal.storageStructureType = LISTS;
    retVal.listFileID =
        ListFileID(listFileType, AdjListsID(RelNodeLabelAndDir(relLabel, srcNodeLabel, dir)));
    return retVal;
}

StorageStructureID StorageStructureID::newRelPropertyListsID(label_t relLabel, label_t srcNodeLabel,
    RelDirection dir, uint32_t propertyID, ListFileType listFileType) {
    StorageStructureID retVal;
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
