#pragma once

#include <map>
#include <string>

#include "catalog/catalog.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

class WALReplayerUtils {
public:
    static inline void replaceNodeFilesWithVersionFromWALIfExists(
        NodeTableSchema* nodeTableSchema, const string& directory) {
        fileOperationOnNodeFiles(nodeTableSchema, directory,
            replaceOriginalColumnFilesWithWALVersionIfExists,
            replaceOriginalListFilesWithWALVersionIfExists);
    }

    static inline void replaceRelPropertyFilesWithVersionFromWALIfExists(
        RelTableSchema* relTableSchema, const string& directory) {
        fileOperationOnRelFiles(relTableSchema, directory,
            replaceOriginalColumnFilesWithWALVersionIfExists,
            replaceOriginalListFilesWithWALVersionIfExists);
    }

    static inline void removeDBFilesForNodeTable(
        NodeTableSchema* tableSchema, const string& directory) {
        fileOperationOnNodeFiles(
            tableSchema, directory, removeColumnFilesIfExists, removeListFilesIfExists);
    }

    static inline void removeDBFilesForRelTable(
        RelTableSchema* tableSchema, const string& directory) {
        fileOperationOnRelFiles(
            tableSchema, directory, removeColumnFilesIfExists, removeListFilesIfExists);
    }

    static inline void removeDBFilesForNodeProperty(
        const string& directory, table_id_t tableID, property_id_t propertyID) {
        removeColumnFilesIfExists(StorageUtils::getNodePropertyColumnFName(
            directory, tableID, propertyID, DBFileType::ORIGINAL));
    }

    static inline void renameDBFilesForNodeProperty(
        const string& directory, table_id_t tableID, property_id_t propertyID) {
        replaceOriginalColumnFilesWithWALVersionIfExists(StorageUtils::getNodePropertyColumnFName(
            directory, tableID, propertyID, DBFileType::ORIGINAL));
    }

    static void removeDBFilesForRelProperty(
        const string& directory, RelTableSchema* relTableSchema, property_id_t propertyID);

    static void createEmptyDBFilesForNewRelTable(RelTableSchema* relTableSchema,
        const string& directory, const map<table_id_t, offset_t>& maxNodeOffsetsPerTable);

    static void createEmptyDBFilesForNewNodeTable(
        NodeTableSchema* nodeTableSchema, const string& directory);

    static void renameDBFilesForRelProperty(
        const string& directory, RelTableSchema* relTableSchema, property_id_t propertyID);

    static void replaceListsHeadersFilesWithVersionFromWALIfExists(
        unordered_set<RelTableSchema*> relTableSchemas, table_id_t boundTableID,
        const string& directory);

private:
    static inline void removeColumnFilesForPropertyIfExists(const string& directory,
        table_id_t relTableID, table_id_t boundTableID, RelDirection relDirection,
        property_id_t propertyID, DBFileType dbFileType) {
        removeColumnFilesIfExists(StorageUtils::getRelPropertyColumnFName(
            directory, relTableID, boundTableID, relDirection, propertyID, DBFileType::ORIGINAL));
    }

    static inline void removeListFilesForPropertyIfExists(const string& directory,
        table_id_t relTableID, table_id_t boundTableID, RelDirection relDirection,
        property_id_t propertyID, DBFileType dbFileType) {
        removeListFilesIfExists(StorageUtils::getRelPropertyListsFName(
            directory, relTableID, boundTableID, relDirection, propertyID, DBFileType::ORIGINAL));
    }

    static void initLargeListPageListsAndSaveToFile(InMemLists* inMemLists);

    static void createEmptyDBFilesForRelProperties(RelTableSchema* relTableSchema,
        table_id_t tableID, const string& directory, RelDirection relDireciton, uint32_t numNodes,
        bool isForRelPropertyColumn);

    static void createEmptyDBFilesForColumns(const unordered_set<table_id_t>& nodeTableIDs,
        const map<table_id_t, uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
        const string& directory, RelTableSchema* relTableSchema);

    static void createEmptyDBFilesForLists(const unordered_set<table_id_t>& boundTableIDs,
        const map<table_id_t, uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
        const string& directory, RelTableSchema* relTableSchema);

    static void replaceOriginalColumnFilesWithWALVersionIfExists(const string& originalColFileName);

    static void replaceOriginalListFilesWithWALVersionIfExists(const string& originalListFileName);

    static void removeListFilesIfExists(const string& fileName);

    static void removeColumnFilesIfExists(const string& fileName);

    static void fileOperationOnNodeFiles(NodeTableSchema* nodeTableSchema, const string& directory,
        std::function<void(string fileName)> columnFileOperation,
        std::function<void(string fileName)> listFileOperation);

    static void fileOperationOnRelFiles(RelTableSchema* relTableSchema, const string& directory,
        std::function<void(string fileName)> columnFileOperation,
        std::function<void(string fileName)> listFileOperation);

    static void fileOperationOnRelPropertyFiles(RelTableSchema* tableSchema, table_id_t nodeTableID,
        const string& directory, RelDirection relDirection, bool isColumnProperty,
        std::function<void(string fileName)> columnFileOperation,
        std::function<void(string fileName)> listFileOperation);
};

} // namespace storage
} // namespace kuzu
