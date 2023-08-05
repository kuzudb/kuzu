#pragma once

#include <map>
#include <string>

#include "catalog/catalog.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"
#include "storage/store/node_column.h"

namespace kuzu {
namespace storage {

class WALReplayerUtils {
public:
    static inline void removeHashIndexFile(
        catalog::NodeTableSchema* tableSchema, const std::string& directory) {
        fileOperationOnNodeFiles(
            tableSchema, directory, removeColumnFilesIfExists, removeListFilesIfExists);
    }

    static inline void removeDBFilesForRelTable(
        catalog::RelTableSchema* tableSchema, const std::string& directory) {
        fileOperationOnRelFiles(
            tableSchema, directory, removeColumnFilesIfExists, removeListFilesIfExists);
    }

    static void removeDBFilesForRelProperty(const std::string& directory,
        catalog::RelTableSchema* relTableSchema, common::property_id_t propertyID);

    static void createEmptyDBFilesForNewRelTable(catalog::RelTableSchema* relTableSchema,
        const std::string& directory,
        const std::map<common::table_id_t, common::offset_t>& maxNodeOffsetsPerTable);

    // Create empty hash index file for the new node table.
    static void createEmptyHashIndexFiles(
        catalog::NodeTableSchema* nodeTableSchema, const std::string& directory);

    static void renameDBFilesForRelProperty(const std::string& directory,
        catalog::RelTableSchema* relTableSchema, common::property_id_t propertyID);

private:
    static inline void removeColumnFilesForPropertyIfExists(const std::string& directory,
        common::table_id_t relTableID, common::table_id_t boundTableID,
        common::RelDataDirection relDirection, common::property_id_t propertyID,
        common::DBFileType dbFileType) {
        removeColumnFilesIfExists(StorageUtils::getRelPropertyColumnFName(
            directory, relTableID, relDirection, propertyID, common::DBFileType::ORIGINAL));
    }

    static inline void removeListFilesForPropertyIfExists(const std::string& directory,
        common::table_id_t relTableID, common::table_id_t boundTableID,
        common::RelDataDirection relDirection, common::property_id_t propertyID,
        common::DBFileType dbFileType) {
        removeListFilesIfExists(StorageUtils::getRelPropertyListsFName(
            directory, relTableID, relDirection, propertyID, common::DBFileType::ORIGINAL));
    }

    static void createEmptyDBFilesForRelProperties(catalog::RelTableSchema* relTableSchema,
        const std::string& directory, common::RelDataDirection relDirection, uint32_t numNodes,
        bool isForRelPropertyColumn);

    static void createEmptyDBFilesForColumns(
        const std::map<common::table_id_t, uint64_t>& maxNodeOffsetsPerTable,
        common::RelDataDirection relDirection, const std::string& directory,
        catalog::RelTableSchema* relTableSchema);

    static void createEmptyDBFilesForLists(
        const std::map<common::table_id_t, uint64_t>& maxNodeOffsetsPerTable,
        common::RelDataDirection relDirection, const std::string& directory,
        catalog::RelTableSchema* relTableSchema);

    static void replaceOriginalColumnFilesWithWALVersionIfExists(
        const std::string& originalColFileName);

    static void replaceOriginalListFilesWithWALVersionIfExists(
        const std::string& originalListFileName);

    static void removeListFilesIfExists(const std::string& fileName);

    static void removeColumnFilesIfExists(const std::string& fileName);

    static void fileOperationOnNodeFiles(catalog::NodeTableSchema* nodeTableSchema,
        const std::string& directory, std::function<void(std::string fileName)> columnFileOperation,
        std::function<void(std::string fileName)> listFileOperation);

    static void fileOperationOnRelFiles(catalog::RelTableSchema* relTableSchema,
        const std::string& directory, std::function<void(std::string fileName)> columnFileOperation,
        std::function<void(std::string fileName)> listFileOperation);

    static void fileOperationOnRelPropertyFiles(catalog::RelTableSchema* tableSchema,
        common::table_id_t nodeTableID, const std::string& directory,
        common::RelDataDirection relDirection, bool isColumnProperty,
        std::function<void(std::string fileName)> columnFileOperation,
        std::function<void(std::string fileName)> listFileOperation);
};

} // namespace storage
} // namespace kuzu
