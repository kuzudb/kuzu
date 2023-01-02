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
    static void createEmptyDBFilesForNewRelTable(Catalog* catalog, table_id_t tableID,
        const string& directory, const map<table_id_t, node_offset_t>& maxNodeOffsetsPerTable);

    static void createEmptyDBFilesForNewNodeTable(
        Catalog* catalog, table_id_t tableID, string directory);

    static inline void replaceNodeFilesWithVersionFromWALIfExists(
        NodeTableSchema* nodeTableSchema, string directory) {
        fileOperationOnNodeFiles(nodeTableSchema, directory,
            replaceOriginalColumnFilesWithWALVersionIfExists,
            replaceOriginalListFilesWithWALVersionIfExists);
    }

    static void replaceRelPropertyFilesWithVersionFromWALIfExists(
        RelTableSchema* relTableSchema, string directory, const Catalog* catalog) {
        fileOperationOnRelFiles(relTableSchema, directory, catalog,
            replaceOriginalColumnFilesWithWALVersionIfExists,
            replaceOriginalListFilesWithWALVersionIfExists);
    }

    static inline void removeDBFilesForNodeTable(NodeTableSchema* tableSchema, string directory) {
        fileOperationOnNodeFiles(
            tableSchema, directory, removeColumnFilesIfExists, removeListFilesIfExists);
    }

    static void removeDBFilesForRelTable(
        RelTableSchema* tableSchema, string directory, const Catalog* catalog) {
        fileOperationOnRelFiles(
            tableSchema, directory, catalog, removeColumnFilesIfExists, removeListFilesIfExists);
    }

private:
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

    static void replaceOriginalColumnFilesWithWALVersionIfExists(string originalColFileName);

    static void replaceOriginalListFilesWithWALVersionIfExists(string originalListFileName);

    static void removeListFilesIfExists(string fileName);

    static void removeColumnFilesIfExists(string fileName);

    static void fileOperationOnNodeFiles(NodeTableSchema* nodeTableSchema, string directory,
        std::function<void(string fileName)> columnFileOperation,
        std::function<void(string fileName)> listFileOperation);

    static void fileOperationOnRelFiles(RelTableSchema* relTableSchema, string directory,
        const Catalog* catalog, std::function<void(string fileName)> columnFileOperation,
        std::function<void(string fileName)> listFileOperation);

    static void fileOperationOnRelPropertyFiles(RelTableSchema* tableSchema, table_id_t nodeTableID,
        const string& directory, RelDirection relDirection, bool isColumnProperty,
        std::function<void(string fileName)> columnFileOperation,
        std::function<void(string fileName)> listFileOperation);
};

} // namespace storage
} // namespace kuzu
