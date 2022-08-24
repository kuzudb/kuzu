#pragma once

#include <string>

#include "src/catalog/include/catalog.h"
#include "src/storage/in_mem_storage_structure/include/in_mem_column.h"
#include "src/storage/in_mem_storage_structure/include/in_mem_lists.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace storage {

class WALReplayerUtils {
public:
    static void createEmptyDBFilesForNewRelTable(Catalog* catalog, table_id_t tableID,
        const string& directory, const vector<uint64_t>& maxNodeOffsetsPerTable);

    static void createEmptyDBFilesForNewNodeTable(
        Catalog* catalog, table_id_t tableID, string directory);

    static void replaceNodeFilesWithVersionFromWALIfExists(
        catalog::NodeTableSchema* nodeTableSchema, string directory);

    static void replaceRelPropertyFilesWithVersionFromWALIfExists(
        catalog::RelTableSchema* relTableSchema, string directory, const catalog::Catalog* catalog);

private:
    static void initLargeListPageListsAndSaveToFile(InMemLists* inMemLists);

    static void createEmptyDBFilesForRelProperties(RelTableSchema* relTableSchema,
        table_id_t tableID, const string& directory, RelDirection relDireciton, uint32_t numNodes,
        bool isForRelPropertyColumn);

    static void createEmptyDBFilesForColumns(const unordered_set<table_id_t>& nodeTableIDs,
        const vector<uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
        const string& directory, const NodeIDCompressionScheme& directionNodeIDCompressionScheme,
        RelTableSchema* relTableSchema);

    static void createEmptyDBFilesForLists(const unordered_set<table_id_t>& nodeTableIDs,
        const vector<uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
        const string& directory, const NodeIDCompressionScheme& directionNodeIDCompressionScheme,
        RelTableSchema* relTableSchema);

    static void replaceOriginalColumnFilesWithWALVersionIfExists(string originalColFileName);

    static void replaceOriginalListFilesWithWALVersionIfExists(string originalListFileName);

    static void replaceRelPropertyFilesWithVersionFromWAL(catalog::RelTableSchema* relTableSchema,
        table_id_t tableID, const string& directory, RelDirection relDirection,
        bool isColumnProperty);
};

} // namespace storage
} // namespace graphflow
