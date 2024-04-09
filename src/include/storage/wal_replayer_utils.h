#pragma once

#include <string>

#include "catalog/catalog_entry/node_table_catalog_entry.h"

namespace kuzu {
namespace catalog {
class NodeTableSchema;
class RelTableSchema;
} // namespace catalog

namespace common {
class VirtualFileSystem;
} // namespace common

namespace storage {

class WALReplayerUtils {
public:
    static void removeHashIndexFile(common::VirtualFileSystem* vfs, common::table_id_t tableID,
        const std::string& directory);

    // Create empty hash index file for the new node table.
    static void createEmptyHashIndexFiles(catalog::NodeTableCatalogEntry* nodeTableEntry,
        const std::string& directory, common::VirtualFileSystem* vfs);
};

} // namespace storage
} // namespace kuzu
