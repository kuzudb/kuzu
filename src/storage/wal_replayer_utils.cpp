#include "storage/wal_replayer_utils.h"

#include "catalog/node_table_schema.h"
#include "common/file_system/virtual_file_system.h"
#include "storage/index/hash_index_builder.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void WALReplayerUtils::removeHashIndexFile(
    common::VirtualFileSystem* vfs, table_id_t tableID, const std::string& directory) {
    vfs->removeFileIfExists(
        StorageUtils::getNodeIndexFName(vfs, directory, tableID, FileVersionType::ORIGINAL));
}

void WALReplayerUtils::createEmptyHashIndexFiles(catalog::NodeTableSchema* nodeTableSchema,
    const std::string& directory, VirtualFileSystem* vfs) {
    auto pk = nodeTableSchema->getPrimaryKey();
    auto dt = pk->getDataType();
    if (dt->getLogicalTypeID() != LogicalTypeID::SERIAL) {
        auto pkIndex = make_unique<PrimaryKeyIndexBuilder>(
            StorageUtils::getNodeIndexFName(
                vfs, directory, nodeTableSchema->tableID, FileVersionType::ORIGINAL),
            *pk->getDataType(), vfs);
        pkIndex->bulkReserve(0 /* numNodes */);
        pkIndex->flush();
    }
}

} // namespace storage
} // namespace kuzu
