#include "storage/wal_replayer_utils.h"

#include "catalog/node_table_schema.h"
#include "storage/index/hash_index_builder.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void WALReplayerUtils::createEmptyHashIndexFiles(
    catalog::NodeTableSchema* nodeTableSchema, const std::string& directory) {
    auto pk = nodeTableSchema->getPrimaryKey();
    switch (pk->getDataType()->getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        auto pkIndex = make_unique<HashIndexBuilder<int64_t>>(
            StorageUtils::getNodeIndexFName(
                directory, nodeTableSchema->tableID, FileVersionType::ORIGINAL),
            *pk->getDataType());
        pkIndex->bulkReserve(0 /* numNodes */);
        pkIndex->flush();
    } break;
    case LogicalTypeID::STRING: {
        auto pkIndex = make_unique<HashIndexBuilder<ku_string_t>>(
            StorageUtils::getNodeIndexFName(
                directory, nodeTableSchema->tableID, FileVersionType::ORIGINAL),
            *pk->getDataType());
        pkIndex->bulkReserve(0 /* numNodes */);
        pkIndex->flush();
    } break;
    case LogicalTypeID::SERIAL: {
        // DO NOTHING.
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void WALReplayerUtils::removeColumnFilesIfExists(const std::string& fileName) {
    FileUtils::removeFileIfExists(fileName);
    FileUtils::removeFileIfExists(StorageUtils::getOverflowFileName(fileName));
}

void WALReplayerUtils::fileOperationOnNodeFiles(NodeTableSchema* nodeTableSchema,
    const std::string& directory, std::function<void(std::string fileName)> columnFileOperation) {
    columnFileOperation(StorageUtils::getNodeIndexFName(
        directory, nodeTableSchema->tableID, FileVersionType::ORIGINAL));
}

} // namespace storage
} // namespace kuzu
