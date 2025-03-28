#pragma once

#include "hnsw_config.h"
#include "storage/store/list_chunk_data.h"

namespace kuzu {
namespace catalog {
class TableCatalogEntry;
class NodeTableCatalogEntry;
} // namespace catalog

namespace main {
class ClientContext;
}

namespace vector_extension {

struct HNSWIndexUtils {
    enum class KUZU_API IndexOperation { CREATE, QUERY, DROP };

    static void validateIndexExistence(const main::ClientContext& context,
        const catalog::TableCatalogEntry* tableEntry, const std::string& indexName,
        IndexOperation indexOperation);

    static catalog::NodeTableCatalogEntry* bindNodeTable(const main::ClientContext& context,
        const std::string& tableName, const std::string& indexName, IndexOperation indexOperation);

    static void validateAutoTransaction(const main::ClientContext& context,
        const std::string& funcName);

    static double computeDistance(MetricType funcType, const float* left, const float* right,
        uint32_t dimension);

    static void validateColumnType(const catalog::TableCatalogEntry& tableEntry,
        const std::string& columnName);

    static std::string getUpperGraphTableName(common::table_id_t tableID,
        const std::string& indexName) {
        return "_" + std::to_string(tableID) + "_" + indexName + "_UPPER";
    }
    static std::string getLowerGraphTableName(common::table_id_t tableID,
        const std::string& indexName) {
        return "_" + std::to_string(tableID) + "_" + indexName + "_LOWER";
    }

    template<typename T>
    static T* getEmbedding(const storage::ColumnChunkData& embeddings, common::offset_t offset,
        common::length_t dimension) {
        auto& listChunk = embeddings.cast<storage::ListChunkData>();
        return &listChunk.getDataColumnChunk()->getData<T>()[offset * dimension];
    }

private:
    static void validateColumnType(const common::LogicalType& type);
};

} // namespace vector_extension
} // namespace kuzu
