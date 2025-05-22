#pragma once

#include "hnsw_config.h"
#include "storage/table/list_chunk_data.h"

namespace kuzu {
namespace catalog {
class TableCatalogEntry;
class NodeTableCatalogEntry;
} // namespace catalog

namespace main {
class ClientContext;
}

namespace vector_extension {
template<typename T>
concept VectorElementType = std::is_floating_point_v<T>;
using metric_func_t = std::function<double(const void*, const void*, uint32_t)>;

struct HNSWIndexUtils {
    enum class KUZU_API IndexOperation { CREATE, QUERY, DROP };

    static void validateIndexExistence(const main::ClientContext& context,
        const catalog::TableCatalogEntry* tableEntry, const std::string& indexName,
        IndexOperation indexOperation);

    static catalog::NodeTableCatalogEntry* bindNodeTable(const main::ClientContext& context,
        const std::string& tableName, const std::string& indexName, IndexOperation indexOperation);

    static void validateAutoTransaction(const main::ClientContext& context,
        const std::string& funcName);

    static metric_func_t getMetricsFunction(MetricType metric, const common::LogicalType& type);

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
