#pragma once

#include "hnsw_config.h"
#include "storage/store/list_chunk_data.h"

namespace kuzu {
namespace catalog {
class TableCatalogEntry;
} // namespace catalog

namespace storage {
class EmbeddingColumn;

struct HNSWIndexUtils {
    static void validateTableAndColumnType(const catalog::TableCatalogEntry& tableEntry,
        const std::string& columnName);

    static void validateQueryVector(const common::LogicalType& type, uint64_t dimension);

    static std::string getUpperGraphTableName(const std::string& indexName) {
        return "_" + indexName + "_UPPER";
    }
    static std::string getLowerGraphTableName(const std::string& indexName) {
        return "_" + indexName + "_LOWER";
    }

    template<typename T>
    static T* getEmbedding(const ColumnChunkData& embeddings, common::offset_t offset,
        common::length_t dimension) {
        auto& listChunk = embeddings.cast<ListChunkData>();
        return &listChunk.getDataColumnChunk()->getData<T>()[offset * dimension];
    }

    static double computeDistance(DistFuncType funcType, const EmbeddingColumn& embeddings,
        const float* left, common::offset_t rightOffset, transaction::Transaction* transaction);

private:
    static void validateColumnType(const common::LogicalType& type);
};

} // namespace storage
} // namespace kuzu
