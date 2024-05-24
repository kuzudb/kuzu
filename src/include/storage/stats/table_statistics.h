#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/enums/table_type.h"

namespace kuzu {
namespace catalog {
class TableSchema;
} // namespace catalog

namespace common {
class Serializer;
class Deserializer;
} // namespace common

namespace storage {

class TableStatistics {
public:
    explicit TableStatistics(const catalog::TableCatalogEntry& tableEntry);
    TableStatistics(common::TableType tableType, uint64_t numTuples, common::table_id_t tableID);
    TableStatistics(const TableStatistics& other);

    virtual ~TableStatistics() = default;

    bool isEmpty() const { return numTuples == 0; }
    uint64_t getNumTuples() const { return numTuples; }
    virtual void setNumTuples(uint64_t numTuples_) {
        KU_ASSERT(numTuples_ != UINT64_MAX);
        numTuples = numTuples_;
    }

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<TableStatistics> deserialize(common::Deserializer& deserializer);
    virtual void serializeInternal(common::Serializer& serializer) = 0;

    virtual std::unique_ptr<TableStatistics> copy() = 0;

protected:
    common::TableType tableType;
    uint64_t numTuples;
    common::table_id_t tableID;
};

} // namespace storage
} // namespace kuzu
