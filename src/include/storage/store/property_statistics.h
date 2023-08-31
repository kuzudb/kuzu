#pragma once

#include "common/types/types.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class PropertyStatistics {
public:
    PropertyStatistics() = default;
    explicit PropertyStatistics(bool mayHaveNullValue) : mayHaveNullValue{mayHaveNullValue} {}
    PropertyStatistics(const PropertyStatistics& other) {
        this->mayHaveNullValue = other.mayHaveNullValue;
    }

    inline bool mayHaveNull() const { return mayHaveNullValue; }
    PropertyStatistics(PropertyStatistics& other) = default;

    void serialize(common::FileInfo* fileInfo, uint64_t& offset);
    static std::unique_ptr<PropertyStatistics> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    inline void setHasNull() { mayHaveNullValue = true; }

private:
    // Stores whether or not the property is known to have contained a null value
    // If false, the property is guaranteed to not contain any nulls
    bool mayHaveNullValue = false;
};

class TablesStatistics;

// Accessor used by NodeColumn, so that it doesn't need to handle the TableStatistics directly
class RWPropertyStats {
public:
    RWPropertyStats(TablesStatistics* tablesStatistics, common::table_id_t tableID,
        common::property_id_t propertyID);

    static RWPropertyStats empty() { return RWPropertyStats(nullptr, 0, 0); }

    bool mayHaveNull(const transaction::Transaction& transaction);
    void setHasNull(const transaction::Transaction& transaction);

private:
    TablesStatistics* tablesStatistics;
    common::table_id_t tableID;
    common::property_id_t propertyID;
};

} // namespace storage
} // namespace kuzu
