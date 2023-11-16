#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace transaction {
class Transaction;
};
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

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<PropertyStatistics> deserialize(common::Deserializer& deserializer);

    inline void setHasNull() { mayHaveNullValue = true; }

private:
    // Stores whether or not the property is known to have contained a null value
    // If false, the property is guaranteed to not contain any nulls
    bool mayHaveNullValue = false;
};

class TablesStatistics;

// Accessor used by Column, so that it doesn't need to handle the TableStatistics directly
class RWPropertyStats {
public:
    RWPropertyStats(TablesStatistics* tablesStatistics, common::table_id_t tableID,
        common::property_id_t propertyID);

    // This is used for chunks that don't have nullColumn. For example, the serial column.
    inline static RWPropertyStats empty() {
        return RWPropertyStats(nullptr, common::INVALID_PROPERTY_ID, common::INVALID_PROPERTY_ID);
    }

    bool mayHaveNull(const transaction::Transaction& transaction);
    void setHasNull(const transaction::Transaction& transaction);

private:
    TablesStatistics* tablesStatistics;
    common::table_id_t tableID;
    common::property_id_t propertyID;
};

} // namespace storage
} // namespace kuzu
