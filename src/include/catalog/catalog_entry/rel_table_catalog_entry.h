#pragma once

#include "common/enums/rel_direction.h"
#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

enum class RelMultiplicity : uint8_t { MANY, ONE };
struct RelMultiplicityUtils {
    static RelMultiplicity getFwd(const std::string& multiplicityStr);
    static RelMultiplicity getBwd(const std::string& multiplicityStr);
};

class RelTableCatalogEntry final : public TableCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    RelTableCatalogEntry() = default;
    RelTableCatalogEntry(std::string name, common::table_id_t tableID,
        RelMultiplicity srcMultiplicity, RelMultiplicity dstMultiplicity,
        common::table_id_t srcTableID, common::table_id_t dstTableID);
    RelTableCatalogEntry(const RelTableCatalogEntry& other);

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    bool isParent(common::table_id_t tableID) override;
    common::TableType getTableType() const override { return common::TableType::REL; }
    common::table_id_t getSrcTableID() const { return srcTableID; }
    common::table_id_t getDstTableID() const { return dstTableID; }
    bool isSingleMultiplicity(common::RelDataDirection direction) const;
    RelMultiplicity getMultiplicity(common::RelDataDirection direction) const;
    common::table_id_t getBoundTableID(common::RelDataDirection relDirection) const;
    common::table_id_t getNbrTableID(common::RelDataDirection relDirection) const;

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelTableCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::unique_ptr<CatalogEntry> copy() const override;
    std::string toCypher(main::ClientContext* clientContext) const override;

private:
    RelMultiplicity srcMultiplicity;
    RelMultiplicity dstMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
};

} // namespace catalog
} // namespace kuzu
