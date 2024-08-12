#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class ExternalTableCatalogEntry : public TableCatalogEntry {
public:
    ExternalTableCatalogEntry() = default;
    ExternalTableCatalogEntry(CatalogSet* set, CatalogEntryType entryType, std::string name,
        std::string externalDBName, std::string externalTableName,
        std::unique_ptr<TableCatalogEntry> physicalEntry)
        : TableCatalogEntry{set, entryType, std::move(name)},
          externalDBName{std::move(externalDBName)}, externalTableName{std::move(externalTableName)}, physicalEntry{std::move(physicalEntry)} {}

    std::string getExternalDBName() const {
        return externalDBName;
    }
    std::string getExternalTableName() const {
        return externalTableName;
    }
    TableCatalogEntry* getPhysicalEntry() const {
        return physicalEntry.get();
    }

    void copyFrom(const CatalogEntry &other) override;

    void serialize(common::Serializer &serializer) const override;
    static std::unique_ptr<ExternalTableCatalogEntry> deserialize(common::Deserializer& deserializer, CatalogEntryType type);

protected:
    std::string externalDBName;
    std::string externalTableName;
    std::unique_ptr<TableCatalogEntry> physicalEntry;
};

}
}
