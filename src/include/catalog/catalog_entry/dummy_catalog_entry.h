#pragma once

#include "catalog/catalog_entry/catalog_entry.h"

namespace kuzu {
namespace catalog {

class DummyCatalogEntry : public CatalogEntry {
public:
    explicit DummyCatalogEntry(std::string name)
        : CatalogEntry{CatalogEntryType::DUMMY_ENTRY, std::move(name)} {
        setDeleted(true);
        setTimestamp(0);
    }

    void serialize(common::Serializer& /*serializer*/) const override { KU_UNREACHABLE; }
    std::string toCypher(main::ClientContext* /*clientContext*/) const override { KU_UNREACHABLE; }
};

} // namespace catalog
} // namespace kuzu
