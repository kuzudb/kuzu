#include "catalog/catalog_set.h"

#include "common/assert.h"

namespace kuzu {
namespace catalog {

bool CatalogSet::containsEntry(const std::string& name) const {
    return entries.contains(std::move(name));
}

CatalogEntry* CatalogSet::getEntry(const std::string& name) {
    KU_ASSERT(containsEntry(name));
    return entries.at(name).get();
}

void CatalogSet::createEntry(std::unique_ptr<CatalogEntry> entry) {
    KU_ASSERT(!entries.contains(entry->getName()));
    entries.emplace(entry->getName(), std::move(entry));
}

void CatalogSet::removeEntry(const std::string& name) {
    KU_ASSERT(containsEntry(name));
    entries.erase(std::move(name));
}

void CatalogSet::renameEntry(const std::string& oldName, const std::string& newName) {
    KU_ASSERT(containsEntry(oldName));
    auto entry = std::move(entries.at(oldName));
    entry->rename(newName);
    entries.erase(oldName);
    entries.insert_or_assign(newName, std::move(entry));
}

void CatalogSet::serialize(common::Serializer serializer) const {
    serializer.serializeValue<uint64_t>(entries.size());
    for (auto& [name, entry] : entries) {
        entry->serialize(serializer);
    }
}

std::unique_ptr<CatalogSet> CatalogSet::deserialize(common::Deserializer& deserializer) {
    std::unique_ptr<CatalogSet> catalogSet = std::make_unique<CatalogSet>();
    uint64_t numEntries;
    deserializer.deserializeValue(numEntries);
    for (uint64_t i = 0; i < numEntries; i++) {
        auto entry = CatalogEntry::deserialize(deserializer);
        catalogSet->createEntry(std::move(entry));
    }
    return catalogSet;
}

std::unique_ptr<CatalogSet> CatalogSet::copy() const {
    auto newCatalogSet = std::make_unique<CatalogSet>();
    for (auto& [name, entry] : entries) {
        newCatalogSet->createEntry(entry->copy());
    }
    return newCatalogSet;
}

} // namespace catalog
} // namespace kuzu
