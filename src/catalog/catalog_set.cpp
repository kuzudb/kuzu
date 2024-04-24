#include "catalog/catalog_set.h"

#include "common/exception/catalog.h"
#include "common/string_format.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

bool CatalogSet::containsEntry(const std::string& name) const {
    return entries.contains(std::move(name));
}

CatalogEntry* CatalogSet::getEntry(const std::string& name) {
    // LCOV_EXCL_START
    validateExist(name);
    // LCOV_EXCL_STOP
    return entries.at(name).get();
}

void CatalogSet::createEntry(std::unique_ptr<CatalogEntry> entry) {
    // LCOV_EXCL_START
    validateNotExist(entry->getName());
    // LCOV_EXCL_STOP
    entries.emplace(entry->getName(), std::move(entry));
}

void CatalogSet::removeEntry(const std::string& name) {
    // LCOV_EXCL_START
    validateExist(name);
    // LCOV_EXCL_STOP
    entries.erase(std::move(name));
}

void CatalogSet::renameEntry(const std::string& oldName, const std::string& newName) {
    // LCOV_EXCL_START
    validateExist(oldName);
    // LCOV_EXCL_STOP
    auto entry = std::move(entries.at(oldName));
    entry->rename(newName);
    entries.erase(oldName);
    entries.insert_or_assign(newName, std::move(entry));
}

void CatalogSet::serialize(common::Serializer serializer) const {
    uint64_t numEntries = 0;
    for (auto& [name, entry] : entries) {
        switch (entry->getType()) {
        case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
        case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
        case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
        case CatalogEntryType::TABLE_FUNCTION_ENTRY:
            continue;
        default:
            numEntries++;
        }
    }
    serializer.serializeValue(numEntries);
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
        if (entry != nullptr) {
            catalogSet->createEntry(std::move(entry));
        }
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

// Ideally we should not trigger the following check. Instead, we should throw more informative
// error message at catalog level.
void CatalogSet::validateExist(const std::string& name) const {
    if (!containsEntry(name)) {
        throw CatalogException(stringFormat("{} does not exist in catalog.", name));
    }
}

void CatalogSet::validateNotExist(const std::string& name) const {
    if (containsEntry(name)) {
        throw CatalogException(stringFormat("{} already exists in catalog.", name));
    }
}

} // namespace catalog
} // namespace kuzu
