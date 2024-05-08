#include "catalog/catalog_set.h"

#include "binder/ddl/bound_alter_info.h"
#include "catalog/catalog_entry/dummy_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/assert.h"
#include "common/exception/catalog.h"
#include "common/string_format.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

bool CatalogSet::containsEntry(Transaction* transaction, const std::string& name) const {
    if (!entries.contains(name)) {
        return false;
    }
    // Check versions.
    auto entry = traverseVersionChainsForTransaction(transaction, entries.at(name).get());
    return !entry->isDeleted();
}

CatalogEntry* CatalogSet::getEntry(Transaction* transaction, const std::string& name) {
    // LCOV_EXCL_START
    validateExist(transaction, name);
    // LCOV_EXCL_STOP
    auto entry = traverseVersionChainsForTransaction(transaction, entries.at(name).get());
    KU_ASSERT(entry != nullptr && !entry->isDeleted());
    return entry;
}

void CatalogSet::createEntry(Transaction* transaction, std::unique_ptr<CatalogEntry> entry) {
    // LCOV_EXCL_START
    validateNotExist(transaction, entry->getName());
    // LCOV_EXCL_STOP
    entry->setTimestamp(transaction->getID());
    if (entries.contains(entry->getName())) {
        auto existingEntry = entries.at(entry->getName()).get();
        if (checkWWConflict(transaction, existingEntry)) {
            throw CatalogException(stringFormat(
                "Write-write conflict on creating catalog entry with name {}.", entry->getName()));
        }
        if (!existingEntry->isDeleted()) {
            throw CatalogException(
                stringFormat("Catalog entry with name {} already exists.", entry->getName()));
        }
    }
    auto dummyEntry = createDummyEntry(entry->getName());
    entries.emplace(entry->getName(), std::move(dummyEntry));
    auto entryPtr = entry.get();
    emplace(std::move(entry));
    KU_ASSERT(transaction);
    if (transaction->getStartTS() > 0) {
        KU_ASSERT(transaction->getID() != 0);
        transaction->addCatalogEntry(this, entryPtr->getPrev());
    }
}

void CatalogSet::emplace(std::unique_ptr<CatalogEntry> entry) {
    if (entries.contains(entry->getName())) {
        entry->setPrev(std::move(entries.at(entry->getName())));
        entries.erase(entry->getName());
    }
    entries.emplace(entry->getName(), std::move(entry));
}

void CatalogSet::erase(const std::string& name) {
    entries.erase(name);
}

std::unique_ptr<CatalogEntry> CatalogSet::createDummyEntry(std::string name) const {
    return std::make_unique<DummyCatalogEntry>(std::move(name));
}

CatalogEntry* CatalogSet::traverseVersionChainsForTransaction(Transaction* transaction,
    CatalogEntry* currentEntry) const {
    while (currentEntry) {
        if (currentEntry->getTimestamp() == transaction->getID()) {
            // This entry is created by the current transaction.
            break;
        }
        if (currentEntry->getTimestamp() <= transaction->getStartTS()) {
            // This entry was committed before the current transaction starts.
            break;
        }
        currentEntry = currentEntry->getPrev();
    }
    return currentEntry;
}

CatalogEntry* CatalogSet::getCommittedEntry(CatalogEntry* entry) const {
    while (entry) {
        if (entry->getTimestamp() < Transaction::START_TRANSACTION_ID) {
            break;
        }
        entry = entry->getPrev();
    }
    return entry;
}

bool CatalogSet::checkWWConflict(Transaction* transaction, CatalogEntry* entry) const {
    return (entry->getTimestamp() >= Transaction::START_TRANSACTION_ID &&
               entry->getTimestamp() != transaction->getID()) ||
           (entry->getTimestamp() < Transaction::START_TRANSACTION_ID &&
               entry->getTimestamp() > transaction->getStartTS());
}

void CatalogSet::dropEntry(Transaction* transaction, const std::string& name) {
    // LCOV_EXCL_START
    validateExist(transaction, name);
    // LCOV_EXCL_STOP
    // TODO: Should handle rel group and rdf graph.
    auto tombstone = createDummyEntry(name);
    tombstone->setTimestamp(transaction->getID());
    auto tombstonePtr = tombstone.get();
    emplace(std::move(tombstone));
    if (transaction->getStartTS() > 0) {
        KU_ASSERT(transaction->getID() != 0);
        transaction->addCatalogEntry(this, tombstonePtr->getPrev());
    }
}

void CatalogSet::alterEntry(Transaction* transaction, const binder::BoundAlterInfo& alterInfo) {
    // LCOV_EXCL_START
    validateExist(transaction, alterInfo.tableName);
    // LCOV_EXCL_STOP
    auto entry = getEntry(transaction, alterInfo.tableName);
    KU_ASSERT(entry->getType() == CatalogEntryType::NODE_TABLE_ENTRY ||
              entry->getType() == CatalogEntryType::REL_TABLE_ENTRY ||
              entry->getType() == CatalogEntryType::REL_GROUP_ENTRY ||
              entry->getType() == CatalogEntryType::RDF_GRAPH_ENTRY);
    auto tableEntry = ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry);
    auto newEntry = tableEntry->alter(alterInfo);
    newEntry->setTimestamp(transaction->getID());
    if (alterInfo.alterType == AlterType::RENAME_TABLE) {
        // We treat rename table as drop and create.
        dropEntry(transaction, alterInfo.tableName);
        createEntry(transaction, std::move(newEntry));
        return;
    }
    emplace(std::move(newEntry));
    if (transaction->getStartTS() > 0) {
        KU_ASSERT(transaction->getID() != 0);
        transaction->addCatalogEntry(this, entry);
    }
}

CatalogEntrySet CatalogSet::getEntries(Transaction* transaction) {
    CatalogEntrySet result;
    for (auto& [name, entry] : entries) {
        auto currentEntry = traverseVersionChainsForTransaction(transaction, entry.get());
        if (currentEntry->isDeleted()) {
            continue;
        }
        result.emplace(name, currentEntry);
    }
    return result;
}

void CatalogSet::serialize(common::Serializer serializer) const {
    std::vector<CatalogEntry*> entriesToSerialize;
    for (auto& [_, entry] : entries) {
        switch (entry->getType()) {
        case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
        case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
        case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
        case CatalogEntryType::TABLE_FUNCTION_ENTRY:
            continue;
        default: {
            auto committedEntry = getCommittedEntry(entry.get());
            if (committedEntry && !committedEntry->isDeleted()) {
                entriesToSerialize.push_back(committedEntry);
            }
        }
        }
    }
    serializer.serializeValue(nextOID);
    uint64_t numEntriesToSerialize = entriesToSerialize.size();
    serializer.serializeValue(numEntriesToSerialize);
    for (auto entry : entriesToSerialize) {
        entry->serialize(serializer);
    }
}

std::unique_ptr<CatalogSet> CatalogSet::deserialize(common::Deserializer& deserializer) {
    std::unique_ptr<CatalogSet> catalogSet = std::make_unique<CatalogSet>();
    deserializer.deserializeValue(catalogSet->nextOID);
    uint64_t numEntries;
    deserializer.deserializeValue(numEntries);
    for (uint64_t i = 0; i < numEntries; i++) {
        auto entry = CatalogEntry::deserialize(deserializer);
        if (entry != nullptr) {
            catalogSet->emplace(std::move(entry));
        }
    }
    return catalogSet;
}

// Ideally we should not trigger the following check. Instead, we should throw more informative
// error message at catalog level.
void CatalogSet::validateExist(Transaction* transaction, const std::string& name) const {
    if (!containsEntry(transaction, name)) {
        throw CatalogException(stringFormat("{} does not exist in catalog.", name));
    }
}

void CatalogSet::validateNotExist(Transaction* transaction, const std::string& name) const {
    if (containsEntry(transaction, name)) {
        throw CatalogException(stringFormat("{} already exists in catalog.", name));
    }
}

} // namespace catalog
} // namespace kuzu
