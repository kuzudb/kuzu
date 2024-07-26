#include "catalog/catalog_set.h"

#include "binder/ddl/bound_alter_info.h"
#include "catalog/catalog_entry/dummy_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/assert.h"
#include "common/exception/catalog.h"
#include "common/serializer/deserializer.h"
#include "common/string_format.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

static bool checkWWConflict(Transaction* transaction, CatalogEntry* entry) {
    return (entry->getTimestamp() >= Transaction::START_TRANSACTION_ID &&
               entry->getTimestamp() != transaction->getID()) ||
           (entry->getTimestamp() < Transaction::START_TRANSACTION_ID &&
               entry->getTimestamp() > transaction->getStartTS());
}

bool CatalogSet::containsEntry(Transaction* transaction, const std::string& name) {
    std::lock_guard lck{mtx};
    return containsEntryNoLock(transaction, name);
}

bool CatalogSet::containsEntryNoLock(Transaction* transaction, const std::string& name) const {
    if (!entries.contains(name)) {
        return false;
    }
    // Check versions.
    auto entry = traverseVersionChainsForTransactionNoLock(transaction, entries.at(name).get());
    return !entry->isDeleted();
}

CatalogEntry* CatalogSet::getEntry(Transaction* transaction, const std::string& name) {
    std::lock_guard lck{mtx};
    return getEntryNoLock(transaction, name);
}

CatalogEntry* CatalogSet::getEntryNoLock(Transaction* transaction, const std::string& name) {
    // LCOV_EXCL_START
    validateExistNoLock(transaction, name);
    // LCOV_EXCL_STOP
    auto entry = traverseVersionChainsForTransactionNoLock(transaction, entries.at(name).get());
    KU_ASSERT(entry != nullptr && !entry->isDeleted());
    return entry;
}

static void logEntryForTrx(Transaction* transaction, CatalogSet& set, CatalogEntry& entry,
    bool skipLoggingToWAL = false) {
    KU_ASSERT(transaction);
    if (transaction->shouldAppendToUndoBuffer()) {
        transaction->pushCatalogEntry(set, entry, skipLoggingToWAL);
    }
}

void CatalogSet::createEntry(Transaction* transaction, std::unique_ptr<CatalogEntry> entry) {
    CatalogEntry* entryPtr;
    {
        std::lock_guard lck{mtx};
        entryPtr = createEntryNoLock(transaction, std::move(entry));
    }
    KU_ASSERT(entryPtr);
    logEntryForTrx(transaction, *this, *entryPtr);
}

CatalogEntry* CatalogSet::createEntryNoLock(Transaction* transaction,
    std::unique_ptr<CatalogEntry> entry) {
    // LCOV_EXCL_START
    validateNotExistNoLock(transaction, entry->getName());
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
    auto dummyEntry = createDummyEntryNoLock(entry->getName());
    entries.emplace(entry->getName(), std::move(dummyEntry));
    auto entryPtr = entry.get();
    emplaceNoLock(std::move(entry));
    return entryPtr->getPrev();
}

void CatalogSet::emplaceNoLock(std::unique_ptr<CatalogEntry> entry) {
    if (entries.contains(entry->getName())) {
        entry->setPrev(std::move(entries.at(entry->getName())));
        entries.erase(entry->getName());
    }
    entries.emplace(entry->getName(), std::move(entry));
}

void CatalogSet::eraseNoLock(const std::string& name) {
    entries.erase(name);
}

std::unique_ptr<CatalogEntry> CatalogSet::createDummyEntryNoLock(std::string name) const {
    return std::make_unique<DummyCatalogEntry>(std::move(name));
}

CatalogEntry* CatalogSet::traverseVersionChainsForTransactionNoLock(Transaction* transaction,
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

CatalogEntry* CatalogSet::getCommittedEntryNoLock(CatalogEntry* entry) const {
    while (entry) {
        if (entry->getTimestamp() < Transaction::START_TRANSACTION_ID) {
            break;
        }
        entry = entry->getPrev();
    }
    return entry;
}

void CatalogSet::dropEntry(Transaction* transaction, const std::string& name) {
    CatalogEntry* entryPtr;
    {
        std::lock_guard lck{mtx};
        entryPtr = dropEntryNoLock(transaction, name);
    }
    KU_ASSERT(entryPtr);
    logEntryForTrx(transaction, *this, *entryPtr);
}

CatalogEntry* CatalogSet::dropEntryNoLock(Transaction* transaction, const std::string& name) {
    // LCOV_EXCL_START
    validateExistNoLock(transaction, name);
    // LCOV_EXCL_STOP
    auto tombstone = createDummyEntryNoLock(name);
    tombstone->setTimestamp(transaction->getID());
    auto tombstonePtr = tombstone.get();
    emplaceNoLock(std::move(tombstone));
    return tombstonePtr->getPrev();
}

void CatalogSet::alterEntry(Transaction* transaction, const binder::BoundAlterInfo& alterInfo) {
    CatalogEntry* createdEntry = nullptr;
    CatalogEntry* entry;
    {
        std::lock_guard lck{mtx};
        // LCOV_EXCL_START
        validateExistNoLock(transaction, alterInfo.tableName);
        // LCOV_EXCL_STOP
        entry = getEntryNoLock(transaction, alterInfo.tableName);
        KU_ASSERT(entry->getType() == CatalogEntryType::NODE_TABLE_ENTRY ||
                  entry->getType() == CatalogEntryType::REL_TABLE_ENTRY ||
                  entry->getType() == CatalogEntryType::REL_GROUP_ENTRY ||
                  entry->getType() == CatalogEntryType::RDF_GRAPH_ENTRY);
        auto tableEntry = ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry);
        auto newEntry = tableEntry->alter(alterInfo);
        newEntry->setTimestamp(transaction->getID());
        if (alterInfo.alterType == AlterType::RENAME_TABLE) {
            // We treat rename table as drop and create.
            dropEntryNoLock(transaction, alterInfo.tableName);
            createdEntry = createEntryNoLock(transaction, std::move(newEntry));
        } else {
            emplaceNoLock(std::move(newEntry));
        }
        tableEntry->setAlterInfo(alterInfo);
    }
    KU_ASSERT(entry);
    logEntryForTrx(transaction, *this, *entry);
    if (createdEntry) {
        logEntryForTrx(transaction, *this, *createdEntry, true /* skip logging to WAL */);
    }
}

CatalogEntrySet CatalogSet::getEntries(Transaction* transaction) {
    CatalogEntrySet result;
    std::lock_guard lck{mtx};
    for (auto& [name, entry] : entries) {
        auto currentEntry = traverseVersionChainsForTransactionNoLock(transaction, entry.get());
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
        case CatalogEntryType::COPY_FUNCTION_ENTRY:
        case CatalogEntryType::TABLE_FUNCTION_ENTRY:
        case CatalogEntryType::GDS_FUNCTION_ENTRY:
            continue;
        default: {
            auto committedEntry = getCommittedEntryNoLock(entry.get());
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
            catalogSet->emplaceNoLock(std::move(entry));
        }
    }
    return catalogSet;
}

// Ideally we should not trigger the following check. Instead, we should throw more informative
// error message at catalog level.
void CatalogSet::validateExistNoLock(Transaction* transaction, const std::string& name) const {
    if (!containsEntryNoLock(transaction, name)) {
        throw CatalogException(stringFormat("{} does not exist in catalog.", name));
    }
}

void CatalogSet::validateNotExistNoLock(Transaction* transaction, const std::string& name) const {
    if (containsEntryNoLock(transaction, name)) {
        throw CatalogException(stringFormat("{} already exists in catalog.", name));
    }
}

} // namespace catalog
} // namespace kuzu
