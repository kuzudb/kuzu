#pragma once

#include "catalog_entry/catalog_entry.h"
#include "common/case_insensitive_map.h"

namespace kuzu {
namespace binder {
struct BoundAlterInfo;
} // namespace binder

namespace storage {
class UndoBuffer;
} // namespace storage

namespace transaction {
class Transaction;
} // namespace transaction

using CatalogEntrySet = common::case_insensitive_map_t<catalog::CatalogEntry*>;

namespace catalog {
class CatalogSet {
    friend class storage::UndoBuffer;

public:
    //===--------------------------------------------------------------------===//
    // getters & setters
    //===--------------------------------------------------------------------===//
    bool containsEntry(transaction::Transaction* transaction, const std::string& name) const;
    CatalogEntry* getEntry(transaction::Transaction* transaction, const std::string& name);
    KUZU_API void createEntry(transaction::Transaction* transaction,
        std::unique_ptr<CatalogEntry> entry);
    void dropEntry(transaction::Transaction* transaction, const std::string& name);
    void alterEntry(transaction::Transaction* transaction, const binder::BoundAlterInfo& alterInfo);
    CatalogEntrySet getEntries(transaction::Transaction* transaction);

    uint64_t assignNextOID() { return nextOID++; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer serializer) const;
    static std::unique_ptr<CatalogSet> deserialize(common::Deserializer& deserializer);

private:
    void validateExist(transaction::Transaction* transaction, const std::string& name) const;
    void validateNotExist(transaction::Transaction* transaction, const std::string& name) const;

    void emplace(std::unique_ptr<CatalogEntry> entry);
    void erase(const std::string& name);

    std::unique_ptr<CatalogEntry> createDummyEntry(std::string name) const;

    CatalogEntry* traverseVersionChainsForTransaction(transaction::Transaction* transaction,
        CatalogEntry* currentEntry) const;
    CatalogEntry* getCommittedEntry(CatalogEntry* entry) const;
    bool checkWWConflict(transaction::Transaction* transaction, CatalogEntry* entry) const;

private:
    uint64_t nextOID = 0;
    common::case_insensitive_map_t<std::unique_ptr<CatalogEntry>> entries;
};

} // namespace catalog
} // namespace kuzu
