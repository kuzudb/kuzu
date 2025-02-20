#pragma once

#include "catalog/catalog_entry/function_catalog_entry.h"
#include "catalog/catalog_set.h"
#include "common/cast.h"
#include "function/function.h"

namespace kuzu::main {
struct DBConfig;
} // namespace kuzu::main

namespace kuzu {
namespace main {
class AttachedKuzuDatabase;
} // namespace main

namespace binder {
struct BoundAlterInfo;
struct BoundCreateTableInfo;
struct BoundCreateSequenceInfo;
} // namespace binder

namespace common {
class VirtualFileSystem;
} // namespace common

namespace function {
struct ScalarMacroFunction;
} // namespace function

namespace storage {
class WAL;
} // namespace storage

namespace transaction {
class Transaction;
} // namespace transaction

namespace catalog {
class TableCatalogEntry;
class NodeTableCatalogEntry;
class RelTableCatalogEntry;
class RelGroupCatalogEntry;
class FunctionCatalogEntry;
class SequenceCatalogEntry;
class IndexCatalogEntry;

class KUZU_API Catalog {
    friend class main::AttachedKuzuDatabase;

public:
    // This is extended by DuckCatalog and PostgresCatalog.
    Catalog();
    Catalog(const std::string& directory, common::VirtualFileSystem* vfs);
    virtual ~Catalog() = default;

    // ----------------------------- Tables ----------------------------

    // Check if table entry exists.
    bool containsTable(const transaction::Transaction* transaction, const std::string& tableName,
        bool useInternal = true) const;
    // Get table entry with name.
    TableCatalogEntry* getTableCatalogEntry(const transaction::Transaction* transaction,
        const std::string& tableName, bool useInternal = true) const;
    // Get table entry with id.
    TableCatalogEntry* getTableCatalogEntry(const transaction::Transaction* transaction,
        common::table_id_t tableID) const;
    // Get all node table entries.
    std::vector<NodeTableCatalogEntry*> getNodeTableEntries(
        const transaction::Transaction* transaction, bool useInternal = true) const;
    // Get all rel table entries.
    std::vector<RelTableCatalogEntry*> getRelTableEntries(
        const transaction::Transaction* transaction, bool useInternal = true) const;
    // Get all table entries.
    std::vector<TableCatalogEntry*> getTableEntries(const transaction::Transaction* transaction,
        bool useInternal = true) const;

    // Create table catalog entry.
    CatalogEntry* createTableEntry(transaction::Transaction* transaction,
        const binder::BoundCreateTableInfo& info);
    // Drop table entry and all indices within the table.
    void dropTableEntryAndIndex(transaction::Transaction* transaction, const std::string& name);
    // Drop table entry with id.
    void dropTableEntry(transaction::Transaction* transaction, common::table_id_t tableID);
    // Drop table entry.
    void dropTableEntry(transaction::Transaction* transaction, const TableCatalogEntry* entry);
    // Alter table entry.
    void alterTableEntry(transaction::Transaction* transaction, const binder::BoundAlterInfo& info);
    // Alter a rel group entry
    // alterTableEntry() still needs to be called separately for each member of the group
    void alterRelGroupEntry(transaction::Transaction* transaction,
        const binder::BoundAlterInfo& info);

    // ----------------------------- Rel groups ----------------------------

    // Check if rel group entry exists.
    bool containsRelGroup(const transaction::Transaction* transaction,
        const std::string& name) const;
    // Get rel group entry with name.
    RelGroupCatalogEntry* getRelGroupEntry(const transaction::Transaction* transaction,
        const std::string& name) const;
    // Get all rel group entries.
    std::vector<RelGroupCatalogEntry*> getRelGroupEntries(
        const transaction::Transaction* transaction) const;

    // Create rel group entry.
    CatalogEntry* createRelGroupEntry(transaction::Transaction* transaction,
        const binder::BoundCreateTableInfo& info);
    // Drop rel group entry.
    void dropRelGroupEntry(transaction::Transaction* transaction, common::oid_t id);
    // Drop rel group entry.
    void dropRelGroupEntry(transaction::Transaction* transaction,
        const RelGroupCatalogEntry* entry);

    // ----------------------------- Sequences ----------------------------

    // Check if sequence entry exists.
    bool containsSequence(const transaction::Transaction* transaction,
        const std::string& name) const;
    // Get sequence entry with name.
    SequenceCatalogEntry* getSequenceEntry(const transaction::Transaction* transaction,
        const std::string& sequenceName, bool useInternalSeq = true) const;
    // Get sequence entry with id.
    SequenceCatalogEntry* getSequenceEntry(const transaction::Transaction* transaction,
        common::sequence_id_t sequenceID) const;
    // Get all sequence entries.
    std::vector<SequenceCatalogEntry*> getSequenceEntries(
        const transaction::Transaction* transaction) const;

    // Create sequence entry.
    common::sequence_id_t createSequence(transaction::Transaction* transaction,
        const binder::BoundCreateSequenceInfo& info);
    // Drop sequence entry with name.
    void dropSequence(transaction::Transaction* transaction, const std::string& name);
    // Drop sequence entry with id.
    void dropSequence(transaction::Transaction* transaction, common::sequence_id_t sequenceID);

    // ----------------------------- Types ----------------------------

    // Check if type entry exists.
    bool containsType(const transaction::Transaction* transaction, const std::string& name) const;
    // Get type entry with name.
    common::LogicalType getType(const transaction::Transaction*, const std::string& name) const;

    // Create type entry.
    void createType(transaction::Transaction* transaction, std::string name,
        common::LogicalType type);

    // ----------------------------- Indexes ----------------------------

    // Check if index entry exists.
    bool containsIndex(const transaction::Transaction* transaction, common::table_id_t tableID,
        const std::string& indexName) const;
    // Get index entry with name.
    IndexCatalogEntry* getIndex(const transaction::Transaction* transaction,
        common::table_id_t tableID, const std::string& indexName) const;
    // Get all index entries.
    std::vector<IndexCatalogEntry*> getIndexEntries(
        const transaction::Transaction* transaction) const;

    // Create index entry.
    void createIndex(transaction::Transaction* transaction,
        std::unique_ptr<IndexCatalogEntry> indexCatalogEntry);
    // Drop all index entries within a table.
    void dropAllIndexes(transaction::Transaction* transaction, common::table_id_t tableID);
    // Drop index entry with name.
    void dropIndex(transaction::Transaction* transaction, common::table_id_t tableID,
        const std::string& indexName) const;

    // ----------------------------- Functions ----------------------------

    // Check if function exists.
    bool containsFunction(const transaction::Transaction* transaction, const std::string& name,
        bool useInternal = false) const;
    // Get function entry by name.
    // Note we cannot cast to FunctionEntry here because result could also be a MacroEntry.
    CatalogEntry* getFunctionEntry(const transaction::Transaction* transaction,
        const std::string& name, bool useInternal = false) const;
    // Get all function entries.
    std::vector<FunctionCatalogEntry*> getFunctionEntries(
        const transaction::Transaction* transaction) const;

    // Add function with name.
    void addFunction(transaction::Transaction* transaction, CatalogEntryType entryType,
        std::string name, function::function_set functionSet, bool isInternal = false);
    // Drop function with name.
    void dropFunction(transaction::Transaction* transaction, const std::string& name);

    // ----------------------------- Macro ----------------------------

    // Check if macro entry exists.
    bool containsMacro(const transaction::Transaction* transaction,
        const std::string& macroName) const;
    void addScalarMacroFunction(transaction::Transaction* transaction, std::string name,
        std::unique_ptr<function::ScalarMacroFunction> macro);
    function::ScalarMacroFunction* getScalarMacroFunction(
        const transaction::Transaction* transaction, const std::string& name) const;
    std::vector<std::string> getMacroNames(const transaction::Transaction* transaction) const;

    void incrementVersion() { version++; }
    uint64_t getVersion() const { return version; }
    void checkpoint(const std::string& databasePath, common::VirtualFileSystem* fs) const;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }

private:
    // The clientContext needs to be used when reading from a remote filesystem which
    // requires some user-specific configs (e.g. s3 username, password).
    void readFromFile(const std::string& directory, common::VirtualFileSystem* fs,
        common::FileVersionType versionType, main::ClientContext* context = nullptr);
    void saveToFile(const std::string& directory, common::VirtualFileSystem* fs,
        common::FileVersionType versionType) const;

private:
    void initCatalogSets();
    void registerBuiltInFunctions();

    CatalogEntry* createNodeTableEntry(transaction::Transaction* transaction,
        const binder::BoundCreateTableInfo& info);
    CatalogEntry* createRelTableEntry(transaction::Transaction* transaction,
        const binder::BoundCreateTableInfo& info);

    void createSerialSequence(transaction::Transaction* transaction, const TableCatalogEntry* entry,
        bool isInternal);
    void dropSerialSequence(transaction::Transaction* transaction, const TableCatalogEntry* entry);

protected:
    std::unique_ptr<CatalogSet> tables;

private:
    std::unique_ptr<CatalogSet> relGroups;
    std::unique_ptr<CatalogSet> sequences;
    std::unique_ptr<CatalogSet> functions;
    std::unique_ptr<CatalogSet> types;
    std::unique_ptr<CatalogSet> indexes;
    std::unique_ptr<CatalogSet> internalTables;
    std::unique_ptr<CatalogSet> internalSequences;
    std::unique_ptr<CatalogSet> internalFunctions;

    uint64_t version;
};

} // namespace catalog
} // namespace kuzu
