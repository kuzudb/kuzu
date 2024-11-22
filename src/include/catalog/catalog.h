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

    // ----------------------------- Table Schemas ----------------------------
    bool containsTable(const transaction::Transaction* transaction,
        const std::string& tableName) const;

    common::table_id_t getTableID(const transaction::Transaction* transaction,
        const std::string& tableName) const;
    std::vector<common::table_id_t> getNodeTableIDs(
        const transaction::Transaction* transaction) const;
    std::vector<common::table_id_t> getRelTableIDs(
        const transaction::Transaction* transaction) const;

    // TODO: Should remove this.
    std::string getTableName(const transaction::Transaction* transaction,
        common::table_id_t tableID) const;
    TableCatalogEntry* getTableCatalogEntry(const transaction::Transaction* transaction,
        const std::string& tableName) const;
    TableCatalogEntry* getTableCatalogEntry(const transaction::Transaction* transaction,
        common::table_id_t tableID) const;
    std::vector<NodeTableCatalogEntry*> getNodeTableEntries(
        transaction::Transaction* transaction) const;
    std::vector<RelTableCatalogEntry*> getRelTableEntries(
        transaction::Transaction* transaction) const;
    std::vector<RelGroupCatalogEntry*> getRelTableGroupEntries(
        transaction::Transaction* transaction) const;
    std::vector<TableCatalogEntry*> getTableEntries(
        const transaction::Transaction* transaction) const;
    std::vector<TableCatalogEntry*> getTableEntries(const transaction::Transaction* transaction,
        const common::table_id_vector_t& tableIDs) const;
    bool tableInRelGroup(transaction::Transaction* transaction, common::table_id_t tableID) const;
    common::table_id_set_t getFwdRelTableIDs(transaction::Transaction* transaction,
        common::table_id_t nodeTableID) const;
    common::table_id_set_t getBwdRelTableIDs(transaction::Transaction* transaction,
        common::table_id_t nodeTableID) const;

    common::table_id_t createTableSchema(transaction::Transaction* transaction,
        const binder::BoundCreateTableInfo& info);
    void dropTableEntry(transaction::Transaction* transaction, const std::string& name);
    void dropTableEntry(transaction::Transaction* transaction, common::table_id_t tableID);
    void alterTableEntry(transaction::Transaction* transaction, const binder::BoundAlterInfo& info);

    // ----------------------------- Sequences ----------------------------
    bool containsSequence(const transaction::Transaction* transaction,
        const std::string& sequenceName) const;

    common::sequence_id_t getSequenceID(const transaction::Transaction* transaction,
        const std::string& sequenceName) const;
    SequenceCatalogEntry* getSequenceCatalogEntry(const transaction::Transaction* transaction,
        common::sequence_id_t sequenceID) const;
    std::vector<SequenceCatalogEntry*> getSequenceEntries(
        const transaction::Transaction* transaction) const;

    common::sequence_id_t createSequence(transaction::Transaction* transaction,
        const binder::BoundCreateSequenceInfo& info);
    void dropSequence(transaction::Transaction* transaction, const std::string& name);
    void dropSequence(transaction::Transaction* transaction, common::sequence_id_t sequenceID);

    static std::string genSerialName(const std::string& tableName, const std::string& propertyName);

    // ----------------------------- Types ----------------------------
    void createType(transaction::Transaction* transaction, std::string name,
        common::LogicalType type);
    common::LogicalType getType(const transaction::Transaction*, const std::string& name) const;
    bool containsType(const transaction::Transaction* transaction,
        const std::string& typeName) const;

    // ----------------------------- Indexes ----------------------------
    void createIndex(transaction::Transaction* transaction,
        std::unique_ptr<IndexCatalogEntry> indexCatalogEntry);
    IndexCatalogEntry* getIndex(const transaction::Transaction*, common::table_id_t tableID,
        std::string indexName) const;
    bool containsIndex(const transaction::Transaction* transaction, common::table_id_t tableID,
        std::string indexName) const;
    void dropAllIndexes(transaction::Transaction* transaction, common::table_id_t tableID);
    void dropIndex(transaction::Transaction* transaction, common::table_id_t tableID,
        std::string indexName) const;

    // ----------------------------- Functions ----------------------------
    void addFunction(transaction::Transaction* transaction, CatalogEntryType entryType,
        std::string name, function::function_set functionSet);
    void dropFunction(transaction::Transaction* transaction, const std::string& name);
    void addBuiltInFunction(CatalogEntryType entryType, std::string name,
        function::function_set functionSet);
    CatalogSet* getFunctions(transaction::Transaction* transaction) const;
    CatalogEntry* getFunctionEntry(const transaction::Transaction* transaction,
        const std::string& name) const;
    std::vector<FunctionCatalogEntry*> getFunctionEntries(
        const transaction::Transaction* transaction) const;

    bool containsMacro(const transaction::Transaction* transaction,
        const std::string& macroName) const;
    void addScalarMacroFunction(transaction::Transaction* transaction, std::string name,
        std::unique_ptr<function::ScalarMacroFunction> macro);
    function::ScalarMacroFunction* getScalarMacroFunction(
        const transaction::Transaction* transaction, const std::string& name) const;
    std::vector<std::string> getMacroNames(const transaction::Transaction* transaction) const;

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
    // ----------------------------- Functions ----------------------------
    void registerBuiltInFunctions();

    // ----------------------------- Table entries ----------------------------
    template<typename T>
    std::vector<T*> getTableCatalogEntries(transaction::Transaction* transaction,
        CatalogEntryType catalogType) const {
        std::vector<T*> result;
        tables->iterateEntriesOfType(transaction, catalogType, [&](const CatalogEntry* entry) {
            result.push_back(const_cast<T*>(common::ku_dynamic_cast<const T*>(entry)));
        });
        return result;
    }

    std::vector<common::table_id_t> getTableIDs(transaction::Transaction* transaction,
        CatalogEntryType catalogType) const;

    std::unique_ptr<CatalogEntry> createNodeTableEntry(transaction::Transaction* transaction,
        const binder::BoundCreateTableInfo& info) const;
    std::unique_ptr<CatalogEntry> createRelTableEntry(transaction::Transaction* transaction,
        const binder::BoundCreateTableInfo& info) const;
    std::unique_ptr<CatalogEntry> createRelTableGroupEntry(transaction::Transaction* transaction,
        const binder::BoundCreateTableInfo& info);

    // ----------------------------- Sequence entries ----------------------------
    void iterateSequenceCatalogEntries(const transaction::Transaction* transaction,
        const std::function<void(CatalogEntry*)>& func) const {
        for (auto& [_, entry] : sequences->getEntries(transaction)) {
            func(entry);
        }
    }

protected:
    std::unique_ptr<CatalogSet> tables;

private:
    std::unique_ptr<CatalogSet> sequences;
    std::unique_ptr<CatalogSet> functions;
    std::unique_ptr<CatalogSet> types;
    std::unique_ptr<CatalogSet> indexes;
};

} // namespace catalog
} // namespace kuzu
