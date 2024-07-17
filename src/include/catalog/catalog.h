#pragma once

#include "catalog/catalog_entry/function_catalog_entry.h"
#include "catalog/catalog_set.h"
#include "common/cast.h"
#include "function/function.h"

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
class RDFGraphCatalogEntry;
class FunctionCatalogEntry;
class SequenceCatalogEntry;

class KUZU_API Catalog {
    friend class main::AttachedKuzuDatabase;

public:
    // This is extended by DuckCatalog and PostgresCatalog.
    Catalog();
    Catalog(std::string directory, common::VirtualFileSystem* vfs);
    virtual ~Catalog() = default;

    // ----------------------------- Table Schemas ----------------------------
    bool containsTable(transaction::Transaction* tx, const std::string& tableName) const;

    common::table_id_t getTableID(transaction::Transaction* tx, const std::string& tableName) const;
    std::vector<common::table_id_t> getNodeTableIDs(transaction::Transaction* tx) const;
    std::vector<common::table_id_t> getRelTableIDs(transaction::Transaction* tx) const;

    // TODO: Should remove this.
    std::string getTableName(transaction::Transaction* tx, common::table_id_t tableID) const;
    TableCatalogEntry* getTableCatalogEntry(transaction::Transaction* tx,
        common::table_id_t tableID) const;
    std::vector<NodeTableCatalogEntry*> getNodeTableEntries(transaction::Transaction* tx) const;
    std::vector<RelTableCatalogEntry*> getRelTableEntries(transaction::Transaction* tx) const;
    std::vector<RelGroupCatalogEntry*> getRelTableGroupEntries(transaction::Transaction* tx) const;
    std::vector<RDFGraphCatalogEntry*> getRdfGraphEntries(transaction::Transaction* tx) const;
    std::vector<TableCatalogEntry*> getTableEntries(transaction::Transaction* tx) const;
    std::vector<TableCatalogEntry*> getTableEntries(transaction::Transaction* tx,
        const common::table_id_vector_t& tableIDs) const;
    bool tableInRDFGraph(transaction::Transaction* tx, common::table_id_t tableID) const;
    bool tableInRelGroup(transaction::Transaction* tx, common::table_id_t tableID) const;
    common::table_id_set_t getFwdRelTableIDs(transaction::Transaction* tx,
        common::table_id_t nodeTableID) const;
    common::table_id_set_t getBwdRelTableIDs(transaction::Transaction* tx,
        common::table_id_t nodeTableID) const;

    common::table_id_t createTableSchema(transaction::Transaction* tx,
        const binder::BoundCreateTableInfo& info);
    void dropTableEntry(transaction::Transaction* tx, std::string name);
    void dropTableEntry(transaction::Transaction* tx, common::table_id_t tableID);
    void alterTableEntry(transaction::Transaction* tx, const binder::BoundAlterInfo& info);

    // ----------------------------- Sequences ----------------------------
    bool containsSequence(transaction::Transaction* tx, const std::string& sequenceName) const;

    common::sequence_id_t getSequenceID(transaction::Transaction* tx,
        const std::string& sequenceName) const;
    SequenceCatalogEntry* getSequenceCatalogEntry(transaction::Transaction* tx,
        common::sequence_id_t sequenceID) const;
    std::vector<SequenceCatalogEntry*> getSequenceEntries(transaction::Transaction* tx) const;

    common::sequence_id_t createSequence(transaction::Transaction* tx,
        const binder::BoundCreateSequenceInfo& info);
    void dropSequence(transaction::Transaction* tx, std::string name);
    void dropSequence(transaction::Transaction* tx, common::sequence_id_t sequenceID);

    static std::string genSerialName(const std::string& tableName, const std::string& propertyName);

    // ----------------------------- Types ----------------------------
    void createType(transaction::Transaction* transaction, std::string name,
        common::LogicalType type);
    common::LogicalType getType(transaction::Transaction*, std::string name);
    bool containsType(transaction::Transaction* transaction, const std::string& typeName);

    // ----------------------------- Functions ----------------------------
    void addFunction(transaction::Transaction* tx, CatalogEntryType entryType, std::string name,
        function::function_set functionSet);
    void dropFunction(transaction::Transaction* tx, const std::string& name);
    void addBuiltInFunction(CatalogEntryType entryType, std::string name,
        function::function_set functionSet);
    CatalogSet* getFunctions(transaction::Transaction* tx) const;
    CatalogEntry* getFunctionEntry(transaction::Transaction* tx, const std::string& name);
    std::vector<FunctionCatalogEntry*> getFunctionEntries(transaction::Transaction* tx) const;

    bool containsMacro(transaction::Transaction* tx, const std::string& macroName) const;
    void addScalarMacroFunction(transaction::Transaction* tx, std::string name,
        std::unique_ptr<function::ScalarMacroFunction> macro);
    function::ScalarMacroFunction* getScalarMacroFunction(transaction::Transaction* tx,
        const std::string& name) const;
    std::vector<std::string> getMacroNames(transaction::Transaction* tx) const;

    void checkpoint(const std::string& databasePath, common::VirtualFileSystem* fs) const;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<Catalog*, TARGET*>(this);
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

    bool containMacro(const std::string& macroName) const;

    // ----------------------------- Table entries ----------------------------
    uint64_t getNumTables(transaction::Transaction* transaction) const {
        return tables->getEntries(transaction).size();
    }

    void iterateCatalogEntries(transaction::Transaction* transaction,
        std::function<void(CatalogEntry*)> func) const {
        for (auto& [_, entry] : tables->getEntries(transaction)) {
            func(entry);
        }
    }
    template<typename T>
    std::vector<T*> getTableCatalogEntries(transaction::Transaction* transaction,
        CatalogEntryType catalogType) const {
        std::vector<T*> result;
        iterateCatalogEntries(transaction, [&](CatalogEntry* entry) {
            if (entry->getType() == catalogType) {
                result.push_back(common::ku_dynamic_cast<CatalogEntry*, T*>(entry));
            }
        });
        return result;
    }

    std::vector<common::table_id_t> getTableIDs(transaction::Transaction* transaction,
        CatalogEntryType catalogType) const;

    void alterRdfChildTableEntries(transaction::Transaction* transaction, CatalogEntry* entry,
        const binder::BoundAlterInfo& info) const;
    std::unique_ptr<CatalogEntry> createNodeTableEntry(transaction::Transaction* transaction,
        common::table_id_t tableID, const binder::BoundCreateTableInfo& info) const;
    std::unique_ptr<CatalogEntry> createRelTableEntry(transaction::Transaction* transaction,
        common::table_id_t tableID, const binder::BoundCreateTableInfo& info) const;
    std::unique_ptr<CatalogEntry> createRelTableGroupEntry(transaction::Transaction* transaction,
        common::table_id_t tableID, const binder::BoundCreateTableInfo& info);
    std::unique_ptr<CatalogEntry> createRdfGraphEntry(transaction::Transaction* transaction,
        common::table_id_t tableID, const binder::BoundCreateTableInfo& info);

    // ----------------------------- Sequence entries ----------------------------
    void iterateSequenceCatalogEntries(transaction::Transaction* transaction,
        std::function<void(CatalogEntry*)> func) const {
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
};

} // namespace catalog
} // namespace kuzu
