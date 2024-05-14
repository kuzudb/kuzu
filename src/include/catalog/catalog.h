#pragma once

#include "catalog/catalog_set.h"
#include "common/cast.h"

namespace kuzu {
namespace binder {
struct BoundAlterInfo;
struct BoundCreateTableInfo;
struct BoundCreateSequenceInfo;
} // namespace binder

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

class SequenceCatalogEntry;

class Catalog {
public:
    // This is extended by DuckCatalog and PostgresCatalog.
    KUZU_API Catalog();
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

    common::table_id_t createTableSchema(transaction::Transaction* tx,
        const binder::BoundCreateTableInfo& info);
    void dropTableSchema(transaction::Transaction* tx, common::table_id_t tableID);
    void alterTableSchema(transaction::Transaction* tx, const binder::BoundAlterInfo& info);

    void setTableComment(transaction::Transaction* tx, common::table_id_t tableID,
        const std::string& comment) const;

    // ----------------------------- Sequences ----------------------------
    bool containsSequence(transaction::Transaction* tx, const std::string& sequenceName) const;

    common::sequence_id_t getSequenceID(transaction::Transaction* tx,
        const std::string& sequenceName) const;
    SequenceCatalogEntry* getSequenceCatalogEntry(transaction::Transaction* tx,
        common::sequence_id_t sequenceID) const;
    std::vector<SequenceCatalogEntry*> getSequenceEntries(transaction::Transaction* tx) const;

    common::sequence_id_t createSequence(transaction::Transaction* tx,
        const binder::BoundCreateSequenceInfo& info);
    void dropSequence(transaction::Transaction* tx, common::sequence_id_t sequenceID);

    // ----------------------------- Functions ----------------------------
    void addFunction(transaction::Transaction* tx, CatalogEntryType entryType, std::string name,
        function::function_set functionSet);
    void addBuiltInFunction(CatalogEntryType entryType, std::string name,
        function::function_set functionSet);
    CatalogSet* getFunctions(transaction::Transaction* tx) const;
    CatalogEntry* getFunctionEntry(transaction::Transaction* tx, const std::string& name);

    bool containsMacro(transaction::Transaction* tx, const std::string& macroName) const;
    void addScalarMacroFunction(transaction::Transaction* tx, std::string name,
        std::unique_ptr<function::ScalarMacroFunction> macro);
    function::ScalarMacroFunction* getScalarMacroFunction(transaction::Transaction* tx,
        const std::string& name) const;
    std::vector<std::string> getMacroNames(transaction::Transaction* tx) const;

    void prepareCheckpoint(const std::string& databasePath, storage::WAL* wal,
        common::VirtualFileSystem* fs);

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<Catalog*, TARGET*>(this);
    }

private:
    void readFromFile(const std::string& directory, common::VirtualFileSystem* fs,
        common::FileVersionType versionType);
    void saveToFile(const std::string& directory, common::VirtualFileSystem* fs,
        common::FileVersionType versionType);

private:
    // ----------------------------- Functions ----------------------------
    void registerBuiltInFunctions();

    bool containMacro(const std::string& macroName) const {
        return functions->containsEntry(&transaction::DUMMY_READ_TRANSACTION, macroName);
    }

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
};

} // namespace catalog
} // namespace kuzu
