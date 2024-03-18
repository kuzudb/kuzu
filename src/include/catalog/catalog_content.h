#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "catalog_set.h"
#include "common/cast.h"
#include "function/built_in_function_utils.h"
#include "function/scalar_macro_function.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
class VirtualFileSystem;
} // namespace common
namespace catalog {

class CatalogContent {
    friend class Catalog;

public:
    KUZU_API explicit CatalogContent(common::VirtualFileSystem* vfs);

    CatalogContent(const std::string& directory, common::VirtualFileSystem* vfs);

    CatalogContent(std::unique_ptr<CatalogSet> tables,
        std::unordered_map<std::string, common::table_id_t> tableNameToIDMap,
        common::table_id_t nextTableID, std::unique_ptr<CatalogSet> functions,
        common::VirtualFileSystem* vfs)
        : tables{std::move(tables)}, tableNameToIDMap{std::move(tableNameToIDMap)},
          nextTableID{nextTableID}, vfs{vfs}, functions{std::move(functions)} {}

    common::table_id_t getTableID(const std::string& tableName) const;
    CatalogEntry* getTableCatalogEntry(common::table_id_t tableID) const;

    void saveToFile(const std::string& directory, common::FileVersionType dbFileType);
    void readFromFile(const std::string& directory, common::FileVersionType dbFileType);

    std::unique_ptr<CatalogContent> copy() const;

protected:
    common::table_id_t assignNextTableID() { return nextTableID++; }

private:
    // ----------------------------- Functions ----------------------------
    void registerBuiltInFunctions();

    bool containMacro(const std::string& macroName) const {
        return functions->containsEntry(macroName);
    }
    void addFunction(std::string name, function::function_set definitions);

    function::ScalarMacroFunction* getScalarMacroFunction(const std::string& name) const;

    // ----------------------------- Table entries ----------------------------
    uint64_t getNumTables() const { return tables->getEntries().size(); }

    bool containsTable(const std::string& tableName) const;
    bool containsTable(CatalogEntryType catalogType) const;

    std::string getTableName(common::table_id_t tableID) const;

    template<typename T>
    std::vector<T> getTableCatalogEntries(CatalogEntryType catalogType) const {
        std::vector<T> result;
        for (auto& [_, entry] : tables->getEntries()) {
            if (entry->getType() == catalogType) {
                result.push_back(common::ku_dynamic_cast<CatalogEntry*, T>(entry.get()));
            }
        }
        return result;
    }

    std::vector<common::table_id_t> getTableIDs(CatalogEntryType catalogType) const;

    common::table_id_t createNodeTable(const binder::BoundCreateTableInfo& info);
    common::table_id_t createRelTable(const binder::BoundCreateTableInfo& info);
    common::table_id_t createRelGroup(const binder::BoundCreateTableInfo& info);
    common::table_id_t createRDFGraph(const binder::BoundCreateTableInfo& info);
    void dropTable(common::table_id_t tableID);
    void renameTable(common::table_id_t tableID, const std::string& newName);

protected:
    std::unique_ptr<CatalogSet> tables;

private:
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    std::unordered_map<std::string, common::table_id_t> tableNameToIDMap;
    common::table_id_t nextTableID;
    common::VirtualFileSystem* vfs;
    std::unique_ptr<CatalogSet> functions;
};

} // namespace catalog
} // namespace kuzu
