#include "binder/binder.h"
#include "binder/ddl/bound_add_property.h"
#include "binder/ddl/bound_create_node_clause.h"
#include "binder/ddl/bound_create_rel_clause.h"
#include "binder/ddl/bound_drop_property.h"
#include "binder/ddl/bound_drop_table.h"
#include "binder/ddl/bound_rename_property.h"
#include "binder/ddl/bound_rename_table.h"
#include "parser/ddl/add_property.h"
#include "parser/ddl/create_node_clause.h"
#include "parser/ddl/create_rel_clause.h"
#include "parser/ddl/drop_property.h"
#include "parser/ddl/drop_table.h"
#include "parser/ddl/rename_property.h"
#include "parser/ddl/rename_table.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCreateNodeClause(const Statement& statement) {
    auto& createNodeClause = (parser::CreateNodeClause&)statement;
    auto tableName = createNodeClause.getTableName();
    if (catalog.getReadOnlyVersion()->containTable(tableName)) {
        throw BinderException("Node " + tableName + " already exists.");
    }
    auto boundPropertyNameDataTypes =
        bindPropertyNameDataTypes(createNodeClause.getPropertyNameDataTypes());
    auto primaryKeyIdx = bindPrimaryKey(
        createNodeClause.getPKColName(), createNodeClause.getPropertyNameDataTypes());
    return make_unique<BoundCreateNodeClause>(
        tableName, std::move(boundPropertyNameDataTypes), primaryKeyIdx);
}

std::unique_ptr<BoundStatement> Binder::bindCreateRelClause(const Statement& statement) {
    auto& createRelClause = (CreateRelClause&)statement;
    auto tableName = createRelClause.getTableName();
    if (catalog.getReadOnlyVersion()->containTable(tableName)) {
        throw BinderException("Rel " + tableName + " already exists.");
    }
    auto propertyNameDataTypes =
        bindPropertyNameDataTypes(createRelClause.getPropertyNameDataTypes());
    auto relMultiplicity = getRelMultiplicityFromString(createRelClause.getRelMultiplicity());
    return make_unique<BoundCreateRelClause>(tableName, std::move(propertyNameDataTypes),
        relMultiplicity, bindNodeTableID(createRelClause.getSrcTableName()),
        bindNodeTableID(createRelClause.getDstTableName()));
}

std::unique_ptr<BoundStatement> Binder::bindDropTable(const Statement& statement) {
    auto& dropTable = (DropTable&)statement;
    auto tableName = dropTable.getTableName();
    validateTableExist(catalog, tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    if (catalogContent->containNodeTable(tableName)) {
        validateNodeTableHasNoEdge(catalog, tableID);
    }
    return make_unique<BoundDropTable>(tableID, tableName);
}

std::unique_ptr<BoundStatement> Binder::bindRenameTable(const Statement& statement) {
    auto renameTable = (RenameTable&)statement;
    auto tableName = renameTable.getTableName();
    auto catalogContent = catalog.getReadOnlyVersion();
    validateTableExist(catalog, tableName);
    if (catalogContent->containTable(renameTable.getNewName())) {
        throw BinderException("Table: " + renameTable.getNewName() + " already exists.");
    }
    return make_unique<BoundRenameTable>(
        catalogContent->getTableID(tableName), tableName, renameTable.getNewName());
}

std::unique_ptr<BoundStatement> Binder::bindAddProperty(const Statement& statement) {
    auto& addProperty = (AddProperty&)statement;
    auto tableName = addProperty.getTableName();
    validateTableExist(catalog, tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto dataType = Types::dataTypeFromString(addProperty.getDataType());
    if (catalogContent->getTableSchema(tableID)->containProperty(addProperty.getPropertyName())) {
        throw BinderException("Property: " + addProperty.getPropertyName() + " already exists.");
    }
    auto defaultVal = ExpressionBinder::implicitCastIfNecessary(
        expressionBinder.bindExpression(*addProperty.getDefaultValue()), dataType);
    return make_unique<BoundAddProperty>(
        tableID, addProperty.getPropertyName(), dataType, defaultVal, tableName);
}

std::unique_ptr<BoundStatement> Binder::bindDropProperty(const Statement& statement) {
    auto& dropProperty = (DropProperty&)statement;
    auto tableName = dropProperty.getTableName();
    validateTableExist(catalog, tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto isNodeTable = catalogContent->containNodeTable(tableName);
    auto tableID = catalogContent->getTableID(tableName);
    auto propertyID =
        bindPropertyName(catalogContent->getTableSchema(tableID), dropProperty.getPropertyName());
    if (isNodeTable &&
        ((NodeTableSchema*)catalogContent->getTableSchema(tableID))->primaryKeyPropertyID ==
            propertyID) {
        throw BinderException("Cannot drop primary key of a node table.");
    }
    return make_unique<BoundDropProperty>(tableID, propertyID, tableName);
}

std::unique_ptr<BoundStatement> Binder::bindRenameProperty(const Statement& statement) {
    auto& renameProperty = (RenameProperty&)statement;
    auto tableName = renameProperty.getTableName();
    validateTableExist(catalog, tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    auto propertyID = bindPropertyName(tableSchema, renameProperty.getPropertyName());
    if (tableSchema->containProperty(renameProperty.getNewName())) {
        throw BinderException("Property " + renameProperty.getNewName() +
                              " already exists in table: " + tableName + ".");
    }
    return make_unique<BoundRenameProperty>(
        tableID, tableName, propertyID, renameProperty.getNewName());
}

std::vector<PropertyNameDataType> Binder::bindPropertyNameDataTypes(
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes) {
    std::vector<PropertyNameDataType> boundPropertyNameDataTypes;
    std::unordered_set<std::string> boundPropertyNames;
    for (auto& propertyNameDataType : propertyNameDataTypes) {
        if (boundPropertyNames.contains(propertyNameDataType.first)) {
            throw BinderException(StringUtils::string_format(
                "Duplicated column name: %s, column name must be unique.",
                propertyNameDataType.first.c_str()));
        } else if (TableSchema::isReservedPropertyName(propertyNameDataType.first)) {
            throw BinderException(
                StringUtils::string_format("PropertyName: %s is an internal reserved propertyName.",
                    propertyNameDataType.first.c_str()));
        }
        StringUtils::toUpper(propertyNameDataType.second);
        boundPropertyNameDataTypes.emplace_back(
            propertyNameDataType.first, Types::dataTypeFromString(propertyNameDataType.second));
        boundPropertyNames.emplace(propertyNameDataType.first);
    }
    return boundPropertyNameDataTypes;
}

uint32_t Binder::bindPrimaryKey(
    std::string pkColName, std::vector<std::pair<std::string, std::string>> propertyNameDataTypes) {
    uint32_t primaryKeyIdx = UINT32_MAX;
    for (auto i = 0u; i < propertyNameDataTypes.size(); i++) {
        if (propertyNameDataTypes[i].first == pkColName) {
            primaryKeyIdx = i;
        }
    }
    if (primaryKeyIdx == UINT32_MAX) {
        throw BinderException(
            "Primary key " + pkColName + " does not match any of the predefined node properties.");
    }
    auto primaryKey = propertyNameDataTypes[primaryKeyIdx];
    StringUtils::toUpper(primaryKey.second);
    // We only support INT64 and STRING column as the primary key.
    if ((primaryKey.second != std::string("INT64")) &&
        (primaryKey.second != std::string("STRING"))) {
        throw BinderException("Invalid primary key type: " + primaryKey.second + ".");
    }
    return primaryKeyIdx;
}

property_id_t Binder::bindPropertyName(TableSchema* tableSchema, const std::string& propertyName) {
    for (auto& property : tableSchema->properties) {
        if (property.name == propertyName) {
            return property.propertyID;
        }
    }
    throw BinderException(
        tableSchema->tableName + " table doesn't have property: " + propertyName + ".");
}

} // namespace binder
} // namespace kuzu
