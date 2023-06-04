#include "binder/binder.h"
#include "binder/ddl/bound_add_property.h"
#include "binder/ddl/bound_create_node_clause.h"
#include "binder/ddl/bound_create_rel_clause.h"
#include "binder/ddl/bound_drop_property.h"
#include "binder/ddl/bound_drop_table.h"
#include "binder/ddl/bound_rename_property.h"
#include "binder/ddl/bound_rename_table.h"
#include "common/string_utils.h"
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

std::unique_ptr<BoundStatement> Binder::bindCreateNodeTableClause(
    const parser::Statement& statement) {
    auto& createNodeTableClause = (parser::CreateNodeTableClause&)statement;
    auto tableName = createNodeTableClause.getTableName();
    if (catalog.getReadOnlyVersion()->containTable(tableName)) {
        throw BinderException("Node " + tableName + " already exists.");
    }
    auto boundProperties = bindProperties(createNodeTableClause.getPropertyNameDataTypes());
    auto primaryKeyIdx = bindPrimaryKey(
        createNodeTableClause.getPKColName(), createNodeTableClause.getPropertyNameDataTypes());
    for (auto i = 0u; i < boundProperties.size(); ++i) {
        if (boundProperties[i].dataType.getLogicalTypeID() == LogicalTypeID::SERIAL &&
            primaryKeyIdx != i) {
            throw BinderException("Serial property in node table must be the primary key.");
        }
    }
    return make_unique<BoundCreateNodeClause>(tableName, std::move(boundProperties), primaryKeyIdx);
}

std::unique_ptr<BoundStatement> Binder::bindCreateRelTableClause(
    const parser::Statement& statement) {
    auto& createRelClause = (CreateRelClause&)statement;
    auto tableName = createRelClause.getTableName();
    if (catalog.getReadOnlyVersion()->containTable(tableName)) {
        throw BinderException("Rel " + tableName + " already exists.");
    }
    auto boundProperties = bindProperties(createRelClause.getPropertyNameDataTypes());
    for (auto& boundProperty : boundProperties) {
        if (boundProperty.dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            throw BinderException("Serial property is not supported in rel table.");
        }
    }
    auto relMultiplicity = getRelMultiplicityFromString(createRelClause.getRelMultiplicity());
    return make_unique<BoundCreateRelClause>(tableName, std::move(boundProperties), relMultiplicity,
        bindNodeTableID(createRelClause.getSrcTableName()),
        bindNodeTableID(createRelClause.getDstTableName()));
}

std::unique_ptr<BoundStatement> Binder::bindDropTableClause(const parser::Statement& statement) {
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

std::unique_ptr<BoundStatement> Binder::bindRenameTableClause(const parser::Statement& statement) {
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

std::unique_ptr<BoundStatement> Binder::bindAddPropertyClause(const parser::Statement& statement) {
    auto& addProperty = (AddProperty&)statement;
    auto tableName = addProperty.getTableName();
    validateTableExist(catalog, tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto dataType = bindDataType(addProperty.getDataType());
    if (catalogContent->getTableSchema(tableID)->containProperty(addProperty.getPropertyName())) {
        throw BinderException("Property: " + addProperty.getPropertyName() + " already exists.");
    }
    if (dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        throw BinderException("Serial property in node table must be the primary key.");
    }
    auto defaultVal = ExpressionBinder::implicitCastIfNecessary(
        expressionBinder.bindExpression(*addProperty.getDefaultValue()), dataType);
    return make_unique<BoundAddProperty>(
        tableID, addProperty.getPropertyName(), dataType, defaultVal, tableName);
}

std::unique_ptr<BoundStatement> Binder::bindDropPropertyClause(const parser::Statement& statement) {
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

std::unique_ptr<BoundStatement> Binder::bindRenamePropertyClause(
    const parser::Statement& statement) {
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

std::vector<Property> Binder::bindProperties(
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes) {
    std::vector<Property> boundPropertyNameDataTypes;
    std::unordered_set<std::string> boundPropertyNames;
    for (auto& propertyNameDataType : propertyNameDataTypes) {
        if (boundPropertyNames.contains(propertyNameDataType.first)) {
            throw BinderException(StringUtils::string_format(
                "Duplicated column name: {}, column name must be unique.",
                propertyNameDataType.first));
        } else if (TableSchema::isReservedPropertyName(propertyNameDataType.first)) {
            throw BinderException(
                StringUtils::string_format("PropertyName: {} is an internal reserved propertyName.",
                    propertyNameDataType.first));
        }
        StringUtils::toUpper(propertyNameDataType.second);
        auto dataType = bindDataType(propertyNameDataType.second);
        boundPropertyNameDataTypes.emplace_back(propertyNameDataType.first, dataType);
        boundPropertyNames.emplace(propertyNameDataType.first);
    }
    return boundPropertyNameDataTypes;
}

uint32_t Binder::bindPrimaryKey(const std::string& pkColName,
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes) {
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
    // We only support INT64, STRING and SERIAL column as the primary key.
    switch (LogicalTypeUtils::dataTypeFromString(primaryKey.second).getLogicalTypeID()) {
    case common::LogicalTypeID::INT64:
    case common::LogicalTypeID::STRING:
    case common::LogicalTypeID::SERIAL:
        break;
    default:
        throw BinderException(
            "Invalid primary key type: " + primaryKey.second + ". Expected STRING or INT64.");
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

LogicalType Binder::bindDataType(const std::string& dataType) {
    auto boundType = LogicalTypeUtils::dataTypeFromString(dataType);
    if (boundType.getLogicalTypeID() == common::LogicalTypeID::FIXED_LIST) {
        auto validNumericTypes = common::LogicalTypeUtils::getNumericalLogicalTypeIDs();
        auto childType = common::FixedListType::getChildType(&boundType);
        auto numElementsInList = common::FixedListType::getNumElementsInList(&boundType);
        if (find(validNumericTypes.begin(), validNumericTypes.end(),
                childType->getLogicalTypeID()) == validNumericTypes.end()) {
            throw common::BinderException(
                "The child type of a fixed list must be a numeric type. Given: " +
                common::LogicalTypeUtils::dataTypeToString(*childType) + ".");
        }
        if (numElementsInList == 0) {
            // Note: the parser already guarantees that the number of elements is a non-negative
            // number. However, we still need to check whether the number of elements is 0.
            throw common::BinderException(
                "The number of elements in a fixed list must be greater than 0. Given: " +
                std::to_string(numElementsInList) + ".");
        }
        auto numElementsPerPage = storage::PageUtils::getNumElementsInAPage(
            storage::StorageUtils::getDataTypeSize(boundType), true /* hasNull */);
        if (numElementsPerPage == 0) {
            throw common::BinderException(
                StringUtils::string_format("Cannot store a fixed list of size {} in a page.",
                    storage::StorageUtils::getDataTypeSize(boundType)));
        }
    }
    return boundType;
}

} // namespace binder
} // namespace kuzu
