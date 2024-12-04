#include "binder/binder.h"
#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_sequence.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_create_type.h"
#include "binder/ddl/bound_drop.h"
#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "common/types/types.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "main/client_context.h"
#include "parser/ddl/alter.h"
#include "parser/ddl/create_sequence.h"
#include "parser/ddl/create_table.h"
#include "parser/ddl/create_table_info.h"
#include "parser/ddl/create_type.h"
#include "parser/ddl/drop.h"
#include "parser/expression/parsed_literal_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

static void validatePropertyName(const std::vector<PropertyDefinition>& definitions) {
    common::case_insensitve_set_t nameSet;
    for (auto& definition : definitions) {
        if (nameSet.contains(definition.getName())) {
            throw BinderException(stringFormat(
                "Duplicated column name: {}, column name must be unique.", definition.getName()));
        }
        if (Binder::reservedInColumnName(definition.getName())) {
            throw BinderException(
                stringFormat("{} is a reserved property name.", definition.getName()));
        }
        nameSet.insert(definition.getName());
    }
}

static void validateSerialNoDefault(const Expression& expr) {
    if (!ExpressionUtil::isNullLiteral(expr)) {
        throw BinderException("No DEFAULT value should be set for SERIAL columns");
    }
}

std::vector<PropertyDefinition> Binder::bindPropertyDefinitions(
    const std::vector<ParsedPropertyDefinition>& parsedDefinitions, const std::string& tableName) {
    std::vector<PropertyDefinition> definitions;
    for (auto& parsedDefinition : parsedDefinitions) {
        auto type = LogicalType::convertFromString(parsedDefinition.getType(), clientContext);
        auto expr = parsedDefinition.defaultExpr->copy();
        // This will check the type correctness of the default value expression
        auto boundExpr = expressionBinder.implicitCastIfNecessary(
            expressionBinder.bindExpression(*parsedDefinition.defaultExpr), type);
        if (type.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            validateSerialNoDefault(*boundExpr);
            expr = ParsedExpressionUtils::getSerialDefaultExpr(
                Catalog::genSerialName(tableName, parsedDefinition.getName()));
        }
        auto columnDefinition = ColumnDefinition(parsedDefinition.getName(), std::move(type));
        definitions.emplace_back(std::move(columnDefinition), std::move(expr));
    }
    validatePropertyName(definitions);
    return definitions;
}

static void validateNotUDT(const common::LogicalType& type) {
    if (!type.isInternalType()) {
        throw BinderException(ExceptionMessage::invalidPKType(type.toString()));
    }
}

static void validatePrimaryKey(const std::string& pkColName,
    const std::vector<PropertyDefinition>& definitions) {
    uint32_t primaryKeyIdx = UINT32_MAX;
    for (auto i = 0u; i < definitions.size(); i++) {
        if (definitions[i].getName() == pkColName) {
            primaryKeyIdx = i;
        }
    }
    if (primaryKeyIdx == UINT32_MAX) {
        throw BinderException(
            "Primary key " + pkColName + " does not match any of the predefined node properties.");
    }
    const auto& pkType = definitions[primaryKeyIdx].getType();
    validateNotUDT(pkType);
    switch (pkType.getPhysicalType()) {
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT128:
    case PhysicalTypeID::STRING:
    case PhysicalTypeID::FLOAT:
    case PhysicalTypeID::DOUBLE:
        break;
    default:
        throw BinderException(ExceptionMessage::invalidPKType(pkType.toString()));
    }
}

BoundCreateTableInfo Binder::bindCreateTableInfo(const parser::CreateTableInfo* info) {
    switch (info->tableType) {
    case TableType::NODE: {
        return bindCreateNodeTableInfo(info);
    }
    case TableType::REL: {
        return bindCreateRelTableInfo(info);
    }
    case TableType::REL_GROUP: {
        return bindCreateRelTableGroupInfo(info);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

BoundCreateTableInfo Binder::bindCreateNodeTableInfo(const CreateTableInfo* info) {
    auto propertyDefinitions = bindPropertyDefinitions(info->propertyDefinitions, info->tableName);
    auto& extraInfo = info->extraInfo->constCast<ExtraCreateNodeTableInfo>();
    validatePrimaryKey(extraInfo.pKName, propertyDefinitions);
    auto boundExtraInfo = std::make_unique<BoundExtraCreateNodeTableInfo>(extraInfo.pKName,
        std::move(propertyDefinitions));
    return BoundCreateTableInfo(TableType::NODE, info->tableName, info->onConflict,
        std::move(boundExtraInfo));
}

BoundCreateTableInfo Binder::bindCreateRelTableInfo(const CreateTableInfo* info) {
    std::vector<PropertyDefinition> propertyDefinitions;
    propertyDefinitions.emplace_back(
        ColumnDefinition(InternalKeyword::ID, LogicalType::INTERNAL_ID()));
    for (auto& definition : bindPropertyDefinitions(info->propertyDefinitions, info->tableName)) {
        propertyDefinitions.push_back(definition.copy());
    }
    auto& extraInfo = info->extraInfo->constCast<ExtraCreateRelTableInfo>();
    auto srcMultiplicity = RelMultiplicityUtils::getFwd(extraInfo.relMultiplicity);
    auto dstMultiplicity = RelMultiplicityUtils::getBwd(extraInfo.relMultiplicity);
    auto srcTableID = bindTableID(extraInfo.srcTableName);
    validateTableType(srcTableID, TableType::NODE);
    auto dstTableID = bindTableID(extraInfo.dstTableName);
    validateTableType(dstTableID, TableType::NODE);
    auto boundExtraInfo = std::make_unique<BoundExtraCreateRelTableInfo>(srcMultiplicity,
        dstMultiplicity, srcTableID, dstTableID, std::move(propertyDefinitions));
    return BoundCreateTableInfo(TableType::REL, info->tableName, info->onConflict,
        std::move(boundExtraInfo));
}

static std::string getRelGroupTableName(const std::string& relGroupName,
    const std::string& srcTableName, const std::string& dstTableName) {
    return relGroupName + "_" + srcTableName + "_" + dstTableName;
}

BoundCreateTableInfo Binder::bindCreateRelTableGroupInfo(const CreateTableInfo* info) {
    auto relGroupName = info->tableName;
    auto& extraInfo = info->extraInfo->constCast<ExtraCreateRelTableGroupInfo>();
    auto relMultiplicity = extraInfo.relMultiplicity;
    std::vector<BoundCreateTableInfo> boundCreateRelTableInfos;
    auto relCreateInfo = std::make_unique<CreateTableInfo>(TableType::REL, "", info->onConflict);
    relCreateInfo->propertyDefinitions = copyVector(info->propertyDefinitions);
    for (auto& [srcTableName, dstTableName] : extraInfo.srcDstTablePairs) {
        relCreateInfo->tableName = getRelGroupTableName(relGroupName, srcTableName, dstTableName);
        relCreateInfo->extraInfo =
            std::make_unique<ExtraCreateRelTableInfo>(relMultiplicity, srcTableName, dstTableName);
        boundCreateRelTableInfos.push_back(bindCreateRelTableInfo(relCreateInfo.get()));
    }
    auto boundExtraInfo =
        std::make_unique<BoundExtraCreateRelTableGroupInfo>(std::move(boundCreateRelTableInfos));
    return BoundCreateTableInfo(TableType::REL_GROUP, info->tableName, info->onConflict,
        std::move(boundExtraInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCreateTable(const Statement& statement) {
    auto createTable = statement.constPtrCast<CreateTable>();
    auto tableName = createTable->getInfo()->tableName;
    switch (createTable->getInfo()->onConflict) {
    case common::ConflictAction::ON_CONFLICT_THROW: {
        if (clientContext->getCatalog()->containsTable(clientContext->getTx(), tableName)) {
            throw BinderException(tableName + " already exists in catalog.");
        }
    } break;
    default:
        break;
    }
    auto boundCreateInfo = bindCreateTableInfo(createTable->getInfo());
    return std::make_unique<BoundCreateTable>(std::move(boundCreateInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCreateType(const Statement& statement) {
    auto createType = statement.constPtrCast<CreateType>();
    auto name = createType->getName();
    LogicalType type = LogicalType::convertFromString(createType->getDataType(), clientContext);
    if (clientContext->getCatalog()->containsType(clientContext->getTx(), name)) {
        throw BinderException{common::stringFormat("Duplicated type name: {}.", name)};
    }
    return std::make_unique<BoundCreateType>(std::move(name), std::move(type));
}

std::unique_ptr<BoundStatement> Binder::bindCreateSequence(const Statement& statement) {
    auto& createSequence = statement.constCast<CreateSequence>();
    auto info = createSequence.getInfo();
    auto sequenceName = info.sequenceName;
    int64_t startWith = 0;
    int64_t increment = 0;
    int64_t minValue = 0;
    int64_t maxValue = 0;
    switch (info.onConflict) {
    case common::ConflictAction::ON_CONFLICT_THROW: {
        if (clientContext->getCatalog()->containsSequence(clientContext->getTx(), sequenceName)) {
            throw BinderException(sequenceName + " already exists in catalog.");
        }
    } break;
    default:
        break;
    }
    auto literal = ku_string_t{info.increment.c_str(), info.increment.length()};
    if (!function::CastString::tryCast(literal, increment)) {
        throw BinderException("Out of bounds: SEQUENCE accepts integers within INT64.");
    }
    if (increment == 0) {
        throw BinderException("INCREMENT must be non-zero.");
    }

    if (info.minValue == "") {
        minValue = increment > 0 ? 1 : std::numeric_limits<int64_t>::min();
    } else {
        literal = ku_string_t{info.minValue.c_str(), info.minValue.length()};
        if (!function::CastString::tryCast(literal, minValue)) {
            throw BinderException("Out of bounds: SEQUENCE accepts integers within INT64.");
        }
    }
    if (info.maxValue == "") {
        maxValue = increment > 0 ? std::numeric_limits<int64_t>::max() : -1;
    } else {
        literal = ku_string_t{info.maxValue.c_str(), info.maxValue.length()};
        if (!function::CastString::tryCast(literal, maxValue)) {
            throw BinderException("Out of bounds: SEQUENCE accepts integers within INT64.");
        }
    }
    if (info.startWith == "") {
        startWith = increment > 0 ? minValue : maxValue;
    } else {
        literal = ku_string_t{info.startWith.c_str(), info.startWith.length()};
        if (!function::CastString::tryCast(literal, startWith)) {
            throw BinderException("Out of bounds: SEQUENCE accepts integers within INT64.");
        }
    }

    if (maxValue < minValue) {
        throw BinderException("SEQUENCE MAXVALUE should be greater than or equal to MINVALUE.");
    }
    if (startWith < minValue || startWith > maxValue) {
        throw BinderException("SEQUENCE START value should be between MINVALUE and MAXVALUE.");
    }

    auto boundInfo = BoundCreateSequenceInfo(sequenceName, startWith, increment, minValue, maxValue,
        info.cycle, info.onConflict);
    return std::make_unique<BoundCreateSequence>(std::move(boundInfo));
}

void Binder::validateDropTable(const Statement& statement) {
    auto& dropTable = statement.constCast<Drop>();
    auto tableName = dropTable.getDropInfo().name;
    auto catalog = clientContext->getCatalog();
    auto validTable = catalog->containsTable(clientContext->getTx(), tableName);
    if (!validTable) {
        switch (dropTable.getDropInfo().conflictAction) {
        case common::ConflictAction::ON_CONFLICT_THROW: {
            throw BinderException("Table " + tableName + " does not exist.");
        }
        case common::ConflictAction::ON_CONFLICT_DO_NOTHING: {
            return;
        }
        default:
            KU_UNREACHABLE;
        }
    }
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTx(), tableName);
    switch (tableEntry->getTableType()) {
    case TableType::NODE: {
        // Check node table is not referenced by rel table.
        for (auto& relTableEntry : catalog->getRelTableEntries(clientContext->getTx())) {
            if (relTableEntry->isParent(tableEntry->getTableID())) {
                throw BinderException(stringFormat("Cannot delete node table {} because it is "
                                                   "referenced by relationship table {}.",
                    tableEntry->getName(), relTableEntry->getName()));
            }
        }
    } break;
    case TableType::REL: {
        // Check rel table is not referenced by rel group.
        for (auto& relTableGroupEntry : catalog->getRelTableGroupEntries(clientContext->getTx())) {
            if (relTableGroupEntry->isParent(tableEntry->getTableID())) {
                throw BinderException(stringFormat("Cannot delete relationship table {} because it "
                                                   "is referenced by relationship group {}.",
                    tableName, relTableGroupEntry->getName()));
            }
        }
    } break;
    default:
        break;
    }
}

void Binder::validateDropSequence(const parser::Statement& statement) {
    auto& dropSequence = statement.constCast<Drop>();
    if (!clientContext->getCatalog()->containsSequence(clientContext->getTx(),
            dropSequence.getDropInfo().name)) {
        switch (dropSequence.getDropInfo().conflictAction) {
        case common::ConflictAction::ON_CONFLICT_THROW: {
            throw BinderException(common::stringFormat("Sequence {} does not exist.",
                dropSequence.getDropInfo().name));
        }
        case common::ConflictAction::ON_CONFLICT_DO_NOTHING: {
            return;
        }
        default:
            KU_UNREACHABLE;
        }
    }
}

std::unique_ptr<BoundStatement> Binder::bindDrop(const Statement& statement) {
    auto& drop = statement.constCast<Drop>();
    switch (drop.getDropInfo().dropType) {
    case DropType::TABLE: {
        validateDropTable(drop);
    } break;
    case DropType::SEQUENCE: {
        validateDropSequence(drop);
    } break;
    default:
        KU_UNREACHABLE;
    }
    return std::make_unique<BoundDrop>(drop.getDropInfo());
}

std::unique_ptr<BoundStatement> Binder::bindAlter(const Statement& statement) {
    auto& alter = statement.constCast<Alter>();
    switch (alter.getInfo()->type) {
    case AlterType::RENAME_TABLE: {
        return bindRenameTable(statement);
    }
    case AlterType::ADD_PROPERTY: {
        return bindAddProperty(statement);
    }
    case AlterType::DROP_PROPERTY: {
        return bindDropProperty(statement);
    }
    case AlterType::RENAME_PROPERTY: {
        return bindRenameProperty(statement);
    }
    case AlterType::COMMENT: {
        return bindCommentOn(statement);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::unique_ptr<BoundStatement> Binder::bindRenameTable(const Statement& statement) {
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = ku_dynamic_cast<ExtraRenameTableInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto newName = extraInfo->newName;
    validateTableExist(tableName);
    auto catalog = clientContext->getCatalog();
    if (catalog->containsTable(clientContext->getTx(), newName)) {
        throw BinderException("Table: " + newName + " already exists.");
    }
    auto boundExtraInfo = std::make_unique<BoundExtraRenameTableInfo>(newName);
    auto boundInfo = BoundAlterInfo(AlterType::RENAME_TABLE, tableName, std::move(boundExtraInfo),
        info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

using on_conflict_throw_action = std::function<void()>;

static void validateProperty(common::ConflictAction action, on_conflict_throw_action throwAction) {
    switch (action) {
    case common::ConflictAction::ON_CONFLICT_THROW: {
        throwAction();
    } break;
    case common::ConflictAction::ON_CONFLICT_DO_NOTHING:
        break;
    default:
        KU_UNREACHABLE;
    }
}

static void validatePropertyExist(common::ConflictAction action, TableCatalogEntry* tableEntry,
    const std::string& propertyName) {
    validateProperty(action, [&tableEntry, &propertyName]() {
        if (!tableEntry->containsProperty(propertyName)) {
            throw BinderException(
                tableEntry->getName() + " table does not have property " + propertyName + ".");
        }
    });
}

static void validatePropertyNotExist(common::ConflictAction action, TableCatalogEntry* tableEntry,
    const std::string& propertyName) {
    validateProperty(action, [&tableEntry, &propertyName] {
        if (tableEntry->containsProperty(propertyName)) {
            throw BinderException(
                tableEntry->getName() + " table already has property " + propertyName + ".");
        }
    });
}

static void validatePropertyDDLOnTable(TableCatalogEntry* tableEntry,
    const std::string& ddlOperation) {
    switch (tableEntry->getTableType()) {
    case TableType::REL_GROUP: {
        throw BinderException(
            stringFormat("Cannot {} property on table {} with type {}.", ddlOperation,
                tableEntry->getName(), TableTypeUtils::toString(tableEntry->getTableType())));
    }
    default:
        return;
    }
}

std::unique_ptr<BoundStatement> Binder::bindAddProperty(const Statement& statement) {
    auto catalog = clientContext->getCatalog();
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = info->extraInfo->ptrCast<ExtraAddPropertyInfo>();
    auto tableName = info->tableName;
    auto dataType = LogicalType::convertFromString(extraInfo->dataType, clientContext);
    if (extraInfo->defaultValue == nullptr) {
        extraInfo->defaultValue =
            std::make_unique<ParsedLiteralExpression>(Value::createNullValue(dataType), "NULL");
    }
    auto propertyName = extraInfo->propertyName;
    validateTableExist(tableName);
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTx(), tableName);
    validatePropertyDDLOnTable(tableEntry, "add");
    validatePropertyNotExist(info->onConflict, tableEntry, propertyName);
    auto defaultValue = std::move(extraInfo->defaultValue);
    auto boundDefault = expressionBinder.implicitCastIfNecessary(
        expressionBinder.bindExpression(*defaultValue), dataType);
    if (dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        validateSerialNoDefault(*boundDefault);
        defaultValue = ParsedExpressionUtils::getSerialDefaultExpr(
            Catalog::genSerialName(tableName, propertyName));
        boundDefault = expressionBinder.implicitCastIfNecessary(
            expressionBinder.bindExpression(*defaultValue), dataType);
    }
    // Eventually, we want to support non-constant default on rel tables, but it is non-trivial due
    // to FWD/BWD storage
    if (tableEntry->getType() == CatalogEntryType::REL_TABLE_ENTRY &&
        boundDefault->expressionType != ExpressionType::LITERAL) {
        throw BinderException(
            "Cannot set a non-constant default value when adding columns on REL tables.");
    }
    auto columnDefinition = ColumnDefinition(propertyName, dataType.copy());
    auto propertyDefinition =
        PropertyDefinition(std::move(columnDefinition), std::move(defaultValue));
    auto boundExtraInfo = std::make_unique<BoundExtraAddPropertyInfo>(std::move(propertyDefinition),
        std::move(boundDefault));
    auto boundInfo = BoundAlterInfo(AlterType::ADD_PROPERTY, tableName, std::move(boundExtraInfo),
        info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropProperty(const Statement& statement) {
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = info->extraInfo->constPtrCast<ExtraDropPropertyInfo>();
    auto tableName = info->tableName;
    auto propertyName = extraInfo->propertyName;
    validateTableExist(tableName);
    auto catalog = clientContext->getCatalog();
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTx(), tableName);
    validatePropertyDDLOnTable(tableEntry, "drop");
    validatePropertyExist(info->onConflict, tableEntry, propertyName);
    if (tableEntry->getTableType() == TableType::NODE &&
        tableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName() == propertyName) {
        throw BinderException("Cannot drop primary key of a node table.");
    }
    auto boundExtraInfo = std::make_unique<BoundExtraDropPropertyInfo>(propertyName);
    auto boundInfo = BoundAlterInfo(AlterType::DROP_PROPERTY, tableName, std::move(boundExtraInfo),
        info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindRenameProperty(const Statement& statement) {
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = info->extraInfo->constPtrCast<ExtraRenamePropertyInfo>();
    auto tableName = info->tableName;
    auto propertyName = extraInfo->propertyName;
    auto newName = extraInfo->newName;
    validateTableExist(tableName);
    auto catalog = clientContext->getCatalog();
    auto tableSchema = catalog->getTableCatalogEntry(clientContext->getTx(), tableName);
    validatePropertyDDLOnTable(tableSchema, "rename");
    validatePropertyExist(common::ConflictAction::ON_CONFLICT_THROW, tableSchema, propertyName);
    validatePropertyNotExist(common::ConflictAction::ON_CONFLICT_THROW, tableSchema, newName);
    auto boundExtraInfo = std::make_unique<BoundExtraRenamePropertyInfo>(newName, propertyName);
    auto boundInfo = BoundAlterInfo(AlterType::RENAME_PROPERTY, tableName,
        std::move(boundExtraInfo), info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCommentOn(const Statement& statement) {
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = info->extraInfo->constPtrCast<ExtraCommentInfo>();
    auto tableName = info->tableName;
    auto comment = extraInfo->comment;
    validateTableExist(tableName);
    auto boundExtraInfo = std::make_unique<BoundExtraCommentInfo>(comment);
    auto boundInfo =
        BoundAlterInfo(AlterType::COMMENT, tableName, std::move(boundExtraInfo), info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

} // namespace binder
} // namespace kuzu
