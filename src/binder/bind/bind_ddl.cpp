#include "binder/binder.h"
#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_sequence.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_create_type.h"
#include "binder/ddl/bound_drop.h"
#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
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

// TODO(Ziyi/Xiyang/Guodong): Ideally this function can be removed, and we can make use of
// `implicitCastIfNecessary` to handle this. However, the current approach doesn't keep casted
// boundExpr, instead the ParsedExpression, which is not casted, in PropertyDefinition. We have to
// change PropertyDefinition to store boundExpr to get rid of this function.
static void tryResolvingDataTypeForDefaultNull(const ParsedPropertyDefinition& parsedDefinition,
    const LogicalType& type) {
    if (parsedDefinition.defaultExpr->getExpressionType() == ExpressionType::LITERAL) {
        auto& literalVal =
            common::ku_dynamic_cast<ParsedLiteralExpression*>(parsedDefinition.defaultExpr.get())
                ->getValueUnsafe();
        if (literalVal.isNull() &&
            literalVal.getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
            literalVal.setDataType(type);
        }
        KU_ASSERT(literalVal.getDataType().getLogicalTypeID() != LogicalTypeID::ANY);
    }
}

std::vector<PropertyDefinition> Binder::bindPropertyDefinitions(
    const std::vector<ParsedPropertyDefinition>& parsedDefinitions, const std::string& tableName) {
    std::vector<PropertyDefinition> definitions;
    for (auto& parsedDefinition : parsedDefinitions) {
        auto type = LogicalType::convertFromString(parsedDefinition.getType(), clientContext);
        // For default null value, we may need to resolve its data type, as its type may not be
        // resolved during parsing and thus may be ANY.
        tryResolvingDataTypeForDefaultNull(parsedDefinition, type);
        auto expr = parsedDefinition.defaultExpr->copy();
        // This will check the type correctness of the default value expression
        auto boundExpr =
            expressionBinder.implicitCastIfNecessary(expressionBinder.bindExpression(*expr), type);
        if (type.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            validateSerialNoDefault(*boundExpr);
            expr = ParsedExpressionUtils::getSerialDefaultExpr(
                SequenceCatalogEntry::genSerialName(tableName, parsedDefinition.getName()));
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
    switch (info->type) {
    case CatalogEntryType::NODE_TABLE_ENTRY: {
        return bindCreateNodeTableInfo(info);
    }
    case CatalogEntryType::REL_TABLE_ENTRY: {
        return bindCreateRelTableInfo(info);
    }
    case CatalogEntryType::REL_GROUP_ENTRY: {
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
    return BoundCreateTableInfo(CatalogEntryType::NODE_TABLE_ENTRY, info->tableName,
        info->onConflict, std::move(boundExtraInfo),
        clientContext->shouldUseInternalCatalogEntry());
}

static void validateNodeTableType(TableCatalogEntry* entry) {
    if (entry->getType() != CatalogEntryType::NODE_TABLE_ENTRY) {
        throw BinderException(stringFormat("{} is not of type NODE.", entry->getName()));
    }
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

    auto parsingOptions = bindParsingOptions(extraInfo.options);
    auto storageDirection = ExtendDirectionUtil::getDefaultExtendDirection();
    if (parsingOptions.contains(TableOptionConstants::REL_STORAGE_DIRECTION_OPTION)) {
        storageDirection = ExtendDirectionUtil::fromString(
            parsingOptions.at(TableOptionConstants::REL_STORAGE_DIRECTION_OPTION).toString());
    }

    auto srcEntry = bindNodeTableEntry(extraInfo.srcTableName);
    validateNodeTableType(srcEntry);
    auto dstEntry = bindNodeTableEntry(extraInfo.dstTableName);
    validateNodeTableType(dstEntry);
    auto boundExtraInfo = std::make_unique<BoundExtraCreateRelTableInfo>(srcMultiplicity,
        dstMultiplicity, storageDirection, srcEntry->getTableID(), dstEntry->getTableID(),
        std::move(propertyDefinitions));
    return BoundCreateTableInfo(CatalogEntryType::REL_TABLE_ENTRY, info->tableName,
        info->onConflict, std::move(boundExtraInfo),
        clientContext->shouldUseInternalCatalogEntry());
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
    auto relCreateInfo =
        std::make_unique<CreateTableInfo>(CatalogEntryType::REL_TABLE_ENTRY, "", info->onConflict);
    relCreateInfo->propertyDefinitions = copyVector(info->propertyDefinitions);
    for (auto& [srcTableName, dstTableName] : extraInfo.srcDstTablePairs) {
        relCreateInfo->tableName = getRelGroupTableName(relGroupName, srcTableName, dstTableName);
        // TODO(Royi) correctly populate options for create rel table group
        options_t options;
        relCreateInfo->extraInfo = std::make_unique<ExtraCreateRelTableInfo>(relMultiplicity,
            std::move(options), srcTableName, dstTableName);
        auto boundInfo = bindCreateRelTableInfo(relCreateInfo.get());
        boundInfo.hasParent = true;
        boundCreateRelTableInfos.push_back(std::move(boundInfo));
    }
    auto boundExtraInfo =
        std::make_unique<BoundExtraCreateRelTableGroupInfo>(std::move(boundCreateRelTableInfos));
    return BoundCreateTableInfo(CatalogEntryType::REL_GROUP_ENTRY, info->tableName,
        info->onConflict, std::move(boundExtraInfo),
        clientContext->shouldUseInternalCatalogEntry());
}

std::unique_ptr<BoundStatement> Binder::bindCreateTable(const Statement& statement) {
    auto createTable = statement.constPtrCast<CreateTable>();
    auto boundCreateInfo = bindCreateTableInfo(createTable->getInfo());
    return std::make_unique<BoundCreateTable>(std::move(boundCreateInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCreateType(const Statement& statement) {
    auto createType = statement.constPtrCast<CreateType>();
    auto name = createType->getName();
    LogicalType type = LogicalType::convertFromString(createType->getDataType(), clientContext);
    if (clientContext->getCatalog()->containsType(clientContext->getTransaction(), name)) {
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
        if (clientContext->getCatalog()->containsSequence(clientContext->getTransaction(),
                sequenceName)) {
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
        info.cycle, info.onConflict, false /* isInternal */);
    return std::make_unique<BoundCreateSequence>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDrop(const Statement& statement) {
    auto& drop = statement.constCast<Drop>();
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
    if (catalog->containsTable(clientContext->getTransaction(), newName)) {
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
    switch (tableEntry->getType()) {
    case CatalogEntryType::REL_GROUP_ENTRY: {
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
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTransaction(), tableName);
    validatePropertyDDLOnTable(tableEntry, "add");
    validatePropertyNotExist(info->onConflict, tableEntry, propertyName);
    auto defaultValue = std::move(extraInfo->defaultValue);
    auto boundDefault = expressionBinder.implicitCastIfNecessary(
        expressionBinder.bindExpression(*defaultValue), dataType);
    if (dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        validateSerialNoDefault(*boundDefault);
        defaultValue = ParsedExpressionUtils::getSerialDefaultExpr(
            SequenceCatalogEntry::genSerialName(tableName, propertyName));
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
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTransaction(), tableName);
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
    auto tableSchema = catalog->getTableCatalogEntry(clientContext->getTransaction(), tableName);
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
