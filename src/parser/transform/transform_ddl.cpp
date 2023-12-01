#include "parser/ddl/alter.h"
#include "parser/ddl/create_table.h"
#include "parser/ddl/drop.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformDDL(CypherParser::KU_DDLContext& ctx) {
    if (ctx.kU_CreateNodeTable()) {
        return transformCreateNodeTable(*ctx.kU_CreateNodeTable());
    } else if (ctx.kU_CreateRelTable()) {
        return transformCreateRelTable(*ctx.kU_CreateRelTable());
    } else if (ctx.kU_CreateRelTableGroup()) {
        return transformCreateRelTableGroup(*ctx.kU_CreateRelTableGroup());
    } else if (ctx.kU_CreateRdfGraph()) {
        return transformCreateRdfGraphClause(*ctx.kU_CreateRdfGraph());
    } else if (ctx.kU_DropTable()) {
        return transformDropTable(*ctx.kU_DropTable());
    } else {
        return transformAlterTable(*ctx.kU_AlterTable());
    }
}

std::unique_ptr<Statement> Transformer::transformAlterTable(
    CypherParser::KU_AlterTableContext& ctx) {
    if (ctx.kU_AlterOptions()->kU_AddProperty()) {
        return transformAddProperty(ctx);
    } else if (ctx.kU_AlterOptions()->kU_DropProperty()) {
        return transformDropProperty(ctx);
    } else if (ctx.kU_AlterOptions()->kU_RenameTable()) {
        return transformRenameTable(ctx);
    } else {
        return transformRenameProperty(ctx);
    }
}

std::unique_ptr<Statement> Transformer::transformCreateNodeTable(
    CypherParser::KU_CreateNodeTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyDefinitions = transformPropertyDefinitions(*ctx.kU_PropertyDefinitions());
    std::string pkName;
    if (ctx.kU_CreateNodeConstraint()) {
        pkName = transformPrimaryKey(*ctx.kU_CreateNodeConstraint());
    }
    auto extraInfo = std::make_unique<ExtraCreateNodeTableInfo>(pkName);
    auto createTableInfo = std::make_unique<CreateTableInfo>(
        TableType::NODE, tableName, propertyDefinitions, std::move(extraInfo));
    return std::make_unique<CreateTable>(tableName, std::move(createTableInfo));
}

std::unique_ptr<Statement> Transformer::transformCreateRelTable(
    CypherParser::KU_CreateRelTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    std::vector<std::pair<std::string, std::string>> propertyDefinitions;
    if (ctx.kU_PropertyDefinitions()) {
        propertyDefinitions = transformPropertyDefinitions(*ctx.kU_PropertyDefinitions());
    }
    std::string relMultiplicity = "MANY_MANY";
    if (ctx.oC_SymbolicName()) {
        relMultiplicity = transformSymbolicName(*ctx.oC_SymbolicName());
    }
    auto srcTableName = transformSchemaName(*ctx.kU_RelTableConnection()->oC_SchemaName(0));
    auto dstTableName = transformSchemaName(*ctx.kU_RelTableConnection()->oC_SchemaName(1));
    auto extraInfo = std::make_unique<ExtraCreateRelTableInfo>(
        relMultiplicity, std::move(srcTableName), std::move(dstTableName));
    auto createTableInfo = std::make_unique<CreateTableInfo>(
        TableType::REL, tableName, propertyDefinitions, std::move(extraInfo));
    return std::make_unique<CreateTable>(tableName, std::move(createTableInfo));
}

std::unique_ptr<Statement> Transformer::transformCreateRelTableGroup(
    CypherParser::KU_CreateRelTableGroupContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    std::vector<std::pair<std::string, std::string>> propertyDefinitions;
    if (ctx.kU_PropertyDefinitions()) {
        propertyDefinitions = transformPropertyDefinitions(*ctx.kU_PropertyDefinitions());
    }
    std::string relMultiplicity = "MANY_MANY";
    if (ctx.oC_SymbolicName()) {
        relMultiplicity = transformSymbolicName(*ctx.oC_SymbolicName());
    }
    std::vector<std::pair<std::string, std::string>> srcDstTablePairs;
    for (auto& connection : ctx.kU_RelTableConnection()) {
        auto srcTableName = transformSchemaName(*connection->oC_SchemaName(0));
        auto dstTableName = transformSchemaName(*connection->oC_SchemaName(1));
        srcDstTablePairs.emplace_back(srcTableName, dstTableName);
    }
    auto extraInfo = std::make_unique<ExtraCreateRelTableGroupInfo>(
        relMultiplicity, std::move(srcDstTablePairs));
    auto createTableInfo = std::make_unique<CreateTableInfo>(
        TableType::REL_GROUP, tableName, propertyDefinitions, std::move(extraInfo));
    return std::make_unique<CreateTable>(tableName, std::move(createTableInfo));
}

std::unique_ptr<Statement> Transformer::transformCreateRdfGraphClause(
    CypherParser::KU_CreateRdfGraphContext& ctx) {
    auto rdfGraphName = transformSchemaName(*ctx.oC_SchemaName());
    auto createTableInfo = std::make_unique<CreateTableInfo>(TableType::RDF, rdfGraphName);
    return std::make_unique<CreateTable>(rdfGraphName, std::move(createTableInfo));
}

std::unique_ptr<Statement> Transformer::transformDropTable(CypherParser::KU_DropTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    return std::make_unique<Drop>(tableName);
}

std::unique_ptr<Statement> Transformer::transformRenameTable(
    CypherParser::KU_AlterTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto newName = transformSchemaName(*ctx.kU_AlterOptions()->kU_RenameTable()->oC_SchemaName());
    auto extraInfo = std::make_unique<ExtraRenameTableInfo>(std::move(newName));
    auto info =
        std::make_unique<AlterInfo>(AlterType::RENAME_TABLE, tableName, std::move(extraInfo));
    return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformAddProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto addPropertyCtx = ctx.kU_AlterOptions()->kU_AddProperty();
    auto propertyName = transformPropertyKeyName(*addPropertyCtx->oC_PropertyKeyName());
    auto dataType = transformDataType(*addPropertyCtx->kU_DataType());
    std::unique_ptr<ParsedExpression> defaultValue;
    if (addPropertyCtx->oC_Expression()) {
        defaultValue = transformExpression(*addPropertyCtx->oC_Expression());
    } else {
        defaultValue =
            std::make_unique<ParsedLiteralExpression>(Value::createNullValue().copy(), "NULL");
    }
    auto extraInfo = std::make_unique<ExtraAddPropertyInfo>(
        std::move(propertyName), std::move(dataType), std::move(defaultValue));
    auto info =
        std::make_unique<AlterInfo>(AlterType::ADD_PROPERTY, tableName, std::move(extraInfo));
    return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformDropProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyName =
        transformPropertyKeyName(*ctx.kU_AlterOptions()->kU_DropProperty()->oC_PropertyKeyName());
    auto extraInfo = std::make_unique<ExtraDropPropertyInfo>(std::move(propertyName));
    auto info =
        std::make_unique<AlterInfo>(AlterType::DROP_PROPERTY, tableName, std::move(extraInfo));
    return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformRenameProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyName = transformPropertyKeyName(
        *ctx.kU_AlterOptions()->kU_RenameProperty()->oC_PropertyKeyName()[0]);
    auto newName = transformPropertyKeyName(
        *ctx.kU_AlterOptions()->kU_RenameProperty()->oC_PropertyKeyName()[1]);
    auto extraInfo = std::make_unique<ExtraRenamePropertyInfo>(propertyName, newName);
    auto info =
        std::make_unique<AlterInfo>(AlterType::RENAME_PROPERTY, tableName, std::move(extraInfo));
    return std::make_unique<Alter>(std::move(info));
}

std::vector<std::pair<std::string, std::string>> Transformer::transformPropertyDefinitions(
    CypherParser::KU_PropertyDefinitionsContext& ctx) {
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes;
    for (auto property : ctx.kU_PropertyDefinition()) {
        propertyNameDataTypes.emplace_back(
            transformPropertyKeyName(*property->oC_PropertyKeyName()),
            transformDataType(*property->kU_DataType()));
    }
    return propertyNameDataTypes;
}

std::string Transformer::transformDataType(CypherParser::KU_DataTypeContext& ctx) {
    return ctx.getText();
}

std::string Transformer::transformPrimaryKey(CypherParser::KU_CreateNodeConstraintContext& ctx) {
    return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

} // namespace parser
} // namespace kuzu
