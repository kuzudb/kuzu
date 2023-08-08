#include "parser/ddl/add_property.h"
#include "parser/ddl/create_node_clause.h"
#include "parser/ddl/create_rel_clause.h"
#include "parser/ddl/drop_property.h"
#include "parser/ddl/drop_table.h"
#include "parser/ddl/rename_property.h"
#include "parser/ddl/rename_table.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformDDL(CypherParser::KU_DDLContext& ctx) {
    if (ctx.kU_CreateNode()) {
        return transformCreateNodeClause(*ctx.kU_CreateNode());
    } else if (root.oC_Statement()->kU_DDL()->kU_CreateRel()) {
        return transformCreateRelClause(*root.oC_Statement()->kU_DDL()->kU_CreateRel());
    } else if (root.oC_Statement()->kU_DDL()->kU_DropTable()) {
        return transformDropTable(*root.oC_Statement()->kU_DDL()->kU_DropTable());
    } else {
        return transformAlterTable(*root.oC_Statement()->kU_DDL()->kU_AlterTable());
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

std::unique_ptr<Statement> Transformer::transformCreateNodeClause(
    CypherParser::KU_CreateNodeContext& ctx) {
    auto schemaName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyDefinitions = transformPropertyDefinitions(*ctx.kU_PropertyDefinitions());
    auto pkColName =
        ctx.kU_CreateNodeConstraint() ? transformPrimaryKey(*ctx.kU_CreateNodeConstraint()) : "";
    return std::make_unique<CreateNodeTableClause>(
        std::move(schemaName), std::move(propertyDefinitions), pkColName);
}

std::unique_ptr<Statement> Transformer::transformCreateRelClause(
    CypherParser::KU_CreateRelContext& ctx) {
    auto schemaName = transformSchemaName(*ctx.oC_SchemaName(0));
    auto propertyDefinitions = ctx.kU_PropertyDefinitions() ?
                                   transformPropertyDefinitions(*ctx.kU_PropertyDefinitions()) :
                                   std::vector<std::pair<std::string, std::string>>();
    auto relMultiplicity =
        ctx.oC_SymbolicName() ? transformSymbolicName(*ctx.oC_SymbolicName()) : "MANY_MANY";
    return make_unique<CreateRelClause>(std::move(schemaName), std::move(propertyDefinitions),
        relMultiplicity, transformSchemaName(*ctx.oC_SchemaName(1)),
        transformSchemaName(*ctx.oC_SchemaName(2)));
}

std::unique_ptr<Statement> Transformer::transformDropTable(CypherParser::KU_DropTableContext& ctx) {
    return std::make_unique<DropTable>(transformSchemaName(*ctx.oC_SchemaName()));
}

std::unique_ptr<Statement> Transformer::transformRenameTable(
    CypherParser::KU_AlterTableContext& ctx) {
    return std::make_unique<RenameTable>(transformSchemaName(*ctx.oC_SchemaName()),
        transformSchemaName(*ctx.kU_AlterOptions()->kU_RenameTable()->oC_SchemaName()));
}

std::unique_ptr<Statement> Transformer::transformAddProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    return std::make_unique<AddProperty>(transformSchemaName(*ctx.oC_SchemaName()),
        transformPropertyKeyName(*ctx.kU_AlterOptions()->kU_AddProperty()->oC_PropertyKeyName()),
        transformDataType(*ctx.kU_AlterOptions()->kU_AddProperty()->kU_DataType()),
        ctx.kU_AlterOptions()->kU_AddProperty()->oC_Expression() ?
            transformExpression(*ctx.kU_AlterOptions()->kU_AddProperty()->oC_Expression()) :
            std::make_unique<ParsedLiteralExpression>(
                std::make_unique<Value>(Value::createNullValue()), "NULL"));
}

std::unique_ptr<Statement> Transformer::transformDropProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    return std::make_unique<DropProperty>(transformSchemaName(*ctx.oC_SchemaName()),
        transformPropertyKeyName(*ctx.kU_AlterOptions()->kU_DropProperty()->oC_PropertyKeyName()));
}

std::unique_ptr<Statement> Transformer::transformRenameProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    return std::make_unique<RenameProperty>(transformSchemaName(*ctx.oC_SchemaName()),
        transformPropertyKeyName(
            *ctx.kU_AlterOptions()->kU_RenameProperty()->oC_PropertyKeyName()[0]),
        transformPropertyKeyName(
            *ctx.kU_AlterOptions()->kU_RenameProperty()->oC_PropertyKeyName()[1]));
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
