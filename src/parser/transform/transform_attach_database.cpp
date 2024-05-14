#include "parser/attach_database.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/transformer.h"

namespace kuzu {
namespace parser {

AttachOption Transformer::transformAttachOptions(CypherParser::KU_AttachOptionsContext& ctx) {
    AttachOption option;
    for (auto& attachOption : ctx.kU_AttachOption()) {
        auto key = transformSymbolicName(*attachOption->oC_SymbolicName());
        auto value = transformLiteral(*attachOption->oC_Literal())
                         ->constPtrCast<ParsedLiteralExpression>()
                         ->getValue();
        option.options.emplace(std::move(key), std::move(value));
    }
    return option;
}

std::unique_ptr<Statement> Transformer::transformAttachDatabase(
    CypherParser::KU_AttachDatabaseContext& ctx) {
    auto dbPath = transformStringLiteral(*ctx.StringLiteral());
    auto dbAlias = ctx.oC_SchemaName() ? transformSchemaName(*ctx.oC_SchemaName()) : "";
    auto dbType = transformSymbolicName(*ctx.oC_SymbolicName());
    auto attachOption =
        ctx.kU_AttachOptions() ? transformAttachOptions(*ctx.kU_AttachOptions()) : AttachOption{};
    AttachInfo attachInfo{std::move(dbPath), std::move(dbAlias), std::move(dbType),
        std::move(attachOption)};
    return std::make_unique<AttachDatabase>(std::move(attachInfo));
}

} // namespace parser
} // namespace kuzu
