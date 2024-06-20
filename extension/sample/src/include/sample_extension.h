#pragma once

#include "common/exception/parser.h"
#include "extension/extension.h"
#include "extension/extension_clause.h"
#include "extension/extension_clause_handler.h"
#include "parser/statement.h"

namespace kuzu {
namespace sample {

class SampleExtension : public extension::Extension {
public:
    static void load(main::ClientContext* context);
};

class SampleClauseHandler : public extension::ExtensionClauseHandler {
public:
    SampleClauseHandler();
};

} // namespace sample
} // namespace kuzu
