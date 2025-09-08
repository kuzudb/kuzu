#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "extension/transformer_extension.h"
#include "statement.h"

namespace kuzu {
namespace main {
class ClientContext;
}
namespace parser {

class Parser {

public:
    KUZU_API static std::vector<std::shared_ptr<Statement>> parseQuery(std::string_view query,
        std::vector<extension::TransformerExtension*> transformerExtensions = {});
};

} // namespace parser
} // namespace kuzu
