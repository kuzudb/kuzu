#include <string>

#include "function/fts_config.h"
#include "main/client_context.h"

namespace kuzu {
namespace fts_extension {

class FTSUtils {

public:
    static void normalizeQuery(std::string& query);

    static std::vector<std::string> stemTerms(std::vector<std::string> terms,
        const FTSConfig& config, main::ClientContext& context, bool isConjunctive);
};

} // namespace fts_extension
} // namespace kuzu
