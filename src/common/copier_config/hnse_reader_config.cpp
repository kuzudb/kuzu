#include "common/copier_config/hnsw_reader_config.h"

namespace kuzu {
namespace common {

HnswReaderConfig HnswReaderConfig::construct(
    const std::unordered_map<std::string, common::Value>& options) {
    HnswReaderConfig config;
    for (const auto& [key, value] : options) {
        if (key == "M") {
            config.M = value.getValue<int64_t>();
        } else if (key == "efConstruction") {
            config.efConstruction = value.getValue<int64_t>();
        } else if (key == "efSearch") {
            config.efSearch = value.getValue<int64_t>();
        } else if (key == "alpha") {
            config.alpha = value.getValue<float>();
        } else {
            KU_ASSERT(false);
        }
    }
    return config;
}

} // namespace common
} // namespace kuzu
