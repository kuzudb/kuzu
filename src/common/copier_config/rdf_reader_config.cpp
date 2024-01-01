#include "common/copier_config/rdf_reader_config.h"

#include "common/constants.h"
#include "common/exception/binder.h"
#include "common/string_utils.h"

namespace kuzu {
namespace common {

RdfReaderConfig RdfReaderConfig::construct(
    const std::unordered_map<std::string, common::Value>& options) {
    auto config = RdfReaderConfig();
    for (auto& op : options) {
        auto name = op.first;
        StringUtils::toUpper(name);
        if (name == RdfConstants::IN_MEMORY_OPTION) {
            if (*op.second.getDataType() != *LogicalType::BOOL()) {
                throw BinderException(
                    stringFormat("The type of option {} must be a boolean.", name));
            }
            config.inMemory = op.second.getValue<bool>();
        } else {
            throw BinderException(stringFormat("Unrecognized csv parsing option: {}.", name));
        }
    }
    return config;
}

} // namespace common
} // namespace kuzu
