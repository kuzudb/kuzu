#pragma once

#include <string>

#include "common/copy_constructors.h"

namespace kuzu {
namespace parser {

enum class SequenceInfoType {
	START,
	INCREMENT,
	MINVALUE,
	MAXVALUE,
	CYCLE,
};

struct CreateSequenceInfo {
    std::string sequenceName;
    int64_t startWith = 1;
    int64_t increment = 1;
    int64_t minValue = 1;
    int64_t maxValue = INT64_MAX;
    bool cycle = false;

    CreateSequenceInfo(std::string sequenceName)
        : sequenceName{std::move(sequenceName)} {}
    DELETE_COPY_DEFAULT_MOVE(CreateSequenceInfo);
};

} // namespace parser
} // namespace kuzu
