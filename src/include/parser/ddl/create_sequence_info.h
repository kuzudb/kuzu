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
    std::string startWith = "";
    std::string increment = "1";
    std::string minValue = "";
    std::string maxValue = "";
    bool cycle = false;

    explicit CreateSequenceInfo(std::string sequenceName) : sequenceName{std::move(sequenceName)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(CreateSequenceInfo);

private:
    CreateSequenceInfo(const CreateSequenceInfo& other)
        : sequenceName{other.sequenceName}, startWith{other.startWith}, increment{other.increment},
          minValue{other.minValue}, maxValue{other.maxValue}, cycle{other.cycle} {}
};

} // namespace parser
} // namespace kuzu
