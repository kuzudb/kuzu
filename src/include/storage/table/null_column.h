#pragma once

#include "storage/table/column.h"

namespace kuzu {
namespace storage {

class NullColumn final : public Column {
public:
    NullColumn(const std::string& name, FileHandle* dataFH, MemoryManager* mm,
        ShadowFile* shadowFile, bool enableCompression);
};

} // namespace storage
} // namespace kuzu
