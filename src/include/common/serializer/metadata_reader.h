#pragma once

#include "common/serializer/reader.h"

namespace kuzu::storage {
class FileHandle;
}
namespace kuzu {
namespace common {

class MetadataReader final : public Reader {
public:
    explicit MetadataReader(storage::FileHandle* fileHandle);

    void read(uint8_t* data, uint64_t size) override;

    bool finished() override;

private:
    storage::FileHandle* fileHandle;
};

} // namespace common
} // namespace kuzu
