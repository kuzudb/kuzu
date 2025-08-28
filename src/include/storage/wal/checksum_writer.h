#pragma once

#include <memory>

#include "common/serializer/serializer.h"
#include "common/serializer/writer.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace storage {
class ChecksumWriter;

struct ChecksumSerializer {
    ChecksumSerializer(std::shared_ptr<common::Writer> outputWriter, MemoryManager& memoryManager);

    std::shared_ptr<ChecksumWriter> writer;
    common::Serializer serializer;
};

// A wrapper on top of another Writer that accumulates serialized data
// Then flushes that data (along with a computed checksum) when the data has completed serializing
class ChecksumWriter : public common::Writer {
public:
    explicit ChecksumWriter(std::shared_ptr<common::Writer> outputWriter,
        MemoryManager& memoryManager);

    void write(const uint8_t* data, uint64_t size) override;
    uint64_t getSize() const override;

    void clear() override;
    void sync() override;

    // Calculate checksum + write the checksum + serialized contents to underlying writer
    void flush() override;

private:
    common::Serializer outputSerializer;
    uint64_t currentEntrySize;
    std::unique_ptr<MemoryBuffer> entryBuffer;
};

} // namespace storage
} // namespace kuzu
