#pragma once

#include <memory>
#include <optional>

#include "common/serializer/writer.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace storage {
// A wrapper on top of another Writer that computes checksums for bytes written between startEntry()
// and finishEntry() calls
class ChecksumWriter : public common::Writer {
public:
    explicit ChecksumWriter(std::shared_ptr<Writer> writer, MemoryManager& memoryManager);

    void write(const uint8_t* data, uint64_t size) override;
    uint64_t getSize() const override;

    void clear() override;
    void flush() override;
    void sync() override;

    void startEntry();
    void finishEntry();

private:
    std::shared_ptr<Writer> writer;
    std::unique_ptr<common::Serializer> serializer;

    std::optional<uint64_t> currentEntrySize;
    std::unique_ptr<MemoryBuffer> entryBuffer;
};
} // namespace storage
} // namespace kuzu
