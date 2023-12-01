#pragma once

#include <cstdint>

#include "common/data_chunk/data_chunk.h"

namespace kuzu {
namespace processor {

// TODO(Keenan): Split up this file.
class BaseCSVReader;
class ParsingDriver {
    common::DataChunk& chunk;
    bool rowEmpty;

public:
    explicit ParsingDriver(common::DataChunk& chunk);
    virtual ~ParsingDriver() = default;

    bool done(uint64_t rowNum);
    void addValue(uint64_t rowNum, common::column_id_t columnIdx, std::string_view value);
    bool addRow(uint64_t rowNum, common::column_id_t columnCount);

private:
    virtual bool doneEarly() = 0;
    virtual BaseCSVReader* getReader() = 0;
};

class ParallelCSVReader;

class ParallelParsingDriver : public ParsingDriver {
    ParallelCSVReader* reader;

public:
    ParallelParsingDriver(common::DataChunk& chunk, ParallelCSVReader* reader);
    bool doneEarly() override;

private:
    BaseCSVReader* getReader() override;
};

class SerialCSVReader;

class SerialParsingDriver : public ParsingDriver {
    SerialCSVReader* reader;

public:
    SerialParsingDriver(common::DataChunk& chunk, SerialCSVReader* reader);

    bool doneEarly() override;

private:
    BaseCSVReader* getReader() override;
};

struct SniffCSVNameAndTypeDriver {
    std::vector<std::pair<std::string, common::LogicalType>> columns;

    bool done(uint64_t rowNum);
    void addValue(uint64_t rowNum, common::column_id_t columnIdx, std::string_view value);
    bool addRow(uint64_t rowNum, common::column_id_t columntCount);
};

struct SniffCSVColumnCountDriver {
    bool emptyRow = true;
    uint64_t numColumns = 0;

    bool done(uint64_t rowNum) const;
    void addValue(uint64_t rowNum, common::column_id_t columnIdx, std::string_view value);
    bool addRow(uint64_t rowNum, common::column_id_t columntCount);
};

} // namespace processor
} // namespace kuzu
