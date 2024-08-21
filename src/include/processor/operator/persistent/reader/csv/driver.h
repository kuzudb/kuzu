#pragma once

#include <cstdint>

#include "common/data_chunk/data_chunk.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace main {
class ClientContext;
}

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
    bool addValue(uint64_t rowNum, common::column_id_t columnIdx, std::string_view value);
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
    std::vector<bool> sniffType;
    // if the type isn't declared in the header, sniff it
    SerialCSVReader* reader;

    SniffCSVNameAndTypeDriver(SerialCSVReader* reader,
        const function::ScanTableFuncBindInput* bindInput);
    bool done(uint64_t rowNum) const;
    bool addValue(uint64_t rowNum, common::column_id_t columnIdx, std::string_view value);
    bool addRow(uint64_t rowNum, common::column_id_t columntCount);
};

} // namespace processor
} // namespace kuzu
