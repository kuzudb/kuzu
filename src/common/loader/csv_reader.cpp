#include "src/common/loader/include/csv_reader.h"

#include <fstream>

#include "src/common/include/configs.h"

namespace graphflow {
namespace common {

CSVReader::CSVReader(const string &fname, const char tokenSeparator, uint64_t blockId)
    : tokenSeparator(tokenSeparator), readingBlockIdx(CSV_READING_BLOCK_SIZE * blockId),
      readingBlockEndIdx(CSV_READING_BLOCK_SIZE * (blockId + 1)) {
    f = make_unique<ifstream>(fname, ifstream::in);
    auto isBeginningOfLine = false;
    if (0 == readingBlockIdx) {
        isBeginningOfLine = true;
    } else {
        f->seekg(--readingBlockIdx);
        updateNext();
        if (isLineSeparator()) {
            isBeginningOfLine = true;
        }
    }
    if (!isBeginningOfLine) {
        while (!isLineSeparator()) {
            updateNext();
        }
    }
    updateNext();
}

bool CSVReader::hasMore() {
    return readingBlockIdx < readingBlockEndIdx && !f->eof();
}

void CSVReader::updateNext() {
    readingBlockIdx++;
    f->get(next);
}

void CSVReader::skipLine() {
    while (!isLineSeparator()) {
        updateNext();
    }
    updateNext();
}

bool CSVReader::skipTokenIfNULL() {
    auto ret = isTokenSeparator();
    if (ret) {
        skipToken();
    }
    return ret;
}

void CSVReader::skipToken() {
    while (!isTokenSeparator()) {
        updateNext();
    }
    updateNext();
}

unique_ptr<string> CSVReader::getLine() {
    auto val = make_unique<string>();
    while (!isLineSeparator()) {
        val->push_back(next);
        updateNext();
    }
    updateNext();
    return val;
}

uint64_t CSVReader::getNodeID() {
    auto state = 0;
    uint64_t val = 0;
    while (!isTokenSeparator()) {
        if (state == 0) {
            if (next >= 48 && next <= 57) {
                val = (val * 10) + (next - 48);
                state = 1;
            } else {
                throw std::invalid_argument("Cannot read the token as a 8-byte NodeID.");
            }
        } else if (state == 1) {
            if (next >= 48 && next <= 57) {
                val = (val * 10) + (next - 48);
            } else {
                throw std::invalid_argument("Cannot read the token as a 8-byte NodeID.");
            }
        }
        updateNext();
    }
    updateNext();
    if (!(state == 1)) {
        throw std::invalid_argument("Cannot read the token as a 8-byte NodeID.");
    }
    return val;
}

gfInt_t CSVReader::getInteger() {
    auto state = 0;
    gfInt_t val = 0;
    while (!isTokenSeparator()) {
        if (state == 1) {
            if (next >= 48 && next <= 57) {
                if (val > (numeric_limits<gfInt_t>::max() - (next - 48)) / 10) {
                    throw std::invalid_argument("Overflow in Integer.");
                }
                val = (val * 10) + (next - 48);
            } else {
                throw std::invalid_argument("Cannot read the token as Integer.");
            }
        } else if (state == 0) {
            if (next >= 48 && next <= 57) {
                val = (val * 10) + (next - 48);
                state = 1;
            } else if (next == '-') {
                state = 2;
            } else if (next == '+') {
                state = 4;
            } else {
                throw std::invalid_argument("Cannot read the token as Integer.");
            }
        } else if (state == 4) {
            if (next >= 48 && next <= 57) {
                val = next - 48;
                state = 1;
            } else {
                throw std::invalid_argument("Cannot read the token as Integer.");
            }
        } else if (state == 2) {
            if (next >= 48 && next <= 57) {
                val = -(next - 48);
                state = 3;
            } else {
                throw std::invalid_argument("Cannot read the token as Integer.");
            }
        } else if (state == 3) {
            if (next >= 48 && next <= 57) {
                if (val < (numeric_limits<gfInt_t>::min() + (next - 48)) / 10) {
                    throw std::invalid_argument("Underflow in Integer.");
                }
                val = (val * 10) - (next - 48);
            } else {
                throw std::invalid_argument("Cannot read the token as Integer.");
            }
        }
        updateNext();
    }
    updateNext();
    if (!(state == 1 || state == 3)) {
        throw std::invalid_argument("Cannot read the token as Integer.");
    }
    return val;
}

gfDouble_t CSVReader::getDouble() {
    auto state = 0;
    gfDouble_t val = 0.0;
    auto decimalR = 0.0;
    auto factor = 1.0;
    auto power10 = 0;
    auto isExponentNegative = false;
    auto isNegative = false;
    while (!isTokenSeparator()) {
        if (1 == state) {
            if (48 <= next && 57 >= next) {
                val = (val * 10) + (next - 48);
            } else if ('.' == next) {
                state = 3;
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        } else if (4 == state) {
            if (48 <= next && 57 >= next) {
                factor *= 10;
                decimalR = (decimalR * 10) + (next - 48);
            } else if ('e' == next || 'E' == next) {
                state = 5;
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        } else if (0 == state) {
            if (48 <= next && 57 >= next) {
                val = (val * 10) + (next - 48);
                state = 1;
            } else if ('-' == next) {
                isNegative = true;
                state = 2;
            } else if ('+' == next) {
                isNegative = true;
                state = 8;
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        } else if (3 == state) {
            if (48 <= next && 57 >= next) {
                factor *= 10;
                decimalR = (decimalR * 10) + (next - 48);
                state = 4;
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        } else if (2 == state) {
            if (48 <= next && 57 >= next) {
                val = (val * 10) + (next - 48);
                state = 1;
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        } else if (8 == state) {
            if (48 <= next && 57 >= next) {
                val = (val * 10) + (next - 48);
                state = 1;
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        } else if (5 == state) {
            if (48 <= next && 57 >= next) {
                power10 = (power10 * 10) + (next - 48);
                state = 6;
            } else if ('-' == next) {
                isExponentNegative = true;
                state = 7;
            } else if ('+' == next) {
                isExponentNegative = true;
                state = 9;
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        } else if (6 == state) {
            if (48 <= next && 57 >= next) {
                power10 = (power10 * 10) + (next - 48);
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        } else if (7 == state) {
            if (48 <= next && 57 >= next) {
                power10 = (power10 * 10) + (next - 48);
                state = 6;
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        } else if (9 == state) {
            if (48 <= next && 57 >= next) {
                power10 = (power10 * 10) + (next - 48);
                state = 6;
            } else {
                throw std::invalid_argument("Cannot read the token as Double.");
            }
        }
        updateNext();
    }
    updateNext();
    if (!(1 == state || 4 == state || 6 == state)) {
        throw std::invalid_argument("Cannot read the token as Double.");
    }
    if (isExponentNegative) {
        power10 = -power10;
    }
    auto exp = pow(10, power10);
    decimalR /= factor;
    val += decimalR;
    val *= exp;
    val = isNegative ? -val : val;
    return val;
}

gfBool_t CSVReader::getBoolean() {
    char trueTokens[] = {'t', 'r', 'u', 'e'};
    char falseTokens[] = {'f', 'a', 'l', 's', 'e'};
    if (trueTokens[0] == tolower(next)) {
        updateNext();
        auto i = 1;
        while (!isTokenSeparator()) {
            if (i == 4) {
                throw std::invalid_argument("Cannot read the token as Boolean.");
            }
            if (trueTokens[i++] == tolower(next)) {
                updateNext();
            } else {
                throw std::invalid_argument("Cannot read the token as Boolean.");
            }
        }
        updateNext();
        if (i != 4) {
            throw std::invalid_argument("Cannot read the token as Boolean.");
        }
        return 1;
    } else if (falseTokens[0] == tolower(next)) {
        updateNext();
        auto i = 1;
        while (!isTokenSeparator()) {
            if (i == 5) {
                throw std::invalid_argument("Cannot read the token as Boolean.");
            }
            if (falseTokens[i++] == tolower(next)) {
                updateNext();
            } else {
                throw std::invalid_argument("Cannot read the token as Boolean.");
            }
        }
        updateNext();
        if (i != 5) {
            throw std::invalid_argument("Cannot read the token as Boolean.");
        }
        return 2;
    }
}

std::unique_ptr<std::string> CSVReader::getString() {
    auto val = std::make_unique<std::string>();
    while (!isTokenSeparator()) {
        val->push_back(next);
        updateNext();
    }
    updateNext();
    return val;
}

} // namespace common
} // namespace graphflow
