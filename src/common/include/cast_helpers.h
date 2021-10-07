namespace graphflow {
namespace common {

// This is copied from third_party/fmt/include/fmt/format.h and format-inl.h.
static const char digits[] = "0001020304050607080910111213141516171819"
                             "2021222324252627282930313233343536373839"
                             "4041424344454647484950515253545556575859"
                             "6061626364656667686970717273747576777879"
                             "8081828384858687888990919293949596979899";

//! NumericHelper is a static class that holds helper functions for integers/doubles
class NumericHelper {

public:
    // Formats value in reverse and returns a pointer to the beginning.
    template<class T>
    static char* FormatUnsigned(T value, char* ptr) {
        while (value >= 100) {
            // Integer division is slow so do it for a group of two digits instead
            // of for every digit. The idea comes from the talk by Alexandrescu
            // "Three Optimization Tips for C++".
            auto index = static_cast<unsigned>((value % 100) * 2);
            value /= 100;
            *--ptr = digits[index + 1];
            *--ptr = digits[index];
        }
        if (value < 10) {
            *--ptr = static_cast<char>('0' + value);
            return ptr;
        }
        auto index = static_cast<unsigned>(value * 2);
        *--ptr = digits[index + 1];
        *--ptr = digits[index];
        return ptr;
    }
};

struct DateToStringCast {
    static uint64_t Length(int32_t date[], uint64_t& year_length, bool& add_bc) {
        // format is YYYY-MM-DD with optional (BC) at the end
        // regular length is 10
        uint64_t length = 6;
        year_length = 4;
        add_bc = false;
        if (date[0] <= 0) {
            // add (BC) suffix
            length += 5;
            date[0] = -date[0] + 1;
            add_bc = true;
        }

        // potentially add extra characters depending on length of year
        year_length += date[0] >= 10000;
        year_length += date[0] >= 100000;
        year_length += date[0] >= 1000000;
        year_length += date[0] >= 10000000;
        length += year_length;
        return length;
    }

    static void Format(char* data, int32_t date[], uint64_t year_length, bool add_bc) {
        // now we write the string, first write the year
        auto endptr = data + year_length;
        endptr = NumericHelper::FormatUnsigned(date[0], endptr);
        // add optional leading zeros
        while (endptr > data) {
            *--endptr = '0';
        }
        // now write the month and day
        auto ptr = data + year_length;
        for (int i = 1; i <= 2; i++) {
            ptr[0] = '-';
            if (date[i] < 10) {
                ptr[1] = '0';
                ptr[2] = '0' + date[i];
            } else {
                auto index = static_cast<unsigned>(date[i] * 2);
                ptr[1] = digits[index];
                ptr[2] = digits[index + 1];
            }
            ptr += 3;
        }
        // optionally add BC to the end of the date
        if (add_bc) {
            memcpy(ptr, " (BC)", 5);
        }
    }
};

struct TimeToStringCast {
    // Format microseconds to a buffer of length 6. Returns the number of trailing zeros
    static int32_t FormatMicros(uint32_t microseconds, char micro_buffer[]) {
        char* endptr = micro_buffer + 6;
        endptr = NumericHelper::FormatUnsigned<uint32_t>(microseconds, endptr);
        while (endptr > micro_buffer) {
            *--endptr = '0';
        }
        uint64_t trailing_zeros = 0;
        for (uint64_t i = 5; i > 0; i--) {
            if (micro_buffer[i] != '0') {
                break;
            }
            trailing_zeros++;
        }
        return trailing_zeros;
    }

    static uint64_t Length(int32_t time[], char micro_buffer[]) {
        // format is HH:MM:DD.MS
        // microseconds come after the time with a period separator
        uint64_t length;
        if (time[3] == 0) {
            // no microseconds
            // format is HH:MM:DD
            length = 8;
        } else {
            length = 15;
            // for microseconds, we truncate any trailing zeros (i.e. "90000" becomes ".9")
            // first write the microseconds to the microsecond buffer
            // we write backwards and pad with zeros to the left
            // now we figure out how many digits we need to include by looking backwards
            // and checking how many zeros we encounter
            length -= FormatMicros(time[3], micro_buffer);
        }
        return length;
    }

    static void FormatTwoDigits(char* ptr, int32_t value) {
        if (value < 10) {
            ptr[0] = '0';
            ptr[1] = '0' + value;
        } else {
            auto index = static_cast<unsigned>(value * 2);
            ptr[0] = digits[index];
            ptr[1] = digits[index + 1];
        }
    }

    static void Format(char* data, uint64_t length, int32_t time[], char micro_buffer[]) {
        // first write hour, month and day
        auto ptr = data;
        ptr[2] = ':';
        ptr[5] = ':';
        for (int i = 0; i <= 2; i++) {
            FormatTwoDigits(ptr, time[i]);
            ptr += 3;
        }
        if (length > 8) {
            // write the micro seconds at the end
            data[8] = '.';
            memcpy(data + 9, micro_buffer, length - 9);
        }
    }
};

} // namespace common
} // namespace graphflow
