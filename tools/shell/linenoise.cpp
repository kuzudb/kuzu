/* linenoise.c -- guerrilla line editing library against the idea that a
 * line editing lib needs to be 20,000 lines of C code.
 *
 * You can find the latest source code at:
 *
 *   http://github.com/antirez/linenoise
 *
 * Does a number of crazy assumptions that happen to be true in 99.9999% of
 * the 2010 UNIX computers around.
 *
 * ------------------------------------------------------------------------
 *
 * Copyright (c) 2010-2016, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2013, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *  *  Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  *  Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * ------------------------------------------------------------------------
 *
 * References:
 * - http://invisible-island.net/xterm/ctlseqs/ctlseqs.html
 * - http://www.3waylabs.com/nw/WWW/products/wizcon/vt220.html
 *
 * Todo list:
 * - Filter bogus Ctrl+<char> combinations.
 * - Win32 support
 *
 * Bloat:
 * - History search like Ctrl+r in readline?
 *
 * List of escape sequences used by this program, we do everything just
 * with three sequences. In order to be so cheap we may have some
 * flickering effect with some slow terminal, but the lesser sequences
 * the more compatible.
 *
 * EL (Erase Line)
 *    Sequence: ESC [ n K
 *    Effect: if n is 0 or missing, clear from cursor to end of line
 *    Effect: if n is 1, clear from beginning of line to cursor
 *    Effect: if n is 2, clear entire line
 *
 * CUF (CUrsor Forward)
 *    Sequence: ESC [ n C
 *    Effect: moves cursor forward n chars
 *
 * CUB (CUrsor Backward)
 *    Sequence: ESC [ n D
 *    Effect: moves cursor backward n chars
 *
 * The following is used to get the terminal width if getting
 * the width with the TIOCGWINSZ ioctl fails
 *
 * DSR (Device Status Report)
 *    Sequence: ESC [ 6 n
 *    Effect: reports the current cusor position as ESC [ n ; m R
 *            where n is the row and m is the column
 *
 * When multi line mode is enabled, we also use an additional escape
 * sequence. However multi line editing is disabled by default.
 *
 * CUU (Cursor Up)
 *    Sequence: ESC [ n A
 *    Effect: moves cursor up of n chars.
 *
 * CUD (Cursor Down)
 *    Sequence: ESC [ n B
 *    Effect: moves cursor down of n chars.
 *
 * When linenoiseClearScreen() is called, two additional escape sequences
 * are used in order to clear the screen and position the cursor at home
 * position.
 *
 * CUP (Cursor position)
 *    Sequence: ESC [ H
 *    Effect: moves the cursor to upper left corner
 *
 * ED (Erase display)
 *    Sequence: ESC [ 2 J
 *    Effect: clear the whole screen
 *
 */

/*
 * Windows port adapted from
 * https://github.com/microsoftarchive/redis/blob/3.0/deps/linenoise/linenoise.c
 *
 * Copyright (c) 2006-2015, Salvatore Sanfilippo
 * Modifications copyright (c) Microsoft Open Technologies, Inc.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 * - Redistributions of source code must retain the above copyright notice, this list of conditions
 *   and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of
 *   conditions and the following disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * - Neither the name of Redis nor the names of its contributors may be used to endorse or promote
 *   products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Windows port also adapted from https://github.com/LuaDist-testing/linenoise-windows
 *
 * Copyright (c) 2011-2012 Rob Hoelz <rob@hoelz.ro>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished
 * to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include "linenoise.h"

#include <regex>

#include "cypher_lexer.h"
#include "keywords.h"
#ifndef _WIN32
#include <termios.h>
#include <unistd.h>

#include <sys/ioctl.h>
#endif
#include <ctype.h>
#include <errno.h>
#ifdef _WIN32
#include <io.h>
#include <winsock2.h>
#ifndef STDIN_FILENO
#define STDIN_FILENO (_fileno(stdin))
#endif
#ifndef STDOUT_FILENO
#define STDOUT_FILENO (_fileno(stdout))
#endif
#else
#include <poll.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cstddef>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/string_utils.h"
#include "utf8proc.h"
#include "utf8proc_wrapper.h"

#ifdef _WIN32
#define REDIS_NOTUSED(V) ((void)V)
#endif

using namespace kuzu::utf8proc;

#define LINENOISE_DEFAULT_HISTORY_MAX_LEN 1000
#define LINENOISE_HISTORY_NEXT 0
#define LINENOISE_HISTORY_PREV 1
static const char* unsupported_term[] = {"dumb", "cons25", "emacs", NULL};
static linenoiseCompletionCallback* completionCallback = NULL;
static linenoiseHintsCallback* hintsCallback = NULL;
static linenoiseFreeHintsCallback* freeHintsCallback = NULL;
static linenoiseHighlightCallback* highlightCallback = NULL;
static int highlightEnabled = 1;  /* Enable syntax highlighting by default */
static int errorsEnabled = 1;     /* Enable error highlighting by default */
static int completionEnabled = 1; /* Enable completion by default */

#ifndef _WIN32
static struct termios orig_termios; /* In order to restore at exit.*/
#endif
static int maskmode = 0;          /* Show "***" instead of input. For passwords. */
static int rawmode = 0;           /* For atexit() function to check if restore is needed*/
static int mlmode = 1;            /* Multi line mode. Default is multiline line. */
static int search_mlmode = 1;     /* Marks if multiline is used for search*/
static int atexit_registered = 0; /* Register atexit just 1 time. */
static int history_max_len = LINENOISE_DEFAULT_HISTORY_MAX_LEN;
static int history_len = 0;
static char** history = NULL;

struct searchMatch {
    size_t history_index;
    size_t match_start;
    size_t match_end;
};

struct Completion {
    std::string completion;
    uint64_t cursor_pos;
};

struct TabCompletion {
    std::vector<Completion> completions;
};

/* The linenoiseState structure represents the state during line editing.
 * We pass this state to functions implementing specific editing
 * functionalities. */
struct linenoiseState {
    int ifd;                    /* Terminal stdin file descriptor. */
    int ofd;                    /* Terminal stdout file descriptor. */
    char* buf;                  /* Edited line buffer. */
    size_t buflen;              /* Edited line buffer size. */
    const char* prompt;         /* Prompt to display. */
    const char* continuePrompt; /* Prompt to display when continuing a line */
    const char*
        selectedContinuePrompt; /* Prompt to display when continuing a line and text is selected */
    size_t plen;                /* Prompt length. */
    size_t pos;                 /* Current cursor position. */
    size_t oldpos;              /* Previous refresh cursor position. */
    size_t len;                 /* Current edited line length. */
    size_t totalUTF8Chars;      /* Number of utf-8 chars in buffer. */
    size_t cols;                /* Number of columns in terminal. */
    size_t rows;                /* Number of rows in terminal. */
    size_t maxrows;             /* Maximum num of rows used so far (multiline mode) */
    size_t y_scroll;            /* The y scroll position (multiline mode) */
    int history_index;          /* The history index we are currently editing. */
    bool search;                /* Whether or not we are searching our history */
    bool render;                /* Whether or not to re-render */
    bool clear_screen;          /* Whether we are clearing the screen */
    bool hasMoreData; /* Whether or not there is more data available in the buffer (copy+paste)*/
    bool continuePromptActive; /* Whether or not the continue prompt is active */
    bool insert;               /* Whether or not the last action was inserting a new character */
    std::string search_buf;    //! The search buffer
    std::vector<searchMatch> search_matches; //! The set of search matches in our history
    std::string prev_search_match;           //! The previous search match
    int prev_search_match_history_index;     //! The previous search match history index
    size_t search_index;                     //! The current match index
    size_t search_maxrows; //! The maximum number of rows used so far in search mode
    size_t search_oldpos;  //! The previous refresh cursor position in search mode
};

enum KEY_ACTION {
    KEY_NULL = 0, /* NULL */
    CTRL_A = 1,   /* Ctrl+a */
    CTRL_B = 2,   /* Ctrl-b */
    CTRL_C = 3,   /* Ctrl-c */
    CTRL_D = 4,   /* Ctrl-d */
    CTRL_E = 5,   /* Ctrl-e */
    CTRL_F = 6,   /* Ctrl-f */
    CTRL_G = 7,   /* Ctrl-g */
    CTRL_H = 8,   /* Ctrl-h */
    TAB = 9,      /* Tab */
    CTRL_K = 11,  /* Ctrl+k */
    CTRL_L = 12,  /* Ctrl+l */
    ENTER = 13,   /* Enter */
    CTRL_N = 14,  /* Ctrl-n */
    CTRL_P = 16,  /* Ctrl-p */
    CTRL_R = 18,  /* Ctrl-r */
    CTRL_S = 19,
    CTRL_T = 20,    /* Ctrl-t */
    CTRL_U = 21,    /* Ctrl+u */
    CTRL_W = 23,    /* Ctrl+w */
    ESC = 27,       /* Escape */
    BACKSPACE = 127 /* Backspace */
};

enum class EscapeSequence {
    INVALID = 0,
    UNKNOWN = 1,
    CTRL_MOVE_BACKWARDS,
    CTRL_MOVE_FORWARDS,
    HOME,
    END,
    UP,
    DOWN,
    RIGHT,
    LEFT,
    DEL,
    SHIFT_TAB,
    ESCAPE,
    ALT_A,
    ALT_B,
    ALT_C,
    ALT_D,
    ALT_E,
    ALT_F,
    ALT_G,
    ALT_H,
    ALT_I,
    ALT_J,
    ALT_K,
    ALT_L,
    ALT_M,
    ALT_N,
    ALT_O,
    ALT_P,
    ALT_Q,
    ALT_R,
    ALT_S,
    ALT_T,
    ALT_U,
    ALT_V,
    ALT_W,
    ALT_X,
    ALT_Y,
    ALT_Z,
    ALT_BACKSPACE,
    ALT_LEFT_ARROW,
    ALT_RIGHT_ARROW,
    ALT_BACKSLASH,
};

static void linenoiseAtExit(void);
int linenoiseHistoryAdd(const char* line);
static void refreshLine(struct linenoiseState* l);
void linenoiseEditHistoryNext(struct linenoiseState* l, int dir);

std::string oldInput = "";
bool inputLeft;

/* Debugging macro. */
#if 0
FILE *lndebug_fp = NULL;
#define lndebug(...)                                                                               \
    do {                                                                                           \
        if (lndebug_fp == NULL) {                                                                  \
            lndebug_fp = fopen("/tmp/lndebug.txt", "a");                                           \
            fprintf(lndebug_fp, "[%d %d %d] p: %d, rows: %d, rpos: %d, max: %d, oldmax: %d\n",     \
                (int)l->len, (int)l->pos, (int)l->oldpos, plen, rows, rpos, (int)l->maxrows,       \
                old_rows);                                                                         \
        }                                                                                          \
        fprintf(lndebug_fp, ", " __VA_ARGS__);                                                     \
        fflush(lndebug_fp);                                                                        \
    } while (0)
#else
#define lndebug(fmt, ...)
#endif

/* ================================ History ================================= */

/* Free the history, but does not reset it. Only used when we have to
 * exit() to avoid memory leaks are reported by valgrind & co. */
static void freeHistory(void) {
    if (history) {
        int j;

        for (j = 0; j < history_len; j++)
            free(history[j]);
        free(history);
    }
}

/* This is the API call to add a new entry in the linenoise history.
 * It uses a fixed array of char pointers that are shifted (memmoved)
 * when the history max length is reached in order to remove the older
 * entry and make room for the new one, so it is not exactly suitable for huge
 * histories, but will work well for a few hundred of entries.
 *
 * Using a circular buffer is smarter, but a bit more complex to handle. */
int linenoiseHistoryAdd(const char* line) {
    char* linecopy;

    if (history_max_len == 0)
        return 0;

    /* Initialization on first call. */
    if (history == NULL) {
        history = (char**)malloc(sizeof(char*) * history_max_len);
        if (history == NULL)
            return 0;
        memset(history, 0, (sizeof(char*) * history_max_len));
    }

    if (!Utf8Proc::isValid(line, strlen(line))) {
        return 0;
    }

    /* Add an heap allocated copy of the line in the history.
     * If we reached the max length, remove the older line. */
    if (!mlmode) {
        std::string singleLineCopy = "";
        int spaceCount = 0;
        // replace all newlines and tabs with spaces
        const char* z = line;
        char quote = 0;
        bool inQuote = false;
        bool inComment = false;
        bool endWhile = false;
        while (*z) {
            switch (*z) {
            case ' ':
            case '\r':
            case '\t':
            case '\n':
            case '\f': { /* White space is turned into a space */
                spaceCount++;
                if (spaceCount >= 4) {
                    spaceCount = 0;
                    singleLineCopy += ' ';
                } else if (z[1] && !isspace((unsigned char)z[1])) {
                    for (auto i = 0; i < spaceCount; i++) {
                        singleLineCopy += ' ';
                    }
                    spaceCount = 0;
                }
                break;
            }
            case '*': {
                singleLineCopy += *z;
                if (inQuote || !inComment) {
                    break;
                }
                if (z[1] == '/') {
                    z++;
                    singleLineCopy += *z;
                    inComment = false;
                    break;
                }
                break;
            }
            case '/': { /* C-style comments */
                if (inQuote || inComment) {
                    singleLineCopy += *z;
                    break;
                }
                if (z[1] == '*') {
                    singleLineCopy += *z;
                    z++;
                    singleLineCopy += *z;
                    inComment = true;
                    break;
                } else if (z[1] == '/') {
                    while (*z && *z != '\n') {
                        z++;
                    }
                    if (*z == 0) {
                        endWhile = true;
                        break;
                    }
                    break;
                } else {
                    break;
                }
            }
            case '`': /* Grave-accent quoted symbols */
            case '"': /* single- and double-quoted strings */
            case '\'': {
                singleLineCopy += *z;
                if (!inComment) {
                    if (!inQuote) {
                        quote = *z;
                        inQuote = true;
                    } else if (quote == *z && (z[-1] != '\\' || (z[-1] == '\\' && z[-2] == '\\'))) {
                        inQuote = false;
                    }
                }
                break;
            }
            default: {
                singleLineCopy += *z;
                break;
            }
            }
            if (endWhile) {
                break;
            }
            z++;
        }
        linecopy = strdup(singleLineCopy.c_str());
    } else {
        // replace all '\n' with '\r\n'
        uint64_t replaced_newline_count = 0;
        uint64_t len;
        for (len = 0; line[len]; len++) {
            if (line[len] == '\r' && line[len + 1] == '\n') {
                // \r\n - skip past the \n
                len++;
            } else if (line[len] == '\n') {
                replaced_newline_count++;
            }
        }
        linecopy = (char*)malloc((len + replaced_newline_count + 1) * sizeof(char));
        uint64_t pos = 0;
        for (len = 0; line[len]; len++) {
            if (line[len] == '\r' && line[len + 1] == '\n') {
                // \r\n - skip past the \n
                linecopy[pos++] = '\r';
                len++;
            } else if (line[len] == '\n') {
                linecopy[pos++] = '\r';
            }
            linecopy[pos++] = line[len];
        }
        linecopy[pos] = '\0';
    }

    /* Don't add duplicated lines. */
    if (history_len && !strcmp(history[history_len - 1], linecopy)) {
        free(linecopy);
        return 0;
    }

    if (history_len == history_max_len) {
        free(history[0]);
        memmove(history, history + 1, sizeof(char*) * (history_max_len - 1));
        history_len--;
    }
    history[history_len] = linecopy;
    history_len++;
    return 1;
}

/* Set the maximum length for the history. This function can be called even
 * if there is already some history, the function will make sure to retain
 * just the latest 'len' elements if the new history length value is smaller
 * than the amount of items already inside the history. */
int linenoiseHistorySetMaxLen(int len) {
    char** new_entry;

    if (len < 1)
        return 0;
    if (history) {
        int tocopy = history_len;

        new_entry = (char**)malloc(sizeof(char*) * len);
        if (new_entry == NULL)
            return 0;

        /* If we can't copy everything, free the elements we'll not use. */
        if (len < tocopy) {
            int j;

            for (j = 0; j < tocopy - len; j++)
                free(history[j]);
            tocopy = len;
        }
        memset(new_entry, 0, sizeof(char*) * len);
        memcpy(new_entry, history + (history_len - tocopy), sizeof(char*) * tocopy);
        free(history);
        history = new_entry;
    }
    history_max_len = len;
    if (history_len > history_max_len)
        history_len = history_max_len;
    return 1;
}

/* Save the history in the specified file. On success 0 is returned
 * otherwise -1 is returned. */
int linenoiseHistorySave(const char* filename) {
#ifdef _WIN32
    long old_umask = umask(_S_IREAD | _S_IWRITE);
    FILE* fp = fopen(filename, "wb");
#else
    mode_t old_umask = umask(S_IXUSR | S_IRWXG | S_IRWXO);
    FILE* fp = fopen(filename, "w");
#endif
    int j;

    umask(old_umask);
    if (fp == NULL)
        return -1;
#ifdef _WIN32
    chmod(filename, _S_IREAD | _S_IWRITE);
#else
    chmod(filename, S_IRUSR | S_IWUSR);
#endif
    for (j = 0; j < history_len; j++)
        fprintf(fp, "%s\n", history[j]);
    fclose(fp);
    return 0;
}

/* Load the history from the specified file. If the file does not exist
 * zero is returned and no operation is performed.
 *
 * If the file exists and the operation succeeded 0 is returned, otherwise
 * on error -1 is returned. */
int linenoiseHistoryLoad(const char* filename) {
    FILE* fp = fopen(filename, "r");
    char buf[LINENOISE_MAX_LINE + 1];
    buf[LINENOISE_MAX_LINE] = '\0';

    if (fp == NULL)
        return -1;

    std::string result;
    while (fgets(buf, LINENOISE_MAX_LINE, fp) != NULL) {
        char* p;

        // strip the newline first
        p = strchr(buf, '\r');
        if (!p) {
            p = strchr(buf, '\n');
        }
        if (p) {
            *p = '\0';
        }
        if (result.empty() && buf[0] == ':') {
            // if the first character is a colon this is a colon command
            // add the full line to the history
            linenoiseHistoryAdd(buf);
            continue;
        } else if (result.empty() && buf[0] == '\0') {
            continue;
        }
        // else we are parsing a Cypher statement
        result += buf;
        if (cypherComplete(buf)) {
            // this line contains a full Cypher statement - add it to the history
            linenoiseHistoryAdd(result.c_str());
            result = std::string();
            continue;
        }
        // the result does not contain a full Cypher statement - add a newline deliminator and move
        // on to the next line
        result += "\r\n";
    }
    fclose(fp);

    return 0;
}

#ifdef _WIN32
HANDLE hOut;
HANDLE hIn;
DWORD consolemodeIn = 0;

/* ======================= Low level terminal handling ====================== */

static int win32read(char* c) {
    DWORD foo;
    INPUT_RECORD b;
    KEY_EVENT_RECORD e;
    BOOL altgr;

    while (1) {
        if (!ReadConsoleInputW(hIn, &b, 1, &foo))
            return 0;
        if (!foo)
            return 0;

        if (b.EventType == KEY_EVENT && b.Event.KeyEvent.bKeyDown) {

            e = b.Event.KeyEvent;
            *c = b.Event.KeyEvent.uChar.AsciiChar;

            switch (e.wVirtualKeyCode) {

            case VK_ESCAPE: /* ignore - send ctrl-c, will return -1 */
                *c = CTRL_C;
                return 1;
            case VK_RETURN: /* enter */
                *c = ENTER;
                return 1;
            case VK_LEFT: /* left */
                *c = 2;
                return 1;
            case VK_RIGHT: /* right */
                *c = 6;
                return 1;
            case VK_UP: /* up */
                *c = 16;
                return 1;
            case VK_DOWN: /* down */
                *c = 14;
                return 1;
            case VK_HOME:
                *c = 1;
                return 1;
            case VK_END:
                *c = 5;
                return 1;
            case VK_BACK:
                *c = 8;
                return 1;
            case VK_DELETE:
                *c = BACKSPACE;
                return 1;
            default:
                WCHAR wch = e.uChar.UnicodeChar;
                if (wch >= 0xD800 && wch <= 0xDBFF) {
                    WCHAR wchHigh = wch;
                    // read key release event
                    if (!ReadConsoleInputW(hIn, &b, 1, &foo))
                        return 0;
                    if (!foo)
                        return 0;
                    // read key down event
                    if (!ReadConsoleInputW(hIn, &b, 1, &foo))
                        return 0;
                    if (!foo)
                        return 0;
                    if (b.EventType == KEY_EVENT && b.Event.KeyEvent.bKeyDown) {
                        wch = b.Event.KeyEvent.uChar.UnicodeChar;
                        if (wch >= 0xDC00 && wch <= 0xDFFF) {
                            WCHAR utf16[2] = {wchHigh, wch};
                            char utf8[5];
                            int len = WideCharToMultiByte(CP_UTF8, 0, utf16, 2, utf8, sizeof(utf8),
                                NULL, NULL);
                            for (int i = 0; i < len; i++) {
                                c[i] = utf8[i];
                            }
                            return len;
                        }
                    }
                } else if (wch) {
                    char utf8[5];
                    int len =
                        WideCharToMultiByte(CP_UTF8, 0, &wch, 1, utf8, sizeof(utf8), NULL, NULL);
                    for (int i = 0; i < len; i++) {
                        c[i] = utf8[i];
                    }
                    return len;
                }
            }
        }
    }
    return -1; /* Makes compiler happy */
}

#ifdef __STRICT_ANSI__
char* strdup(const char* s) {
    size_t l = strlen(s) + 1;
    char* p = malloc(l);

    memcpy(p, s, l);
    return p;
}
#endif /*   __STRICT_ANSI__   */

#endif

/* Enable "mask mode". When it is enabled, instead of the input that
 * the user is typing, the terminal will just display a corresponding
 * number of asterisks, like "****". This is useful for passwords and other
 * secrets that should not be displayed. */
void linenoiseMaskModeEnable(void) {
    maskmode = 1;
}

/* Disable mask mode. */
void linenoiseMaskModeDisable(void) {
    maskmode = 0;
}

/* Set if to use or not the multi line mode. */
void linenoiseSetMultiLine(int ml, const char* filename) {
    mlmode = ml;
    // reset history for new mode
    for (int i = 0; i < history_len; i++)
        free(history[i]);
    history_len = 0;
    if (filename) {
        linenoiseHistoryLoad(filename);
    }
}

/* Return true if the terminal name is in the list of terminals we know are
 * not able to understand basic escape sequences. */
static int isUnsupportedTerm(void) {
#ifndef _WIN32
    char* term = getenv("TERM");
    int j;

    if (term == NULL)
        return 0;
    for (j = 0; unsupported_term[j]; j++)
        if (!strcasecmp(term, unsupported_term[j]))
            return 1;
#endif
    return 0;
}

/* Raw mode: 1960 magic shit. */
static int enableRawMode(int fd) {
#ifndef _WIN32
    struct termios raw;

    if (!isatty(STDIN_FILENO))
        goto fatal;
    if (!atexit_registered) {
        atexit(linenoiseAtExit);
        atexit_registered = 1;
    }
    if (tcgetattr(fd, &orig_termios) == -1)
        goto fatal;

    raw = orig_termios; /* modify the original mode */
    /* input modes: no break, no CR to NL, no parity check, no strip char,
     * no start/stop output control. */
    raw.c_iflag &= ~(BRKINT | ICRNL | INPCK | ISTRIP | IXON);
    /* output modes - disable post processing */
    raw.c_oflag &= ~(OPOST);
    /* control modes - set 8 bit chars */
    raw.c_cflag |= (CS8);
    /* local modes - choing off, canonical off, no extended functions,
     * no signal chars (^Z,^C) */
    raw.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);
    /* control chars - set return condition: min number of bytes and timer.
     * We want read to return every single byte, without timeout. */
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0; /* 1 byte, no timer */

    /* put terminal in raw mode after flushing */
    if (tcsetattr(fd, TCSADRAIN, &raw) < 0)
        goto fatal;
    rawmode = 1;
#else
    REDIS_NOTUSED(fd);

    if (!atexit_registered) {
        /* Cleanup them at exit */
        atexit(linenoiseAtExit);
        atexit_registered = 1;

        /* Init windows console handles only once */
        hOut = GetStdHandle(STD_OUTPUT_HANDLE);
        if (hOut == INVALID_HANDLE_VALUE)
            goto fatal;
    }

    DWORD consolemodeOut;
    if (!GetConsoleMode(hOut, &consolemodeOut)) {
        CloseHandle(hOut);
        errno = ENOTTY;
        return -1;
    };

    hIn = GetStdHandle(STD_INPUT_HANDLE);
    if (hIn == INVALID_HANDLE_VALUE) {
        CloseHandle(hOut);
        errno = ENOTTY;
        return -1;
    }

    GetConsoleMode(hIn, &consolemodeIn);
    SetConsoleMode(hIn, consolemodeIn & ~ENABLE_PROCESSED_INPUT);

    rawmode = 1;
#endif
    return 0;

fatal:
    errno = ENOTTY;
    return -1;
}

static void disableRawMode(int fd) {
#ifdef _WIN32
    REDIS_NOTUSED(fd);
    if (consolemodeIn) {
        SetConsoleMode(hIn, consolemodeIn);
        consolemodeIn = 0;
    }
    rawmode = 0;
#else
    /* Don't even check the return value as it's too late. */
    if (rawmode && tcsetattr(fd, TCSADRAIN, &orig_termios) != -1)
        rawmode = 0;
#endif
}

/* Use the ESC [6n escape sequence to query the horizontal cursor position
 * and return it. On error -1 is returned, on success the position of the
 * cursor. */
static int getCursorPosition(int ifd, int ofd) {
    char buf[32];
    int cols, rows;
    unsigned int i = 0;

    /* Report cursor location */
    if (write(ofd, "\x1b[6n", 4) != 4)
        return -1;

    /* Read the response: ESC [ rows ; cols R */
    while (i < sizeof(buf) - 1) {
        if (read(ifd, buf + i, 1) != 1)
            break;
        if (buf[i] == 'R')
            break;
        i++;
    }
    buf[i] = '\0';

    /* Parse it. */
    if (buf[0] != ESC || buf[1] != '[')
        return -1;
    if (sscanf(buf + 2, "%d;%d", &rows, &cols) != 2)
        return -1;
    return cols;
}

/* Try to get the number of columns in the current terminal, or assume 80
 * if it fails. */
int getColumns(int ifd, int ofd) {
#ifdef _WIN32
    CONSOLE_SCREEN_BUFFER_INFO b;

    if (!GetConsoleScreenBufferInfo(hOut, &b))
        return 80;
    return b.srWindow.Right - b.srWindow.Left;
#else
    struct winsize ws;

    if (ioctl(1, TIOCGWINSZ, &ws) == -1 || ws.ws_col == 0) {
        /* ioctl() failed. Try to query the terminal itself. */
        int start, cols;

        /* Get the initial position so we can restore it later. */
        start = getCursorPosition(ifd, ofd);
        if (start == -1)
            goto failed;

        /* Go to right margin and get position. */
        if (write(ofd, "\x1b[999C", 6) != 6)
            goto failed;
        cols = getCursorPosition(ifd, ofd);
        if (cols == -1)
            goto failed;

        /* Restore position. */
        if (cols > start) {
            char seq[32];
            snprintf(seq, 32, "\x1b[%dD", cols - start);
            if (write(ofd, seq, strlen(seq)) == -1) {
                /* Can't recover... */
            }
        }
        return cols;
    } else {
        return ws.ws_col;
    }

failed:
    return 80;
#endif
}

/* Try to get the number of rows in the current terminal, or assume 24
 * if it fails. */
int getRows(int ifd, int ofd) {
#ifdef _WIN32
    CONSOLE_SCREEN_BUFFER_INFO b;

    if (!GetConsoleScreenBufferInfo(hOut, &b))
        return 24;
    return b.srWindow.Bottom - b.srWindow.Top;
#else
    struct winsize ws;

    if (ioctl(1, TIOCGWINSZ, &ws) == -1 || ws.ws_row == 0) {
        /* ioctl() failed. Try to query the terminal itself. */
        int start, rows;

        /* Get the initial position so we can restore it later. */
        start = getCursorPosition(ifd, ofd);
        if (start == -1)
            goto failed;

        /* Go to bottom margin and get position. */
        if (write(ofd, "\x1b[999B", 6) != 6)
            goto failed;
        rows = getCursorPosition(ifd, ofd);
        if (rows == -1)
            goto failed;

        /* Restore position. */
        if (rows > start) {
            char seq[32];
            snprintf(seq, 32, "\x1b[%dA", rows - start);
            if (write(ofd, seq, strlen(seq)) == -1) {
                /* Can't recover... */
            }
        }
        return rows;
    } else {
        return ws.ws_row;
    }

failed:
    return 24;
#endif
}

/* Clear the screen. Used to handle ctrl+l */
void linenoiseClearScreen(void) {
    if (write(STDOUT_FILENO, "\x1b[H\x1b[2J", 7) <= 0) {
        /* nothing to do, just to avoid warning. */
    }
}

/* Beep, used for completion when there is nothing to complete or when all
 * the choices were already shown. */
static void linenoiseBeep(void) {
    fprintf(stderr, "\x7");
    fflush(stderr);
}

void linenoiseSetHighlightCallback(linenoiseHighlightCallback* fn) {
    highlightCallback = fn;
}

void linenoiseSetErrors(int enabled) {
    errorsEnabled = enabled;
}

void linenoiseSetHighlighting(int enabled) {
    highlightEnabled = enabled;
}

/* ============================== Completion ================================ */

void linenoiseSetCompletion(int enabled) {
    completionEnabled = enabled;
}

/* Free a list of completion option populated by linenoiseAddCompletion(). */
static void freeCompletions(linenoiseCompletions* lc) {
    size_t i;
    for (i = 0; i < lc->len; i++)
        free(lc->cvec[i]);
    if (lc->cvec != NULL)
        free(lc->cvec);
}

TabCompletion linenoiseTabComplete(struct linenoiseState* l) {
    TabCompletion result;
    if (!completionCallback) {
        return result;
    }
    linenoiseCompletions lc;
    lc.cvec = nullptr;
    lc.len = 0;
    // complete based on the cursor position
    auto prev_char = l->buf[l->pos];
    l->buf[l->pos] = '\0';
    completionCallback(l->buf, &lc);
    l->buf[l->pos] = prev_char;
    result.completions.reserve(lc.len);
    for (uint64_t i = 0; i < lc.len; i++) {
        Completion c;
        c.completion = lc.cvec[i];
        c.cursor_pos = c.completion.size();
        c.completion += l->buf + l->pos;
        result.completions.emplace_back(std::move(c));
    }
    freeCompletions(&lc);
    return result;
}

uint64_t linenoiseReadEscapeSequence(int ifd, char seq[]) {
    if (read(ifd, seq, 1) == -1) {
        return 0;
    }
    switch (seq[0]) {
    case 'O':
    case '[':
        // these characters have multiple bytes following them
        break;
    default:
        return 1;
    }
    if (read(ifd, seq + 1, 1) == -1) {
        return 0;
    }

    if (seq[0] != '[') {
        return 2;
    }
    if (seq[1] < '0' || seq[1] > '9') {
        return 2;
    }
    /* Extended escape, read additional byte. */
    if (read(ifd, seq + 2, 1) == -1) {
        return 0;
    }
    if (seq[2] == ';') {
        // read 2 extra bytes
        if (read(ifd, seq + 3, 2) == -1) {
            return 0;
        }
        return 5;
    } else {
        return 3;
    }
}

EscapeSequence linenoiseReadEscapeSequence(int ifd) {
    char seq[5];
    uint64_t length = linenoiseReadEscapeSequence(ifd, seq);
    if (length == 0) {
        return EscapeSequence::INVALID;
    }
    lndebug("escape of length %d\n", length);
    switch (length) {
    case 1:
        if (seq[0] >= 'a' && seq[0] <= 'z') {
            return EscapeSequence(uint64_t(EscapeSequence::ALT_A) + (seq[0] - 'a'));
        }
        if (seq[0] >= 'A' && seq[0] <= 'Z') {
            return EscapeSequence(uint64_t(EscapeSequence::ALT_A) + (seq[0] - 'A'));
        }
        switch (seq[0]) {
        case BACKSPACE:
            return EscapeSequence::ALT_BACKSPACE;
        case ESC:
            return EscapeSequence::ESCAPE;
        case '<':
            return EscapeSequence::ALT_LEFT_ARROW;
        case '>':
            return EscapeSequence::ALT_RIGHT_ARROW;
        case '\\':
            return EscapeSequence::ALT_BACKSLASH;
        default:
            lndebug("unrecognized escape sequence of length 1 - %d\n", seq[0]);
            break;
        }
        break;
    case 2:
        if (seq[0] == 'O') {
            switch (seq[1]) {
            case 'A': /* Up */
                return EscapeSequence::UP;
            case 'B': /* Down */
                return EscapeSequence::DOWN;
            case 'C': /* Right */
                return EscapeSequence::RIGHT;
            case 'D': /* Left */
                return EscapeSequence::LEFT;
            case 'H': /* Home */
                return EscapeSequence::HOME;
            case 'F': /* End*/
                return EscapeSequence::END;
            case 'c':
                return EscapeSequence::ALT_F;
            case 'd':
                return EscapeSequence::ALT_B;
            default:
                lndebug("unrecognized escape sequence (O) %d\n", seq[1]);
                break;
            }
        } else if (seq[0] == '[') {
            switch (seq[1]) {
            case 'A': /* Up */
                return EscapeSequence::UP;
            case 'B': /* Down */
                return EscapeSequence::DOWN;
            case 'C': /* Right */
                return EscapeSequence::RIGHT;
            case 'D': /* Left */
                return EscapeSequence::LEFT;
            case 'H': /* Home */
                return EscapeSequence::HOME;
            case 'F': /* End*/
                return EscapeSequence::END;
            case 'Z': /* Shift Tab */
                return EscapeSequence::SHIFT_TAB;
            default:
                lndebug("unrecognized escape sequence (seq[1]) %d\n", seq[1]);
                break;
            }
        } else {
            lndebug("unrecognized escape sequence of length %d (%d %d)\n", length, seq[0], seq[1]);
        }
        break;
    case 3:
        if (seq[2] == '~') {
            switch (seq[1]) {
            case '1':
                return EscapeSequence::HOME;
            case '3': /* Delete key. */
                return EscapeSequence::DEL;
            case '4':
            case '8':
                return EscapeSequence::END;
            default:
                lndebug("unrecognized escape sequence (~) %d\n", seq[1]);
                break;
            }
        } else if (seq[1] == '5' && seq[2] == 'C') {
            return EscapeSequence::ALT_F;
        } else if (seq[1] == '5' && seq[2] == 'D') {
            return EscapeSequence::ALT_B;
        } else {
            lndebug("unrecognized escape sequence of length %d\n", length);
        }
        break;
    case 5:
        if (memcmp(seq, "[1;5C", 5) == 0 || memcmp(seq, "[1;3C", 5) == 0) {
            // [1;5C: move word right
            return EscapeSequence::CTRL_MOVE_FORWARDS;
        } else if (memcmp(seq, "[1;5D", 5) == 0 || memcmp(seq, "[1;3D", 5) == 0) {
            // [1;5D: move word left
            return EscapeSequence::CTRL_MOVE_BACKWARDS;
        } else {
            lndebug("unrecognized escape sequence (;) %d\n", seq[1]);
        }
        break;
    default:
        lndebug("unrecognized escape sequence of length %d\n", length);
        break;
    }
    return EscapeSequence::UNKNOWN;
}

/* This is an helper function for linenoiseEdit() and is called when the
 * user types the <tab> key in order to complete the string currently in the
 * input.
 *
 * The state of the editing is encapsulated into the pointed linenoiseState
 * structure as described in the structure definition. */
static int completeLine(EscapeSequence& current_sequence, struct linenoiseState* l) {
    int nread, nwritten;
    char c = 0;

    auto completion_list = linenoiseTabComplete(l);
    auto& completions = completion_list.completions;
    if (completions.empty()) {
        linenoiseBeep();
    } else {
        bool stop = false;
        bool accept_completion = false;
        uint64_t i = 0;

        while (!stop) {
            /* Show completion or original buffer */
            if (i < completions.size()) {
                struct linenoiseState saved = *l;

                l->len = completions[i].completion.size();
                l->pos = completions[i].cursor_pos;
                l->buf = (char*)completions[i].completion.c_str();
                refreshLine(l);
                l->len = saved.len;
                l->pos = saved.pos;
                l->buf = saved.buf;
            } else {
                refreshLine(l);
            }

            nread = read(l->ifd, &c, 1);
            if (nread <= 0) {
                return -1;
            }

            lndebug("\nComplete Character %d\n", (int)c);
            switch (c) {
            case TAB: /* tab */
                i = (i + 1) % (completions.size() + 1);
                if (i == completions.size()) {
                    linenoiseBeep();
                }
                break;
            case ESC: { /* escape */
                auto escape = linenoiseReadEscapeSequence(l->ifd);
                switch (escape) {
                case EscapeSequence::SHIFT_TAB:
                    // shift-tab: move backwards
                    if (i == 0) {
                        linenoiseBeep();
                    } else {
                        i--;
                    }
                    break;
                case EscapeSequence::ESCAPE:
                    /* Re-show original buffer */
                    if (i < completions.size()) {
                        refreshLine(l);
                    }
                    current_sequence = escape;
                    stop = true;
                    break;
                default:
                    current_sequence = escape;
                    accept_completion = true;
                    stop = true;
                    break;
                }
                break;
            }
            default:
                accept_completion = true;
                stop = true;
                break;
            }
            if (stop && accept_completion && i < completions.size()) {
                /* Update buffer and return */
                if (i < completions.size()) {
                    nwritten = snprintf(l->buf, l->buflen, "%s", completions[i].completion.c_str());
                    l->pos = completions[i].cursor_pos;
                    l->len = nwritten;
                }
            }
        }
    }
    return c; /* Return last read character */
}

/* Register a callback function to be called for tab-completion. */
void linenoiseSetCompletionCallback(linenoiseCompletionCallback* fn) {
    completionCallback = fn;
}

/* Register a hits function to be called to show hits to the user at the
 * right of the prompt. */
void linenoiseSetHintsCallback(linenoiseHintsCallback* fn) {
    hintsCallback = fn;
}

/* Register a function to free the hints returned by the hints callback
 * registered with linenoiseSetHintsCallback(). */
void linenoiseSetFreeHintsCallback(linenoiseFreeHintsCallback* fn) {
    freeHintsCallback = fn;
}

/* This function is used by the callback function registered by the user
 * in order to add completion options given the input string when the
 * user typed <tab>. See the example.c source code for a very easy to
 * understand example. */
void linenoiseAddCompletion(linenoiseCompletions* lc, const char* str) {
    size_t len = strlen(str);
    char *copy, **cvec;

    copy = (char*)malloc(len + 1);
    if (copy == NULL)
        return;
    memcpy(copy, str, len + 1);
    cvec = (char**)realloc(lc->cvec, sizeof(char*) * (lc->len + 1));
    if (cvec == NULL) {
        free(copy);
        return;
    }
    lc->cvec = cvec;
    lc->cvec[lc->len++] = copy;
}

/* =========================== Line editing ================================= */

/* We define a very simple "append buffer" structure, that is an heap
 * allocated string where we can append to. This is useful in order to
 * write all the escape sequences in a buffer and flush them to the standard
 * output in a single call, to avoid flickering effects. */
struct AppendBuffer {
    void abAppend(const char* s, uint64_t len) { buffer.append(s, len); }
    void abAppend(const char* s) { buffer.append(s); }

    void abWrite(int fd) {
        if (write(fd, buffer.c_str(), buffer.size()) == -1) {
            /* Can't recover from write error. */
            lndebug("%s", "Failed to write buffer\n");
        }
    }

private:
    std::string buffer;
};

/* Helper of refreshSingleLine() and refreshMultiLine() to show hints
 * to the right of the prompt. */
void refreshShowHints(AppendBuffer& append_buffer, struct linenoiseState* l, int plen) {
    char seq[64];
    if (hintsCallback && plen + l->len < l->cols) {
        int color = -1, bold = 0;
        char* hint = hintsCallback(l->buf, &color, &bold);
        if (hint) {
            int hintlen = strlen(hint);
            int hintmaxlen = l->cols - (plen + l->len);
            if (hintlen > hintmaxlen)
                hintlen = hintmaxlen;
            if (bold == 1 && color == -1)
                color = 37;
            if (color != -1 || bold != 0)
                snprintf(seq, 64, "\033[%d;%d;49m", bold, color);
            else
                seq[0] = '\0';
            append_buffer.abAppend(seq, strlen(seq));
            append_buffer.abAppend(hint, hintlen);
            if (color != -1 || bold != 0)
                append_buffer.abAppend("\033[0m");
            /* Call the function to free the hint returned. */
            if (freeHintsCallback)
                freeHintsCallback(hint);
        }
    }
}

uint32_t linenoiseComputeRenderWidth(const char* buf, size_t len) {
    // UTF8 in prompt, get render width.
    uint32_t charPos = 0;
    uint32_t renderWidth = 0;
    int sz;
    while (charPos < len) {
        // Incorrect UTF8 character in prompt, ignore and increment cursor.
        if (Utf8Proc::utf8ToCodepoint(buf + charPos, sz) < 0) {
            charPos++;
            renderWidth++;
            continue;
        }
        uint32_t charRenderWidth = Utf8Proc::renderWidth(buf, charPos);
        charPos = utf8proc_next_grapheme(buf, len, charPos);
        renderWidth += charRenderWidth;
    }
    return renderWidth;
}

std::vector<highlightToken> getShellCommandTokens(char* buf, size_t len) {
    std::vector<highlightToken> tokens;

    // identifier token for the shell command itself
    highlightToken shell_token;
    shell_token.type = tokenType::TOKEN_KEYWORD;
    shell_token.start = 0;
    tokens.push_back(shell_token);

    for (uint64_t i = 0; i + 1 < len; i++) {
        if (isspace(buf[i])) {
            highlightToken argument_token;
            argument_token.type = tokenType::TOKEN_STRING_CONSTANT;
            argument_token.start = i + 1;
            tokens.push_back(argument_token);
        }
    }
    return tokens;
}

static tokenType convertToken(antlr4::Token* token) {
    auto tokenType = token->getType();
    auto tokenText = token->getText();
    std::transform(tokenText.begin(), tokenText.end(), tokenText.begin(), ::toupper);

    switch (tokenType) {
    case CypherLexer::StringLiteral:
        return tokenType::TOKEN_STRING_CONSTANT;
    case CypherLexer::SP: // comments can show up as SP
        if (tokenText.length() > 1 && tokenText[0] == '/' && tokenText[1] == '/') {
            return tokenType::TOKEN_COMMENT;
        } else {
            return tokenType::TOKEN_IDENTIFIER;
        }
    case CypherLexer::CypherComment:
        return tokenType::TOKEN_COMMENT;
    case CypherLexer::DecimalInteger:
    case CypherLexer::Digit:
    case CypherLexer::HexDigit:
    case CypherLexer::HexLetter:
    case CypherLexer::ZeroDigit:
    case CypherLexer::NonZeroDigit:
    case CypherLexer::NonZeroOctDigit:
    case CypherLexer::RegularDecimalReal:
        return tokenType::TOKEN_NUMERIC_CONSTANT;
    default:
        break;
    }

    for (auto& keyword : _keywordList) {
        if (tokenText == keyword) {
            return tokenType::TOKEN_KEYWORD;
        }
    }

    return tokenType::TOKEN_IDENTIFIER;
}

std::vector<highlightToken> getParseTokens(char* buf, size_t len) {
    std::string cypher(buf, len);
    auto inputStream = antlr4::ANTLRInputStream(cypher);

    auto cypherLexer = CypherLexer(&inputStream);
    auto lexerTokens = antlr4::CommonTokenStream(&cypherLexer);
    lexerTokens.fill();

    std::vector<highlightToken> tokens;
    for (auto& token : lexerTokens.getTokens()) {
        highlightToken new_token;
        new_token.type = convertToken(token);
        if (tokens.empty()) {
            new_token.start = 0;
        } else {
            new_token.start = tokens.back().start + tokens.back().length;
        }
        new_token.length = token->getText().length();
        tokens.push_back(new_token);
    }

    if (!tokens.empty() && tokens[0].start > 0) {
        highlightToken new_token;
        new_token.type = tokenType::TOKEN_IDENTIFIER;
        new_token.start = 0;
        tokens.insert(tokens.begin(), new_token);
    }
    if (tokens.empty() && cypher.size() > 0) {
        highlightToken new_token;
        new_token.type = tokenType::TOKEN_IDENTIFIER;
        new_token.start = 0;
        tokens.push_back(new_token);
    }
    return tokens;
}

std::vector<highlightToken> highlightingTokenize(char* buf, size_t len, bool is_shell_command) {
    std::vector<highlightToken> tokens;
    if (!is_shell_command) {
        // SQL query - use parser to obtain tokens
        tokens = getParseTokens(buf, len);
    } else {
        // : command
        tokens = getShellCommandTokens(buf, len);
    }
    return tokens;
}

// insert a token of length 1 of the specified type
void insertToken(tokenType insert_type, uint64_t insert_pos, std::vector<highlightToken>& tokens) {
    std::vector<highlightToken> new_tokens;
    new_tokens.reserve(tokens.size() + 1);
    uint64_t i;
    bool found = false;
    for (i = 0; i < tokens.size(); i++) {
        // find the exact position where we need to insert the token
        if (tokens[i].start == insert_pos) {
            // this token is exactly at this render position

            // insert highlighting for the bracket
            highlightToken token;
            token.start = insert_pos;
            token.type = insert_type;
            new_tokens.push_back(token);

            // now we need to insert the other token ONLY if the other token is not immediately
            // following this one
            if (i + 1 >= tokens.size() || tokens[i + 1].start > insert_pos + 1) {
                token.start = insert_pos + 1;
                token.type = tokens[i].type;
                new_tokens.push_back(token);
            }
            i++;
            found = true;
            break;
        } else if (tokens[i].start > insert_pos) {
            // the next token is AFTER the render position
            // insert highlighting for the bracket
            highlightToken token;
            token.start = insert_pos;
            token.type = insert_type;
            new_tokens.push_back(token);

            // now just insert the next token
            new_tokens.push_back(tokens[i]);
            i++;
            found = true;
            break;
        } else {
            // insert the token
            new_tokens.push_back(tokens[i]);
        }
    }
    // copy over the remaining tokens
    for (; i < tokens.size(); i++) {
        new_tokens.push_back(tokens[i]);
    }
    if (!found) {
        // token was not added - add it to the end
        highlightToken token;
        token.start = insert_pos;
        token.type = insert_type;
        new_tokens.push_back(token);
    }
    tokens = std::move(new_tokens);
}

static void openBracket(std::vector<uint64_t>& brackets, std::vector<uint64_t>& cursor_brackets,
    uint64_t pos, uint64_t i) {
    // check if the cursor is at this position
    if (pos == i) {
        // cursor is exactly on this position - always highlight this bracket
        if (!cursor_brackets.empty()) {
            cursor_brackets.clear();
        }
        cursor_brackets.push_back(i);
    }
    if (cursor_brackets.empty() && ((i + 1) == pos || (pos + 1) == i)) {
        // cursor is either BEFORE or AFTER this bracket and we don't have any highlighted bracket
        // yet highlight this bracket
        cursor_brackets.push_back(i);
    }
    brackets.push_back(i);
}

static void closeBracket(std::vector<uint64_t>& brackets, std::vector<uint64_t>& cursor_brackets,
    uint64_t pos, uint64_t i, std::vector<uint64_t>& errors) {
    if (pos == i) {
        // cursor is on this closing bracket
        // clear any selected brackets - we always select this one
        cursor_brackets.clear();
    }
    if (brackets.empty()) {
        // closing bracket without matching opening bracket
        errors.push_back(i);
    } else {
        if (cursor_brackets.size() == 1) {
            if (cursor_brackets.back() == brackets.back()) {
                // this closing bracket matches the highlighted opening cursor bracket - highlight
                // both
                cursor_brackets.push_back(i);
            }
        } else if (cursor_brackets.empty() && (pos == i || (i + 1) == pos || (pos + 1) == i)) {
            // no cursor bracket selected yet and cursor is BEFORE or AFTER this bracket
            // add this bracket
            cursor_brackets.push_back(i);
            cursor_brackets.push_back(brackets.back());
        }
        brackets.pop_back();
    }
}

static void HandleBracketErrors(const std::vector<uint64_t>& brackets,
    std::vector<uint64_t>& errors) {
    if (brackets.empty()) {
        return;
    }
    // if there are unclosed brackets remaining not all brackets were closed
    for (auto& bracket : brackets) {
        errors.push_back(bracket);
    }
}

void addErrorHighlighting(uint64_t render_start, uint64_t render_end,
    std::vector<highlightToken>& tokens, struct linenoiseState* l) {
    enum class ScanState {
        STANDARD,
        IN_SINGLE_QUOTE,
        IN_DOUBLE_QUOTE,
        IN_COMMENT,
        IN_MULTILINE_COMMENT
    };

    static constexpr const uint64_t MAX_ERROR_LENGTH = 2000;
    if (l->len >= MAX_ERROR_LENGTH) {
        return;
    }
    // do a pass over the buffer to collect errors:
    // * brackets without matching closing/opening bracket
    // * single quotes without matching closing single quote
    // * double quote without matching double quote
    ScanState state = ScanState::STANDARD;
    std::vector<uint64_t> brackets;        // ()
    std::vector<uint64_t> square_brackets; // []
    std::vector<uint64_t> curly_brackets;  // {}
    std::vector<uint64_t> errors;
    std::vector<uint64_t> cursor_brackets;
    std::vector<uint64_t> comment_start;
    std::vector<uint64_t> comment_end;
    std::string dollar_quote_marker;
    uint64_t quote_pos = 0;
    for (uint64_t i = 0; i < l->len; i++) {
        auto c = l->buf[i];
        switch (state) {
        case ScanState::STANDARD:
            switch (c) {
            case '/':
                if (i + 1 < l->len && l->buf[i + 1] == '/') {
                    // // puts us in a comment
                    comment_start.push_back(i);
                    i++;
                    state = ScanState::IN_COMMENT;
                    break;
                } else if (i + 1 < l->len && l->buf[i + 1] == '*') {
                    // /* puts us in a multiline comment
                    comment_start.push_back(i);
                    i++;
                    state = ScanState::IN_MULTILINE_COMMENT;
                    break;
                }
                break;
            case '\'':
                state = ScanState::IN_SINGLE_QUOTE;
                quote_pos = i;
                break;
            case '\"':
                state = ScanState::IN_DOUBLE_QUOTE;
                quote_pos = i;
                break;
            case '(':
                openBracket(brackets, cursor_brackets, l->pos, i);
                break;
            case '[':
                openBracket(square_brackets, cursor_brackets, l->pos, i);
                break;
            case '{':
                openBracket(curly_brackets, cursor_brackets, l->pos, i);
                break;
            case ')':
                closeBracket(brackets, cursor_brackets, l->pos, i, errors);
                break;
            case ']':
                closeBracket(square_brackets, cursor_brackets, l->pos, i, errors);
                break;
            case '}':
                closeBracket(curly_brackets, cursor_brackets, l->pos, i, errors);
                break;
            default:
                break;
            }
            break;
        case ScanState::IN_COMMENT:
            // comment state - the only thing that will get us out is a newline
            switch (c) {
            case '\r':
            case '\n':
                // newline - left comment state
                state = ScanState::STANDARD;
                comment_end.push_back(i);
                break;
            default:
                break;
            }
            break;
        case ScanState::IN_MULTILINE_COMMENT:
            // multiline comment state - the only thing that will get us out is a */
            if (c == '*' && i + 1 < l->len && l->buf[i + 1] == '/') {
                // found the end of the multiline comment
                state = ScanState::STANDARD;
                comment_end.push_back(i + 1);
                i++;
            }
            break;
        case ScanState::IN_SINGLE_QUOTE:
            // single quote - all that will get us out is an unescaped single-quote
            if (c == '\'') {
                if (i > 0 && l->buf[i - 1] == '\\') { // escaped quote
                    i++;
                    break;
                } else {
                    // otherwise revert to standard scan state
                    state = ScanState::STANDARD;
                    break;
                }
            }
            break;
        case ScanState::IN_DOUBLE_QUOTE:
            // double quote - all that will get us out is an unescaped quote
            if (c == '"') {
                if (i > 0 && l->buf[i - 1] == '\\') { // escaped quote
                    i++;
                    break;
                } else {
                    // otherwise revert to standard scan state
                    state = ScanState::STANDARD;
                    break;
                }
            }
            break;
        default:
            break;
        }
    }
    if (state == ScanState::IN_DOUBLE_QUOTE || state == ScanState::IN_SINGLE_QUOTE) {
        // quote is never closed
        errors.push_back(quote_pos);
    }
    HandleBracketErrors(brackets, errors);
    HandleBracketErrors(square_brackets, errors);
    HandleBracketErrors(curly_brackets, errors);

    // insert all the errors for highlighting
    for (auto& error : errors) {
        lndebug("Error found at position %llu\n", error);
        if (error < render_start || error > render_end) {
            continue;
        }
        auto render_error = error - render_start;
        insertToken(tokenType::TOKEN_ERROR, render_error, tokens);
    }
    if (cursor_brackets.size() != 2) {
        // no matching cursor brackets found
        cursor_brackets.clear();
    }
    // insert bracket for highlighting
    for (auto& bracket_position : cursor_brackets) {
        lndebug("Highlight bracket at position %d\n", bracket_position);
        if (bracket_position < render_start || bracket_position > render_end) {
            continue;
        }

        uint64_t render_position = bracket_position - render_start;
        insertToken(tokenType::TOKEN_BRACKET, render_position, tokens);
    }
    // insert comments
    if (!comment_start.empty()) {
        std::vector<highlightToken> new_tokens;
        new_tokens.reserve(tokens.size());
        uint64_t token_idx = 0;
        for (uint64_t c = 0; c < comment_start.size(); c++) {
            auto c_start = comment_start[c];
            auto c_end = c < comment_end.size() ? comment_end[c] : l->len;
            if (c_start < render_start || c_end > render_end) {
                continue;
            }
            lndebug("Comment at position %d to %d\n", c_start, c_end);
            c_start -= render_start;
            c_end -= render_start;
            bool inserted_comment = false;

            highlightToken comment_token;
            comment_token.start = c_start;
            comment_token.type = tokenType::TOKEN_COMMENT;

            for (; token_idx < tokens.size(); token_idx++) {
                if (tokens[token_idx].start >= c_start) {
                    // insert the comment here
                    new_tokens.push_back(comment_token);
                    inserted_comment = true;
                    break;
                }
                new_tokens.push_back(tokens[token_idx]);
            }
            if (!inserted_comment) {
                new_tokens.push_back(comment_token);
            } else {
                // skip all tokens until we exit the comment again
                for (; token_idx < tokens.size(); token_idx++) {
                    if (tokens[token_idx].start > c_end) {
                        break;
                    }
                }
            }
        }
        for (; token_idx < tokens.size(); token_idx++) {
            new_tokens.push_back(tokens[token_idx]);
        }
        tokens = std::move(new_tokens);
    }
}

std::string linenoiseHighlightText(char* buf, size_t len, size_t start_pos, size_t end_pos,
    const std::vector<highlightToken>& tokens) {
    static std::string underline = "\033[4m";
    static std::string keyword = "\033[32m";
    static std::string continuation_selected = "\033[32m";
    static std::string constant = "\033[33m";
    static std::string continuation = "\033[90m";
    static std::string comment = "\033[90m";
    static std::string error = "\033[31m";
    static std::string reset = "\033[39m";
    static std::string reset_underline = "\033[24m";

    std::stringstream ss;
    size_t prev_pos = 0;
    for (size_t i = 0; i < tokens.size(); i++) {
        size_t next = i + 1 < tokens.size() ? tokens[i + 1].start : len;
        if (next < start_pos) {
            // this token is not rendered at all
            continue;
        }

        auto& token = tokens[i];
        size_t start = token.start > start_pos ? token.start : start_pos;
        size_t end = next > end_pos ? end_pos : next;
        if (end <= start) {
            continue;
        }
        if (prev_pos > start) {
            lndebug("ERROR - Rendering at position %llu after rendering at position %llu\n", start,
                prev_pos);
            continue;
        }
        prev_pos = start;
        std::string text = std::string(buf + start, end - start);
        switch (token.type) {
        case tokenType::TOKEN_KEYWORD:
            ss << keyword << text << reset;
            break;
        case tokenType::TOKEN_NUMERIC_CONSTANT:
        case tokenType::TOKEN_STRING_CONSTANT:
            ss << constant << text << reset;
            break;
        case tokenType::TOKEN_CONTINUATION:
            ss << continuation << text << reset;
            break;
        case tokenType::TOKEN_CONTINUATION_SELECTED:
            ss << continuation_selected << text << reset;
            break;
        case tokenType::TOKEN_BRACKET:
            ss << underline << text << reset_underline;
            break;
        case tokenType::TOKEN_ERROR:
            ss << error << text << reset;
            break;
        case tokenType::TOKEN_COMMENT:
            ss << comment << text << reset;
            break;
        default:
            ss << text;
        }
    }
    ss << "\033[0m";
    return ss.str();
}

bool isCompletionCharacter(char c) {
    if (c >= 'A' && c <= 'Z') {
        return true;
    }
    if (c >= 'a' && c <= 'z') {
        return true;
    }
    if (c == '_') {
        return true;
    }
    if (c == '.') {
        return true;
    }
    if (c == ':') {
        return true;
    }
    return false;
}

bool linenoiseAddCompletionMarker(const char* buf, uint64_t len, std::string& result_buffer,
    std::vector<highlightToken>& tokens, struct linenoiseState* l) {
    if (!completionEnabled) {
        return false;
    }
    if (!l->continuePromptActive) {
        // don't render when pressing ctrl+c, only when editing
        return false;
    }
    static constexpr const uint64_t MAX_COMPLETION_LENGTH = 1000;
    if (len >= MAX_COMPLETION_LENGTH) {
        return false;
    }
    if (!l->insert || l->pos != len) {
        // only show when inserting a character at the end
        return false;
    }
    if (l->pos < 1) {
        // we need at least 1 bytes
        return false;
    }
    if (!tokens.empty() && tokens.back().type == tokenType::TOKEN_ERROR) {
        // don't show auto-completion when we have errors
        return false;
    }
    // we ONLY show completion if we have typed at least one character that is supported for
    // completion for now this is ONLY the characters a-z, A-Z, underscore (_), period (.), and
    // colon (:)
    if (!isCompletionCharacter(buf[l->pos - 1])) {
        return false;
    }
    auto completion = linenoiseTabComplete(l);
    if (completion.completions.empty()) {
        // no completions found
        return false;
    }
    if (completion.completions[0].completion.size() <= len) {
        // completion is not long enough
        return false;
    }
    // we have stricter requirements for rendering completions - the completion must match exactly
    for (uint64_t i = l->pos; i > 0; i--) {
        auto cpos = i - 1;
        if (!isCompletionCharacter(buf[cpos])) {
            break;
        }
        if (completion.completions[0].completion[cpos] != buf[cpos]) {
            return false;
        }
    }
    // add the first completion found for rendering purposes
    result_buffer = std::string(buf, len);
    result_buffer += completion.completions[0].completion.substr(len);

    highlightToken completion_token;
    completion_token.start = len;
    completion_token.type = tokenType::TOKEN_COMMENT;
    tokens.push_back(completion_token);
    return true;
}

static void truncateText(char*& buf, size_t& len, size_t pos, size_t cols, size_t plen,
    bool highlight, size_t& render_pos, char* highlightBuf) {
    if (Utf8Proc::isValid(buf, len)) {
        // UTF8 in prompt, handle rendering.
        size_t remainingRenderWidth = cols - plen - 1;
        size_t startPos = 0;
        size_t charPos = 0;
        size_t prevPos = 0;
        size_t totalRenderWidth = 0;
        while (charPos < len) {
            uint32_t charRenderWidth = Utf8Proc::renderWidth(buf, charPos);
            prevPos = charPos;
            charPos = utf8proc_next_grapheme(buf, len, charPos);
            totalRenderWidth += charPos - prevPos;
            if (totalRenderWidth >= remainingRenderWidth) {
                if (prevPos >= pos) {
                    // We passed the cursor: break, we no longer need to render.
                    charPos = prevPos;
                    break;
                } else {
                    // We did not pass the cursor yet, meaning we still need to render.
                    // Remove characters from the start until it fits again.
                    while (totalRenderWidth >= remainingRenderWidth) {
                        uint32_t startCharWidth = Utf8Proc::renderWidth(buf, startPos);
                        uint32_t newStart = utf8proc_next_grapheme(buf, len, startPos);
                        totalRenderWidth -= newStart - startPos;
                        startPos = newStart;
                        render_pos -= startCharWidth;
                    }
                }
            }
            if (prevPos < pos) {
                render_pos += charRenderWidth;
            }
        }
        std::string highlight_buffer;
        if (highlight) {
            bool is_shell_command = buf[0] == ':';
            auto tokens = highlightingTokenize(buf, len, is_shell_command);
            highlight_buffer = linenoiseHighlightText(buf, len, startPos, charPos, tokens);
        } else {
            highlight_buffer = std::string(buf + startPos, charPos - startPos);
        }
        std::strcpy(highlightBuf, highlight_buffer.c_str());
        len = highlight_buffer.size();
    } else {
        // Invalid UTF8: fallback.
        while ((plen + pos) >= cols) {
            buf++;
            len--;
            pos--;
        }
        while (plen + len > cols) {
            len--;
        }
        render_pos = pos;
    }
}

/* Single line low level line refresh.
 *
 * Rewrite the currently edited line accordingly to the buffer content,
 * cursor position, and number of columns of the terminal. */
static void refreshSingleLine(struct linenoiseState* l) {
    if (!l->render) {
        return;
    }

    char seq[64];
    uint32_t plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
    int fd = l->ofd;
    char buf[LINENOISE_MAX_LINE];
    size_t len = l->len;
    size_t pos = l->pos;
    size_t chars = l->totalUTF8Chars;
    AppendBuffer append_buffer;
    size_t renderPos = 0;

    truncateText(l->buf, len, pos, l->cols, plen, highlightEnabled, renderPos, buf);

    /* Cursor to left edge */
    snprintf(seq, 64, "\r");
    append_buffer.abAppend(seq);

    append_buffer.abAppend("\033[0m"); // reset all attributes

    /* Write the prompt and the current buffer content */
    append_buffer.abAppend(l->prompt);
    if (maskmode == 1) {
        while (chars--)
            append_buffer.abAppend("*");
    } else {
        append_buffer.abAppend(buf, len);
    }
    /* Show hits if any. */
    refreshShowHints(append_buffer, l, plen);
    /* Erase to right */
    snprintf(seq, 64, "\x1b[0K");
    append_buffer.abAppend(seq);
    /* Move cursor to original position. */
    snprintf(seq, 64, "\r\x1b[%dC", (int)(renderPos + plen));
    append_buffer.abAppend(seq);

    append_buffer.abWrite(fd);
}

bool isNewline(char c) {
    return c == '\n' || c == '\r';
}

void nextPosition(const char* buf, size_t len, size_t& cpos, size_t& rows, size_t& cols, int plen,
    size_t ws_col) {
    if (isNewline(buf[cpos])) {
        // explicit newline! move to next line and insert a prompt
        rows++;
        cols = plen;
        cpos++;
        if (buf[cpos - 1] == '\r' && cpos < len && buf[cpos] == '\n') {
            cpos++;
        }
        return;
    }
    int sz;
    int char_render_width;
    if (Utf8Proc::utf8ToCodepoint(buf + cpos, sz) < 0) {
        char_render_width = 1;
        cpos++;
    } else {
        char_render_width = (int)Utf8Proc::renderWidth(buf, cpos);
        cpos = utf8proc_next_grapheme(buf, len, cpos);
    }
    if (cols + char_render_width > ws_col) {
        // exceeded l->cols, move to next row
        rows++;
        cols = char_render_width;
    }
    cols += char_render_width;
}

void positionToColAndRow(size_t target_pos, int& out_row, int& out_col, size_t& rows, size_t& cols,
    struct linenoiseState* l) {
    int plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
    out_row = -1;
    out_col = 0;
    rows = 1;
    cols = plen;
    size_t cpos = 0;
    while (cpos < l->len) {
        if (cols >= l->cols && !isNewline(l->buf[cpos])) {
            // exceeded width - move to next line
            rows++;
            cols = 0;
        }
        if (out_row < 0 && cpos >= target_pos) {
            out_row = rows;
            out_col = cols;
        }
        nextPosition(l->buf, l->len, cpos, rows, cols, plen, l->cols);
    }
    if (target_pos == l->len) {
        out_row = rows;
        out_col = cols;
    }
}

size_t colAndRowToPosition(size_t target_row, size_t target_col, struct linenoiseState* l) {
    int plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
    size_t rows = 1;
    size_t cols = plen;
    size_t last_cpos = 0;
    size_t cpos = 0;
    while (cpos < l->len) {
        if (cols >= l->cols) {
            // exceeded width - move to next line
            rows++;
            cols = 0;
        }
        if (rows > target_row) {
            // we have skipped our target row - that means "target_col" was out of range for this
            // row return the last position within the target row
            return last_cpos;
        }
        if (rows == target_row) {
            last_cpos = cpos;
        }
        if (rows == target_row && cols == target_col) {
            return cpos;
        }
        nextPosition(l->buf, l->len, cpos, rows, cols, plen, l->cols);
    }
    return cpos;
}

std::string linenoiseAddContinuationMarkers(const char* buf, size_t len, size_t plen,
    int cursor_row, struct linenoiseState* l, std::vector<highlightToken>& tokens) {
    std::string result;
    size_t rows = 1;
    size_t cols = plen;
    size_t cpos = 0;
    size_t prev_pos = 0;
    size_t extra_bytes = 0;    // extra bytes introduced
    size_t token_position = 0; // token position
    std::vector<highlightToken> new_tokens;
    new_tokens.reserve(tokens.size());
    while (cpos < len) {
        bool is_newline = isNewline(buf[cpos]);
        nextPosition(buf, len, cpos, rows, cols, plen, l->cols);
        for (; prev_pos < cpos; prev_pos++) {
            result += buf[prev_pos];
        }
        if (is_newline) {
            bool is_cursor_row = (int)rows == cursor_row;
            const char* prompt = is_cursor_row ? l->selectedContinuePrompt : l->continuePrompt;
            if (!l->continuePromptActive) {
                prompt = "";
            }
            size_t continuationLen = strlen(prompt);
            size_t continuationRender = linenoiseComputeRenderWidth(prompt, continuationLen);
            // pad with spaces prior to prompt
            for (size_t i = continuationRender; i < plen; i++) {
                result += " ";
            }
            result += prompt;
            size_t continuationBytes = plen - continuationRender + continuationLen;
            if (token_position < tokens.size()) {
                for (; token_position < tokens.size(); token_position++) {
                    if (tokens[token_position].start >= cpos) {
                        // not there yet
                        break;
                    }
                    tokens[token_position].start += extra_bytes;
                    new_tokens.push_back(tokens[token_position]);
                }
                tokenType prev_type = tokenType::TOKEN_IDENTIFIER;
                if (token_position > 0 && token_position < tokens.size() + 1) {
                    prev_type = tokens[token_position - 1].type;
                }
                highlightToken token;
                token.start = cpos + extra_bytes;
                token.type = is_cursor_row ? tokenType::TOKEN_CONTINUATION_SELECTED :
                                             tokenType::TOKEN_CONTINUATION;
                new_tokens.push_back(token);

                token.start = cpos + extra_bytes + continuationBytes;
                token.type = prev_type;
                new_tokens.push_back(token);
            }
            extra_bytes += continuationBytes;
        }
    }
    for (; prev_pos < cpos; prev_pos++) {
        result += buf[prev_pos];
    }
    for (; token_position < tokens.size(); token_position++) {
        tokens[token_position].start += extra_bytes;
        new_tokens.push_back(tokens[token_position]);
    }
    tokens = std::move(new_tokens);
    return result;
}

/* Multi line low level line refresh.
 *
 * Rewrite the currently edited line accordingly to the buffer content,
 * cursor position, and number of columns of the terminal. */
static void refreshMultiLine(struct linenoiseState* l) {
    if (!l->render) {
        return;
    }
    char seq[64];
    int plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
    // utf8 in prompt, get render width
    size_t rows, cols;
    int new_cursor_row, new_cursor_x;
    positionToColAndRow(l->pos, new_cursor_row, new_cursor_x, rows, cols, l);
    int col; /* colum position, zero-based. */
    int old_rows = l->maxrows ? l->maxrows : 1;
    int fd = l->ofd;
    auto render_buf = l->buf;
    auto render_len = l->len;
    uint64_t render_start = 0;
    uint64_t render_end = render_len;
    if (l->clear_screen) {
        l->oldpos = 0;
        old_rows = 0;
        l->clear_screen = false;
    }
    if (rows > l->rows) {
        // the text does not fit in the terminal (too many rows)
        // enable scrolling mode
        // check if, given the current y_scroll, the cursor is visible
        // display range is [y_scroll, y_scroll + l->rows]
        if (new_cursor_row < int(l->y_scroll) + 1) {
            l->y_scroll = new_cursor_row - 1;
        } else if (new_cursor_row > int(l->y_scroll) + int(l->rows)) {
            l->y_scroll = new_cursor_row - l->rows;
        }
        // display only characters up to the current scroll position
        if (l->y_scroll == 0) {
            render_start = 0;
        } else {
            render_start = colAndRowToPosition(l->y_scroll, 0, l);
        }
        if (l->y_scroll + l->rows >= rows) {
            render_end = l->len;
        } else {
            render_end = colAndRowToPosition(l->y_scroll + l->rows, 99999, l);
        }
        new_cursor_row -= l->y_scroll;
        render_buf += render_start;
        render_len = render_end - render_start;
        lndebug("truncate to rows %d - %d (render bytes %d to %d)", l->y_scroll,
            l->y_scroll + l->rows, render_start, render_end);
        rows = l->rows;
    } else {
        l->y_scroll = 0;
    }

    /* Update maxrows if needed. */
    if (rows > l->maxrows) {
        l->maxrows = rows;
    }

    std::vector<highlightToken> tokens;
    std::string highlight_buffer;
    if (highlightEnabled) {
        bool is_shell_command = l->buf[0] == ':';
        tokens = highlightingTokenize(render_buf, render_len, is_shell_command);

        if (errorsEnabled) {
            // add error highlighting
            addErrorHighlighting(render_start, render_end, tokens, l);
        }

        // add completion hint
        if (linenoiseAddCompletionMarker(render_buf, render_len, highlight_buffer, tokens, l)) {
            render_buf = (char*)highlight_buffer.c_str();
            render_len = highlight_buffer.size();
        }
    }
    std::string continue_buffer;
    if (rows > 1) {
        // add continuation markers
        continue_buffer = linenoiseAddContinuationMarkers(render_buf, render_len, plen,
            l->y_scroll > 0 ? new_cursor_row + 1 : new_cursor_row, l, tokens);
        render_buf = (char*)continue_buffer.c_str();
        render_len = continue_buffer.size();
    }
    if (highlightEnabled) {
        if (Utf8Proc::isValid(render_buf, render_len)) {
            highlight_buffer =
                linenoiseHighlightText(render_buf, render_len, 0, render_len, tokens);
            render_buf = (char*)highlight_buffer.c_str();
            render_len = highlight_buffer.size();
        }
    }

    /* First step: clear all the lines used before. To do so start by
     * going to the last row. */
    AppendBuffer append_buffer;
    if (old_rows - l->oldpos > 0) {
        lndebug("go down %d", old_rows - l->oldpos);
        snprintf(seq, 64, "\x1b[%dB", old_rows - int(l->oldpos));
        append_buffer.abAppend(seq);
    }

    /* Now for every row clear it, go up. */
    for (int j = 0; j < old_rows - 1; j++) {
        lndebug("clear+up");
        append_buffer.abAppend("\r\x1b[0K\x1b[1A");
    }

    /* Clean the top line. */
    lndebug("clear");
    append_buffer.abAppend("\r\x1b[0K");

    append_buffer.abAppend("\033[0m"); // reset all attributes

    /* Write the prompt and the current buffer content */
    if (l->y_scroll == 0) {
        append_buffer.abAppend(l->prompt);
    }
    append_buffer.abAppend(render_buf, render_len);

    /* Show hints if any. */
    refreshShowHints(append_buffer, l, plen);

    /* If we are at the very end of the screen with our prompt, we need to
     * emit a newline and move the prompt to the first column. */
    lndebug("pos > 0 %d", l->pos > 0 ? 1 : 0);
    lndebug("pos == len %d", l->pos == l->len ? 1 : 0);
    lndebug("new_cursor_x == cols %d", new_cursor_x == l->cols ? 1 : 0);
    if (l->pos > 0 && l->pos == l->len && new_cursor_x == (int)l->cols) {
        lndebug("<newline>", 0);
        append_buffer.abAppend("\n");
        append_buffer.abAppend("\r");
        rows++;
        new_cursor_row++;
        new_cursor_x = 0;
        if (rows > l->maxrows) {
            l->maxrows = rows;
        }
    }
    lndebug("render %d rows (old rows %d)", rows, old_rows);

    /* Move cursor to right position. */
    lndebug("new_cursor_row %d", new_cursor_row);
    lndebug("new_cursor_x %d", new_cursor_x);
    lndebug("len %d", l->len);
    lndebug("old_cursor_rows %d", l->oldpos);
    lndebug("pos %d", l->pos);
    lndebug("max cols %d", l->cols);

    /* Go up till we reach the expected positon. */
    // printf("rows - new_cursor_row %d\n", rows - new_cursor_row);
    if (rows - new_cursor_row > 0) {
        lndebug("go-up %d", rows - new_cursor_row);
        snprintf(seq, 64, "\x1b[%ldA", rows - new_cursor_row);
        append_buffer.abAppend(seq);
    }

    /* Set column. */
    col = new_cursor_x;
    lndebug("set col %d", 1 + col);
    if (col) {
        snprintf(seq, 64, "\r\x1b[%dC", col);
    } else {
        snprintf(seq, 64, "\r");
    }
    append_buffer.abAppend(seq);

    lndebug("\n");
    l->oldpos = new_cursor_row;
    append_buffer.abWrite(fd);
}

/* Calls the two low level functions refreshSingleLine() or
 * refreshMultiLine() according to the selected mode. */
static void refreshLine(struct linenoiseState* l) {
    if (mlmode)
        refreshMultiLine(l);
    else
        refreshSingleLine(l);
}

static void highlightSearchMatch(char* buffer, char* resultBuf, const char* search_buffer) {
    const char* matchUnderlinePrefix = "\033[4m";
    const char* matchUnderlineResetPostfix = "\033[24m";

    std::string searchPhrase(search_buffer);
    std::string buf(buffer);
    std::string underlinedBuf = "";

    std::string matchedWord;
    auto searchPhrasePos = 0u;
    bool matching = false;
    bool matched = false;
    bool ansiCode = false;

    if (mlmode) {
        // Replace newlines with spaces
        std::regex newline_regex("[\n\r]");
        buf = std::regex_replace(buf, newline_regex, " ");
    }

    for (auto i = 0u; i < buf.length(); i++) {
        if (matched) {
            underlinedBuf += buf[i];
            continue;
        }
        if (matching) {
            if (searchPhrasePos >= searchPhrase.length()) {
                underlinedBuf += std::string(matchUnderlinePrefix);
                underlinedBuf += matchedWord;
                underlinedBuf += std::string(matchUnderlineResetPostfix);
                underlinedBuf += buf[i];
                matched = true;
                continue;
            }
            if (ansiCode) {
                if (tolower(buf[i]) == 'm') {
                    ansiCode = false;
                }
            } else if (tolower(buf[i]) == tolower(searchPhrase[searchPhrasePos])) {
                searchPhrasePos++;
            } else if (buf[i] == '\033') {
                ansiCode = true;
            } else {
                auto matchIndex = 0u;
                // find the oldest place where the match restarts
                for (auto j = i; j > i - matchedWord.length(); j--) {
                    if (tolower(buf[j]) == tolower(searchPhrase[0])) {
                        matchIndex = j;
                    }
                }
                if (matchIndex > 0) {
                    // restart matching at matchIndex
                    for (auto j = i - matchedWord.length(); j < matchIndex; j++) {
                        underlinedBuf += buf[j];
                    }
                    i = matchIndex;
                    matchedWord = buf[i];
                    searchPhrasePos = 1;
                } else {
                    // end matching
                    matching = false;
                    searchPhrasePos = 0;
                    underlinedBuf += matchedWord;
                    underlinedBuf += buf[i];
                    matchedWord = "";
                }
                continue;
            }
            matchedWord += buf[i];
            continue;
        } else if (ansiCode) {
            if (tolower(buf[i]) == 'm') {
                ansiCode = false;
            }
        } else if (buf[i] == '\033') {
            ansiCode = true;
        } else if (tolower(buf[i]) == tolower(searchPhrase[searchPhrasePos])) {
            matchedWord += buf[i];
            matching = true;
            searchPhrasePos++;
            continue;
        }
        underlinedBuf += buf[i];
    }

    if (!matched) {
        if (searchPhrasePos >= searchPhrase.length()) {
            underlinedBuf += std::string(matchUnderlinePrefix);
            underlinedBuf += matchedWord;
            underlinedBuf += std::string(matchUnderlineResetPostfix);
        } else {
            underlinedBuf += matchedWord;
        }
    }
    if (underlinedBuf.empty()) {
        strncpy(resultBuf, buf.c_str(), LINENOISE_MAX_LINE - 1);
    } else {
        strncpy(resultBuf, underlinedBuf.c_str(), LINENOISE_MAX_LINE - 1);
    }
}

static void refreshSearchMultiLine(struct linenoiseState* l, const char* searchPrompt,
    const char* searchText) {
    char seq[64];
    std::string strBuf = l->buf;
    uint32_t plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
    int rows = 1; /* Rows used by current buf. */
    int rpos = 1; /* Cursor relative row. */
    int rpos2;    /* rpos after refresh. */
    int old_rows = l->search_maxrows;
    int fd = l->ofd, j;
    AppendBuffer append_buffer;
    char highlightBuf[LINENOISE_MAX_LINE];
    char buf[LINENOISE_MAX_LINE];
    size_t len = l->len;
    size_t render_pos = 0;
    l->search_maxrows = 1;

    // replace every newline with space for single line printing
    for (size_t i = 0; i < len; i++) {
        if (strBuf[i] == '\n' || strBuf[i] == '\r') {
            strBuf[i] = ' ';
        }
    }

    char* buffer = (char*)strBuf.c_str();
    truncateText(buffer, len, l->pos, l->cols, plen, highlightEnabled, render_pos, highlightBuf);
    if (strlen(highlightBuf) > 0) {
        highlightSearchMatch(highlightBuf, buf, searchText);
    } else {
        highlightSearchMatch(buffer, buf, searchText);
    }
    len = strlen(buf);

    /* First step: clear all the lines used before. To do so start by
     * going to the last row. */
    if (mlmode && search_mlmode) {
        search_mlmode = 0;
        int multi_old_rows = l->maxrows ? l->maxrows : 1;
        if (multi_old_rows < old_rows) {
            multi_old_rows = old_rows;
        }
        if (multi_old_rows - l->oldpos > 0) {
            lndebug("go down %d", multi_old_rows - l->oldpos);
            snprintf(seq, 64, "\x1b[%dB", multi_old_rows - int(l->oldpos));
            append_buffer.abAppend(seq);
        }

        /* Now for every row clear it, go up. */
        for (int j = 0; j < multi_old_rows - 1; j++) {
            lndebug("clear+up");
            append_buffer.abAppend("\r\x1b[0K\x1b[1A");
        }
    } else {
        if (old_rows - rpos > 0) {
            lndebug("go down %d", old_rows - rpos);
            snprintf(seq, 64, "\x1b[%dB", old_rows - rpos);
            append_buffer.abAppend(seq);
        }

        /* Now for every row clear it, go up. */
        for (j = 0; j < old_rows - 1; j++) {
            lndebug("clear+up");
            snprintf(seq, 64, "\r\x1b[0K\x1b[1A");
            append_buffer.abAppend(seq);
        }
    }

    /* Clean the top line. */
    lndebug("clear");
    snprintf(seq, 64, "\r\x1b[0K");
    append_buffer.abAppend(seq);

    /* Cursor to left edge */
    snprintf(seq, 64, "\r");
    append_buffer.abAppend(seq);

    append_buffer.abAppend("\033[0m"); // reset all attributes

    /* Write the prompt and the current buffer content */
    append_buffer.abAppend(l->prompt);
    if (maskmode == 1) {
        unsigned int i;
        for (i = 0; i < len; i++)
            append_buffer.abAppend("*");
    } else {
        append_buffer.abAppend(buf, len);
    }

    /* Erase to right */
    snprintf(seq, 64, "\x1b[0K");
    append_buffer.abAppend(seq);
    if (strlen(searchPrompt) > 0) {
        lndebug("<newline>");
        append_buffer.abAppend("\n", 1);
        /* Cursor to left edge */
        snprintf(seq, 64, "\r");
        append_buffer.abAppend(seq);
        rows = 2;
        l->search_maxrows = 2;
        /* Write the search prompt content */
        append_buffer.abAppend(searchPrompt);
        /* Erase to right */
        snprintf(seq, 64, "\x1b[0K");
        append_buffer.abAppend(seq);
    }

    /* Show hits if any. */
    refreshShowHints(append_buffer, l, plen);

    /* Move cursor to right position. */
    rpos2 = 1; /* current cursor relative row. */
    lndebug("rpos2 %d", rpos2);

    /* Go up till we reach the expected positon. */
    if (strlen(searchPrompt) > 0) {
        lndebug("go-up %d", rows - rpos2);
        snprintf(seq, 64, "\x1b[%dA", rows - rpos2);
        append_buffer.abAppend(seq);
    }

    /* Move cursor to original position. */
    snprintf(seq, 64, "\r\x1b[%dC", (int)(render_pos + plen));
    append_buffer.abAppend(seq);

    l->search_oldpos = l->pos;

    append_buffer.abWrite(fd);
}

static void refreshSearch(struct linenoiseState* l) {
    std::string search_prompt;
    std::string no_matches_text = std::string(l->buf);
    std::string truncatedSearchText = "";
    bool no_matches = l->search_index >= l->search_matches.size();
    if (l->search_buf.empty()) {
        l->prev_search_match = l->buf;
        l->prev_search_match_history_index = history_len - 1 - l->history_index;
        search_prompt = "bck-i-search: _";
    } else {
        std::string search_text;
        std::string matches_text;
        search_text = l->search_buf;
        if (!no_matches) {
            search_prompt = "bck-i-search: ";
        } else {
            no_matches_text = std::string(l->prev_search_match);
            search_prompt = "failing-bck-i-search: ";
        }

        char* search_buf = (char*)search_text.c_str();
        size_t search_len = search_text.size();

        size_t cols = l->cols - search_prompt.length() - 1;

        size_t render_pos = 0;
        char emptyHighlightBuf[LINENOISE_MAX_LINE];
        truncateText(search_buf, search_len, search_len, cols, false, 0, render_pos,
            emptyHighlightBuf);
        truncatedSearchText = std::string(search_buf, search_len);
        search_prompt += truncatedSearchText;
        search_prompt += "_";
    }

    linenoiseState clone = *l;
    l->plen = search_prompt.size();
    if (no_matches) {
        // if there are no matches render the no_matches_text
        l->buf = (char*)no_matches_text.c_str();
        l->len = no_matches_text.size();
        l->pos = 0;
    } else {
        // if there are matches render the current history item
        auto search_match = l->search_matches[l->search_index];
        auto history_index = search_match.history_index;
        auto cursor_position = search_match.match_end;
        l->buf = history[history_index];
        l->len = strlen(history[history_index]);
        l->pos = cursor_position;
        l->prev_search_match = l->buf;
        l->prev_search_match_history_index = history_index;
    }
    refreshSearchMultiLine(l, search_prompt.c_str(), truncatedSearchText.c_str());

    l->buf = clone.buf;
    l->len = clone.len;
    l->pos = clone.pos;
    l->plen = clone.plen;
}

static void cancelSearch(linenoiseState* l) {
    refreshSearchMultiLine(l, (char*)"", (char*)"");

    history_len--;
    free(history[history_len]);
    linenoiseHistoryAdd("");

    l->search = false;
    l->search_buf = std::string();
    l->search_matches.clear();
    l->search_index = 0;
}

static char acceptSearch(linenoiseState* l, char nextCommand) {
    bool hasMatches = false;
    int history_index = l->prev_search_match_history_index;
    if (l->search_index < l->search_matches.size()) {
        // if there is a match - copy it into the buffer
        auto match = l->search_matches[l->search_index];
        hasMatches = true;
        history_index = match.history_index;
    }

    while (history_len > 1 && history_index != (history_len - 1 - l->history_index)) {
        /* Update the current history entry before to
         * overwrite it with the next one. */
        free(history[history_len - 1 - l->history_index]);
        history[history_len - 1 - l->history_index] = strdup(l->buf);
        /* Show the new entry */
        l->history_index += (history_index < (history_len - 1 - l->history_index)) ? 1 : -1;
        if (l->history_index < 0) {
            l->history_index = 0;
            break;
        } else if (l->history_index >= history_len) {
            l->history_index = history_len - 1;
            break;
        }
        strncpy(l->buf, history[history_len - 1 - l->history_index], l->buflen);
        l->buf[l->buflen - 1] = '\0';
        l->len = strlen(l->buf);
        if (!hasMatches) {
            l->pos = l->len;
        } else {
            l->pos = l->search_matches[l->search_index].match_end;
        }
    }

    if (nextCommand == ENTER) {
        l->pos = l->len;
    }

    cancelSearch(l);
    return nextCommand;
}

static void performSearch(linenoiseState* l) {
    // we try to maintain the current match while searching
    size_t current_match = history_len;
    if (l->search_index < l->search_matches.size()) {
        current_match = l->search_matches[l->search_index].history_index;
    }
    l->search_matches.clear();
    l->search_index = 0;
    std::unordered_set<std::string> matches;
    if (l->search_buf.empty()) {
        // we match everything if search_buf is empty
        for (size_t i = history_len; i > 0; i--) {
            size_t history_index = i - 1;
            auto lhistory = std::string(history[history_index]);
            kuzu::common::StringUtils::toLower(lhistory);
            if (matches.find(lhistory) != matches.end()) {
                continue;
            }
            matches.insert(lhistory);
            searchMatch match;
            match.history_index = history_index;
            match.match_start = 0;
            match.match_end = 0;
            l->search_matches.push_back(match);
        }
    } else {
        auto lsearch = l->search_buf;
        kuzu::common::StringUtils::toLower(lsearch);
        for (size_t i = history_len; i > 0; i--) {
            size_t history_index = i - 1;
            auto lhistory = std::string(history[history_index]);

            if (mlmode) {
                // Replace newlines with spaces
                std::regex newline_regex("[\n\r]");
                lhistory = std::regex_replace(lhistory, newline_regex, " ");
            }

            kuzu::common::StringUtils::toLower(lhistory);
            if (matches.find(lhistory) != matches.end()) {
                continue;
            }
            matches.insert(lhistory);
            auto entry = lhistory.find(lsearch);
            if (entry != std::string::npos) {
                if (history_index == current_match) {
                    l->search_index = l->search_matches.size();
                }
                searchMatch match;
                match.history_index = history_index;
                match.match_start = entry;
                match.match_end = entry + lsearch.size();
                l->search_matches.push_back(match);
            }
        }
    }
}

static void searchPrev(linenoiseState* l) {
    if (l->search_index > 0) {
        l->search_index--;
    }
}

static void searchNext(linenoiseState* l) {
    if (l->search_index < l->search_matches.size() - 1) {
        l->search_index += 1;
    }
}

bool pastedInput(int ifd) {
#ifdef _WIN32
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(ifd, &readfds);
    int isPasted = select(0, &readfds, NULL, NULL, NULL);
    return (isPasted != 0 && isPasted != SOCKET_ERROR);
#else
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(ifd, &rfds);

    // no timeout: return immediately
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    return select(1, &rfds, NULL, NULL, &tv);
#endif
}

/* Move cursor one row up. */
bool linenoiseEditMoveRowUp(struct linenoiseState* l) {
    if (!mlmode) {
        return false;
    }
    size_t rows, cols;
    int cursor_row, cursor_col;
    positionToColAndRow(l->pos, cursor_row, cursor_col, rows, cols, l);
    if (cursor_row <= 1) {
        return false;
    }
    // we can move the cursor a line up
    lndebug("source pos %d", l->pos);
    lndebug("move from row %d to row %d", cursor_row, cursor_row - 1);
    cursor_row--;
    l->pos = colAndRowToPosition(cursor_row, cursor_col, l);
    lndebug("new pos %d", l->pos);
    refreshLine(l);
    return true;
}

/* Move cursor one row down. */
bool linenoiseEditMoveRowDown(struct linenoiseState* l) {
    if (!mlmode) {
        return false;
    }
    size_t rows, cols;
    int cursor_row, cursor_col;
    positionToColAndRow(l->pos, cursor_row, cursor_col, rows, cols, l);
    if (cursor_row >= (int)rows) {
        return false;
    }
    // we can move the cursor a line down
    lndebug("source pos %d", l->pos);
    lndebug("move from row %d to row %d", cursor_row, cursor_row + 1);
    cursor_row++;
    l->pos = colAndRowToPosition(cursor_row, cursor_col, l);
    lndebug("new pos %d", l->pos);
    refreshLine(l);
    return true;
}

static char linenoiseSearch(linenoiseState* l, char c) {
    char seq[64];

    switch (c) {
    case 10:
    case ENTER: /* enter */
        // accept search and run
        return acceptSearch(l, ENTER);
    case CTRL_N:
    case CTRL_R:
        // move to the next match index
        searchNext(l);
        break;
    case CTRL_P:
    case CTRL_S:
        // move to the prev match index
        searchPrev(l);
        break;
    case ESC: /* escape sequence */
        /* Read the next two bytes representing the escape sequence.
         * Use two calls to handle slow terminals returning the two
         * chars at different times. */
        // note: in search mode we ignore almost all special commands
        if (read(l->ifd, seq, 1) == -1)
            break;
        if (seq[0] == ESC) {
            // double escape accepts search without any additional command
            return acceptSearch(l, 0);
        }
        if (seq[0] == 'b' || seq[0] == 'f') {
            break;
        }
        if (read(l->ifd, seq + 1, 1) == -1)
            break;

        /* ESC [ sequences. */
        if (seq[0] == '[') {
            if (seq[1] >= '0' && seq[1] <= '9') {
                /* Extended escape, read additional byte. */
                if (read(l->ifd, seq + 2, 1) == -1)
                    break;
                if (seq[2] == '~') {
                    switch (seq[1]) {
                    case '1':
                        return acceptSearch(l, CTRL_A);
                    case '4':
                    case '8':
                        return acceptSearch(l, CTRL_E);
                    default:
                        break;
                    }
                } else if (seq[2] == ';') {
                    // read 2 extra bytes
                    if (read(l->ifd, seq + 3, 2) == -1)
                        break;
                }
            } else {
                switch (seq[1]) {
                case 'A': /* Up */
                    // accepts search without any additional command
                    c = acceptSearch(l, 0);
                    linenoiseEditHistoryNext(l, LINENOISE_HISTORY_PREV);
                    return c;
                case 'B': /* Down */
                    // accepts search without any additional command
                    c = acceptSearch(l, 0);
                    linenoiseEditHistoryNext(l, LINENOISE_HISTORY_NEXT);
                    return c;
                case 'D': /* Left */
                    return acceptSearch(l, CTRL_B);
                case 'C': /* Right */
                    return acceptSearch(l, CTRL_F);
                case 'H': /* Home */
                    return acceptSearch(l, CTRL_A);
                case 'F': /* End*/
                    return acceptSearch(l, CTRL_E);
                default:
                    break;
                }
            }
        }
        /* ESC O sequences. */
        else if (seq[0] == 'O') {
            switch (seq[1]) {
            case 'H': /* Home */
                return acceptSearch(l, CTRL_A);
            case 'F': /* End*/
                return acceptSearch(l, CTRL_E);
            default:
                break;
            }
        }
        break;
    case CTRL_A: // accept search, move to start of line
        return acceptSearch(l, CTRL_A);
    case TAB:
        if (l->hasMoreData) {
            l->search_buf += ' ';
            performSearch(l);
            break;
        }
        return acceptSearch(l, CTRL_E);
    case CTRL_E: // accept search - move to end of line
        return acceptSearch(l, CTRL_E);
    case CTRL_B: // accept search - move cursor left
        return acceptSearch(l, CTRL_B);
    case CTRL_F: // accept search - move cursor right
        return acceptSearch(l, CTRL_F);
    case CTRL_T: // accept search: swap character
        return acceptSearch(l, CTRL_T);
    case CTRL_U: // accept search, clear buffer
        return acceptSearch(l, CTRL_U);
    case CTRL_K: // accept search, clear after cursor
        return acceptSearch(l, CTRL_K);
    case CTRL_D: // accept search, delete a character
        return acceptSearch(l, CTRL_D);
    case CTRL_L:
        linenoiseClearScreen();
        break;
    case CTRL_C:
    case CTRL_G:
        // abort search
        l->buf[0] = '\0';
        l->len = 0;
        l->pos = 0;
        cancelSearch(l);
        return 0;
    case BACKSPACE: /* backspace */
    case 8:         /* ctrl-h */
    case CTRL_W:    /* ctrl-w */
        // remove trailing UTF-8 bytes (if any)
        while (!l->search_buf.empty() && ((l->search_buf.back() & 0xc0) == 0x80)) {
            l->search_buf.pop_back();
        }
        // finally remove the first UTF-8 byte
        if (!l->search_buf.empty()) {
            l->search_buf.pop_back();
        }
        performSearch(l);
        break;
    default:
        // add input to search buffer
        l->search_buf += c;
        // perform the search
        performSearch(l);
        break;
    }
    refreshSearch(l);
    return 0;
}

/* Insert the character 'c' at cursor current position.
 *
 * On error writing to the terminal -1 is returned, otherwise 0. */
void linenoiseInsertCharacter(char c, struct linenoiseState* l) {
    if (l->len < l->buflen) {
        if (l->len == l->pos) {
            l->buf[l->pos] = c;
            l->pos++;
            l->len++;
            l->buf[l->len] = '\0';
        } else {
            memmove(l->buf + l->pos + 1, l->buf + l->pos, l->len - l->pos);
            l->buf[l->pos] = c;
            l->len++;
            l->pos++;
            l->buf[l->len] = '\0';
        }
    }
}

int linenoiseEditInsert(struct linenoiseState* l, char c) {
    if (l->hasMoreData) {
        l->render = false;
    }
    l->insert = true;
    linenoiseInsertCharacter(c, l);
    refreshLine(l);
    return 0;
}

int linenoiseEditInsertMulti(struct linenoiseState* l, const char* c) {
    for (size_t pos = 0; c[pos]; pos++) {
        linenoiseInsertCharacter(c[pos], l);
    }
    refreshLine(l);
    return 0;
}

static uint32_t prevChar(struct linenoiseState* l) {
    return Utf8Proc::previousGraphemeCluster(l->buf, l->len, l->pos);
}

static uint32_t nextChar(struct linenoiseState* l) {
    return utf8proc_next_grapheme(l->buf, l->len, l->pos);
}

/* Move cursor on the left. */
void linenoiseEditMoveLeft(struct linenoiseState* l) {
    if (l->pos > 0) {
        l->pos = prevChar(l);
        refreshLine(l);
    }
}

/* Move cursor on the right. */
void linenoiseEditMoveRight(struct linenoiseState* l) {
    if (l->pos != l->len) {
        l->pos = nextChar(l);
        refreshLine(l);
    }
}

/* Checks to see if character defines a separation between words */
bool isWordSeparator(char c) {
    if (c >= 'a' && c <= 'z') {
        return false;
    }
    if (c >= 'A' && c <= 'Z') {
        return false;
    }
    if (c >= '0' && c <= '9') {
        return false;
    }
    return true;
}

/* Move cursor one word to the left */
void linenoiseEditMoveWordLeft(struct linenoiseState* l) {
    if (l->pos == 0) {
        return;
    }
    do {
        l->pos = prevChar(l);
    } while (l->pos > 0 && !isWordSeparator(l->buf[l->pos]));
    refreshLine(l);
}

/* Move cursor one word to the right */
void linenoiseEditMoveWordRight(struct linenoiseState* l) {
    if (l->pos == l->len) {
        return;
    }
    do {
        l->pos = nextChar(l);
    } while (l->pos != l->len && !isWordSeparator(l->buf[l->pos]));
    refreshLine(l);
}

/* Move cursor to the start of the line. */
void linenoiseEditMoveHome(struct linenoiseState* l) {
    if (l->pos != 0) {
        l->pos = 0;
        refreshLine(l);
    }
}

/* Move cursor to the end of the line. */
void linenoiseEditMoveEnd(struct linenoiseState* l) {
    if (l->pos != l->len) {
        l->pos = l->len;
        refreshLine(l);
    }
}

/* Substitute the currently edited line with the next or previous history
 * entry as specified by 'dir'. */
void linenoiseEditHistoryNext(struct linenoiseState* l, int dir) {
    if (history_len > 1) {
        /* Update the current history entry before to
         * overwrite it with the next one. */
        free(history[history_len - 1 - l->history_index]);
        history[history_len - 1 - l->history_index] = strdup(l->buf);
        /* Show the new entry */
        l->history_index += (dir == LINENOISE_HISTORY_PREV) ? 1 : -1;
        if (l->history_index < 0) {
            l->history_index = 0;
            return;
        } else if (l->history_index >= history_len) {
            l->history_index = history_len - 1;
            return;
        }
        strncpy(l->buf, history[history_len - 1 - l->history_index], l->buflen);
        l->buf[l->buflen - 1] = '\0';
        l->len = l->pos = strlen(l->buf);
        refreshLine(l);
    }
}

/* Delete the character at the right of the cursor without altering the cursor
 * position. Basically this is what happens with the "Delete" keyboard key. */
void linenoiseEditDelete(struct linenoiseState* l) {
    if (l->len > 0 && l->pos < l->len) {
        uint32_t newPos = nextChar(l);
        uint32_t charSize = newPos - l->pos;
        memmove(l->buf + l->pos, l->buf + newPos, l->len - newPos);
        l->len -= charSize;
        if (charSize > 1) {
            l->totalUTF8Chars--;
        }
        l->buf[l->len] = '\0';
        refreshLine(l);
    }
}

/* Backspace implementation. */
void linenoiseEditBackspace(struct linenoiseState* l) {
    if (l->pos > 0 && l->len > 0) {
        uint32_t newPos = prevChar(l);
        uint32_t charSize = l->pos - newPos;
        memmove(l->buf + newPos, l->buf + l->pos, l->len - l->pos);
        l->len -= charSize;
        l->pos = newPos;
        if (charSize > 1) {
            l->totalUTF8Chars--;
        }
        l->buf[l->len] = '\0';
        refreshLine(l);
    }
}

/* Delete the previous word, maintaining the cursor at the start of the
 * current word. */
void linenoiseEditDeletePrevWord(struct linenoiseState* l) {
    size_t old_pos = l->pos;
    size_t diff;

    while (l->pos > 0 && l->buf[l->pos - 1] == ' ')
        l->pos--;
    while (l->pos > 0 && l->buf[l->pos - 1] != ' ')
        l->pos--;
    diff = old_pos - l->pos;
    memmove(l->buf + l->pos, l->buf + old_pos, l->len - old_pos + 1);
    l->len -= diff;
    refreshLine(l);
}

bool linenoiseAllWhitespace(const char* z) {
    for (; *z; z++) {
        // whitespace
        if (isspace((unsigned char)z[0]))
            continue;
        // multi-line comments
        if (*z == '/' && z[1] == '*') {
            z += 2;
            while (*z && (*z != '*' || z[1] != '/')) {
                z++;
            }
            if (*z == 0) {
                return false;
            }
            z++;
            continue;
        }
        // single-line comments
        if (*z == '/' && z[1] == '/') {
            z += 2;
            while (*z && *z != '\n') {
                z++;
            }
            if (*z == 0) {
                return true;
            }
            continue;
        }
        return false;
    }
    return true;
}

bool cypherComplete(const char* z) {
    enum TokenType { tkSEMI = 0, tkWS, tkOTHER };

    unsigned char state = 0; /* Current state, using numbers defined in header comment */
    unsigned char token;     /* Value of the next token */

    /* If triggers are not supported by this compile then the statement machine
    ** used to detect the end of a statement is much simpler
    */
    static const unsigned char trans[3][3] = {
        /* Token:           */
        /* State:       **  SEMI  WS  OTHER */
        /* 0 INVALID: */ {
            1,
            0,
            2,
        },
        /* 1   START: */
        {
            1,
            1,
            2,
        },
        /* 2  NORMAL: */
        {
            1,
            2,
            2,
        },
    };

    while (*z) {
        switch (*z) {
        case ';': { /* A semicolon */
            token = tkSEMI;
            break;
        }
        case ' ':
        case '\r':
        case '\t':
        case '\n':
        case '\f': { /* White space is ignored */
            token = tkWS;
            break;
        }
        case '/': { /* C-style comments */
            if (z[1] == '*') {
                z += 2;
                while (z[0] && (z[0] != '*' || z[1] != '/')) {
                    z++;
                }
                if (z[0] == 0)
                    return 0;
                z++;
                token = tkWS;
                break;
            } else if (z[1] == '/') {
                while (*z && *z != '\n') {
                    z++;
                }
                if (*z == 0)
                    return state == 1;
                token = tkWS;
                break;
            } else {
                token = tkOTHER;
                break;
            }
        }
        case '`': /* Grave-accent quoted symbols */
        case '"': /* single- and double-quoted strings */
        case '\'': {
            int c = *z;
            z++;
            while (*z && (*z != c || (z[-1] == '\\' && z[-2] != '\\'))) {
                z++;
            }
            if (*z == 0)
                return 0;
            token = tkOTHER;
            break;
        }
        default: {
            token = tkOTHER;
            break;
        }
        }
        state = trans[state][token];
        z++;
    }
    return state == 1;
}

/* This function is the core of the line editing capability of linenoise.
 * It expects 'fd' to be already in "raw mode" so that every key pressed
 * will be returned ASAP to read().
 *
 * The resulting string is put into 'buf' when the user type enter, or
 * when ctrl+d is typed.
 *
 * The function returns the length of the current buffer. */
static int linenoiseEdit(int stdin_fd, int stdout_fd, char* buf, size_t buflen, const char* prompt,
    const char* continuePrompt, const char* selectedContinuePrompt) {
    struct linenoiseState l;

    /* Populate the linenoise state that we pass to functions implementing
     * specific editing functionalities. */
    l.ifd = stdin_fd;
    l.ofd = stdout_fd;
    l.buf = buf;
    l.buflen = buflen;
    l.prompt = prompt;
    l.continuePrompt = continuePrompt;
    l.selectedContinuePrompt = selectedContinuePrompt;
    l.plen = strlen(prompt);
    l.oldpos = 1;
    l.pos = 0;
    l.len = 0;
    l.totalUTF8Chars = 0;
    l.cols = getColumns(stdin_fd, stdout_fd);
    l.rows = getRows(stdin_fd, stdout_fd);
    l.maxrows = 0;
    l.history_index = 0;
    l.search = false;
    l.render = true;
    l.hasMoreData = false;
    l.search_maxrows = 0;
    l.y_scroll = 0;
    l.clear_screen = false;
    l.continuePromptActive = true;
    l.insert = false;

    /* Buffer starts empty. */
    l.buf[0] = '\0';
    l.buflen--; /* Make sure there is always space for the nulterm */

#ifdef _WIN32
    std::string uft8Buf = "";
    bool readingUTF8 = false;
#endif

    const char ctrl_c = '\3';

    /* The latest history entry is always our current buffer, that
     * initially is just an empty string. */
    linenoiseHistoryAdd("");

    if (write(l.ofd, prompt, l.plen) == -1)
        return -1;
    while (1) {
        EscapeSequence current_sequence = EscapeSequence::INVALID;
        char c;
        int nread;

#ifdef _WIN32
        char seq[5];
        if (readingUTF8) {
            c = uft8Buf[0];
            uft8Buf.erase(0, 1);
            if (!c) {
                readingUTF8 = false;
            }
        }
        if (!readingUTF8) {
            nread = win32read(seq);
            for (int i = 0; i < nread; i++) {
                uft8Buf += seq[i];
            }
            c = uft8Buf[0];
            uft8Buf.erase(0, 1);
            if (nread > 1) {
                readingUTF8 = true;
            }
        }
#else
        nread = read(l.ifd, &c, 1);
#endif
        if (nread <= 0)
            return l.len;

        l.hasMoreData = pastedInput(l.ifd);
        l.render = true;
        l.insert = false;
        if (!l.hasMoreData) {
            size_t new_cols = getColumns(stdin_fd, stdout_fd);
            size_t new_rows = getRows(stdin_fd, stdout_fd);
            if (new_cols != l.cols || new_rows != l.rows) {
                // terminal resize! re-compute max lines
                l.cols = new_cols;
                l.rows = new_rows;
                size_t rows, cols;
                int cursor_row, cursor_col;
                positionToColAndRow(l.pos, cursor_row, cursor_col, rows, cols, &l);
                l.oldpos = cursor_row;
                if (mlmode) {
                    l.maxrows = rows;
                }
            }
        }

        if (l.search) {
            char ret = linenoiseSearch(&l, c);
            if (l.search || ret == '\0') {
                // still searching - continue searching
                continue;
            }
            // run subsequent command
            c = ret;
        }

        /* Only autocomplete when the callback is set. It returns < 0 when
         * there was an error reading from fd. Otherwise it will return the
         * character that should be handled next. */
        if (c == TAB && completionCallback != NULL) {
            if (l.hasMoreData) {
                for (int i = 0; i < 4; i++) {
                    if (linenoiseEditInsert(&l, ' '))
                        return -1;
                }
                continue;
            }
            int res = completeLine(current_sequence, &l);
            /* Return on errors */
            if (res < 0)
                return l.len;
            c = res;
            /* Read the next character when 0 */
            if (c == 0)
                continue;
        }

        switch (c) {
        case CTRL_G:
        case CTRL_C: /* ctrl-c */
            if (mlmode) {
                l.continuePromptActive = false;
                // force a refresh by setting pos to 0
                l.pos = 0;
                linenoiseEditMoveEnd(&l);
            }
            l.buf[0] = ctrl_c;
            // we keep track of whether or not the line was empty by writing \3 to the second
            // position of the line this is because at a higher level we might want to know if we
            // pressed ctrl c to clear the line or to exit the process
            if (l.len > 0) {
                l.buf[1] = ctrl_c;
                l.buf[2] = '\0';
                l.pos = 2;
                l.len = 2;
            } else {
                l.buf[1] = '\0';
                l.pos = 1;
                l.len = 1;
            }
            return (int)l.len;
        case ENTER:
            if (mlmode && l.len > 0) {
                // check if this forms a complete Cypher statement or not or if enter is pressed in
                // the middle of a line
                l.buf[l.len] = '\0';
                if (l.buf[0] != ':' &&
                    (l.pos != l.len || linenoiseAllWhitespace(l.buf) || !cypherComplete(l.buf))) {
                    if (linenoiseEditInsertMulti(&l, "\r\n")) {
                        return -1;
                    }
                    break;
                }
            }
            [[fallthrough]];
        case 10: /* ctrl+j */ {
            history_len--;
            free(history[history_len]);
            l.continuePromptActive = false;
            if (mlmode) {
                if (l.pos == l.len) {
                    // already at the end - only refresh
                    refreshLine(&l);
                } else {
                    linenoiseEditMoveEnd(&l);
                }
            }
            if (hintsCallback) {
                /* Force a refresh without hints to leave the previous
                 * line as the user typed it after a newline. */
                linenoiseHintsCallback* hc = hintsCallback;
                hintsCallback = NULL;
                refreshLine(&l);
                hintsCallback = hc;
            } else {
                refreshLine(&l);
            }
            // rewrite \r\n to \n
            uint64_t new_len = 0;
            for (uint64_t i = 0; i < l.len; i++) {
                if (l.buf[i] == '\r' && l.buf[i + 1] == '\n') {
                    continue;
                }
                l.buf[new_len++] = l.buf[i];
            }
            l.buf[new_len] = '\0';
            return (int)new_len;
        }
        case BACKSPACE: /* backspace */
#ifdef _WIN32
            /* delete in _WIN32*/
            /* win32read() will send 127 for DEL and 8 for BS and Ctrl-H */
            linenoiseEditDelete(&l);
            break;
#endif
        case 8: /* ctrl-h */
            linenoiseEditBackspace(&l);
            break;
        case CTRL_D: /* ctrl-d, remove char at right of cursor, or if the
                        line is empty, act as end-of-file. */
            if (l.len > 0) {
                linenoiseEditDelete(&l);
            } else {
                history_len--;
                free(history[history_len]);
                return -1;
            }
            break;
        case CTRL_T: /* ctrl-t, swaps current character with previous. */
            if (l.pos > 0 && l.pos < l.len) {
                char tmpBuffer[128];
                uint32_t prevPos = prevChar(&l);
                uint32_t nextPos = nextChar(&l);
                uint32_t prevCharSize = l.pos - prevPos;
                uint32_t curCharSize = nextPos - l.pos;
                memcpy(tmpBuffer, buf + prevPos, prevCharSize);
                memmove(buf + prevPos, buf + l.pos, curCharSize);
                memcpy(buf + prevPos + curCharSize, tmpBuffer, prevCharSize);
                l.pos = nextPos;
                refreshLine(&l);
            }
            break;
        case CTRL_B: /* ctrl-b */
            linenoiseEditMoveLeft(&l);
            break;
        case CTRL_F: /* ctrl-f */
            linenoiseEditMoveRight(&l);
            break;
        case CTRL_P: /* ctrl-p */
#ifdef _WIN32
            // up arrow gets processed as ctrl-p on windows
            if (linenoiseEditMoveRowUp(&l)) {
                break;
            }
#endif
            linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_PREV);
            break;
        case CTRL_N: /* ctrl-n */
#ifdef _WIN32
            // down arrow gets processed as ctrl-n on windows
            if (linenoiseEditMoveRowDown(&l)) {
                break;
            }
#endif
            linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_NEXT);
            break;
        case CTRL_S:
        case CTRL_R: /* ctrl-r */ {
            // initiate reverse search
            search_mlmode = 1;
            l.search = true;
            l.search_buf = std::string();
            l.search_matches.clear();
            l.search_index = 0;
            l.prev_search_match_history_index = history_len - 1 - l.history_index;
            history_len--;
            free(history[history_len]);
            linenoiseHistoryAdd(l.buf);
            performSearch(&l);
            refreshSearch(&l);
            break;
        }
        case ESC: /* escape sequence */
            EscapeSequence escape;
            if (current_sequence == EscapeSequence::INVALID) {
                // read escape sequence
                escape = linenoiseReadEscapeSequence(l.ifd);
            } else {
                // use stored sequence
                escape = current_sequence;
                current_sequence = EscapeSequence::INVALID;
            }

            switch (escape) {
            case EscapeSequence::CTRL_MOVE_BACKWARDS:
            case EscapeSequence::ALT_B:
                linenoiseEditMoveWordLeft(&l);
                break;
            case EscapeSequence::CTRL_MOVE_FORWARDS:
            case EscapeSequence::ALT_F:
                linenoiseEditMoveWordRight(&l);
                break;
            case EscapeSequence::ALT_BACKSPACE:
                linenoiseEditDeletePrevWord(&l);
                break;
            case EscapeSequence::HOME:
                linenoiseEditMoveHome(&l);
                break;
            case EscapeSequence::END:
                linenoiseEditMoveEnd(&l);
                break;
            case EscapeSequence::UP:
                if (linenoiseEditMoveRowUp(&l)) {
                    break;
                }
                linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_PREV);
                break;
            case EscapeSequence::DOWN:
                if (linenoiseEditMoveRowDown(&l)) {
                    break;
                }
                linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_NEXT);
                break;
            case EscapeSequence::RIGHT:
                linenoiseEditMoveRight(&l);
                break;
            case EscapeSequence::LEFT:
                linenoiseEditMoveLeft(&l);
                break;
            case EscapeSequence::DEL:
                linenoiseEditDelete(&l);
                break;
            default:
                lndebug("Unrecognized escape\n");
                break;
            }
            break;
        case CTRL_U: /* Ctrl+u, delete the whole line. */
            buf[0] = '\0';
            l.pos = l.len = 0;
            refreshLine(&l);
            break;
        case CTRL_K: /* Ctrl+k, delete from current to end of line. */
            buf[l.pos] = '\0';
            l.len = l.pos;
            refreshLine(&l);
            break;
        case CTRL_A: /* Ctrl+a, go to the start of the line */
            linenoiseEditMoveHome(&l);
            break;
        case CTRL_E: /* ctrl+e, go to the end of the line */
            linenoiseEditMoveEnd(&l);
            break;
        case CTRL_L: /* ctrl+l, clear screen */
            linenoiseClearScreen();
            l.clear_screen = true;
            refreshLine(&l);
            break;
        case CTRL_W: /* ctrl+w, delete previous word */
            linenoiseEditDeletePrevWord(&l);
            break;
        default:
            if (linenoiseEditInsert(&l, c))
                return -1;
            if (strlen(l.buf) > buflen - 2 && l.hasMoreData) {
                history_len--;
                free(history[history_len]);
                if (mlmode)
                    linenoiseEditMoveEnd(&l);
                if (hintsCallback) {
                    /* Force a refresh without hints to leave the previous
                     * line as the user typed it after a newline. */
                    linenoiseHintsCallback* hc = hintsCallback;
                    hintsCallback = NULL;
                    refreshLine(&l);
                    hintsCallback = hc;
                } else {
                    refreshLine(&l);
                }
                return (int)l.len;
            }
            break;
        }
    }
    return l.len;
}

/* This special mode is used by linenoise in order to print scan codes
 * on screen for debugging / development purposes. It is implemented
 * by the linenoise_example program using the --keycodes option. */
void linenoisePrintKeyCodes(void) {
    char quit[4];

    printf("Linenoise key codes debugging mode.\n"
           "Press keys to see scan codes. Type 'quit' at any time to exit.\n");
    if (enableRawMode(STDIN_FILENO) == -1)
        return;
    memset(quit, ' ', 4);
    while (1) {
        char c;
        int nread;

        nread = read(STDIN_FILENO, &c, 1);
        if (nread <= 0)
            continue;
        memmove(quit, quit + 1, sizeof(quit) - 1); /* shift string to left. */
        quit[sizeof(quit) - 1] = c;                /* Insert current char on the right. */
        if (memcmp(quit, "quit", sizeof(quit)) == 0)
            break;

        printf("'%c' %02x (%d) (type quit to exit)\n", isprint(c) ? c : '?', (int)c, (int)c);
        printf("\r"); /* Go left edge manually, we are in raw mode. */
        fflush(stdout);
    }
    disableRawMode(STDIN_FILENO);
}

/* This function calls the line editing function linenoiseEdit() using
 * the STDIN file descriptor set in raw mode. */
static int linenoiseRaw(char* buf, size_t buflen, const char* prompt, const char* continuePrompt,
    const char* selectedContinuePrompt) {
    int count;

    if (buflen == 0) {
        errno = EINVAL;
        return -1;
    }

    if (enableRawMode(STDIN_FILENO) == -1)
        return -1;
    count = linenoiseEdit(STDIN_FILENO, STDOUT_FILENO, buf, buflen, prompt, continuePrompt,
        selectedContinuePrompt);
    disableRawMode(STDIN_FILENO);
    printf("\n");
    return count;
}

/* This function is called when linenoise() is called with the standard
 * input file descriptor not attached to a TTY. So for example when the
 * program using linenoise is called in pipe or with a file redirected
 * to its standard input. In this case, we want to be able to return the
 * line regardless of its length (by default we are limited to 4k). */
static char* linenoiseNoTTY(void) {
    const int EOF = -1;

    char* line = NULL;
    size_t len = 0, maxlen = 0;

    while (1) {
        if (len == maxlen) {
            if (maxlen == 0)
                maxlen = 16;
            maxlen *= 2;
            char* oldval = line;
            line = (char*)realloc(line, maxlen);
            if (line == NULL) {
                if (oldval)
                    free(oldval);
                return NULL;
            }
        }
        int c = fgetc(stdin);
        if (c == EOF || c == '\n') {
            if (c == EOF && len == 0) {
                free(line);
                return NULL;
            } else {
                line[len] = '\0';
                return line;
            }
        } else {
            line[len] = c;
            len++;
        }
    }
}

/* The high level function that is the main API of the linenoise library.
 * This function checks if the terminal has basic capabilities, just checking
 * for a blacklist of stupid terminals, and later either calls the line
 * editing function or uses dummy fgets() so that you will be able to type
 * something even in the most desperate of the conditions. */
char* linenoise(const char* prompt, const char* continuePrompt,
    const char* selectedContinuePrompt) {
    char buf[LINENOISE_MAX_LINE];
    int count;

    if (!isatty(STDIN_FILENO)) {
        /* Not a tty: read from file / pipe. In this mode we don't want any
         * limit to the line size, so we call a function to handle that. */
        return linenoiseNoTTY();
    } else if (isUnsupportedTerm()) {
        size_t len;

        printf("%s", prompt);
        fflush(stdout);
        if (fgets(buf, LINENOISE_MAX_LINE, stdin) == NULL)
            return NULL;
        len = strlen(buf);
        while (len && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) {
            len--;
            buf[len] = '\0';
        }
        return strdup(buf);
    } else {
        count =
            linenoiseRaw(buf, LINENOISE_MAX_LINE, prompt, continuePrompt, selectedContinuePrompt);
        if (count == -1)
            return NULL;
        return strdup(buf);
    }
}

/* This is just a wrapper the user may want to call in order to make sure
 * the linenoise returned buffer is freed with the same allocator it was
 * created with. Useful when the main program is using an alternative
 * allocator. */
void linenoiseFree(void* ptr) {
    free(ptr);
}

/* At exit we'll try to fix the terminal to the initial conditions. */
static void linenoiseAtExit(void) {
    disableRawMode(STDIN_FILENO);
    freeHistory();
}
