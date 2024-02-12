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
#define LINENOISE_MAX_LINE 4096
#define LINENOISE_HISTORY_NEXT 0
#define LINENOISE_HISTORY_PREV 1
static const char* unsupported_term[] = {"dumb", "cons25", "emacs", NULL};
static linenoiseCompletionCallback* completionCallback = NULL;
static linenoiseHintsCallback* hintsCallback = NULL;
static linenoiseFreeHintsCallback* freeHintsCallback = NULL;
static linenoiseHighlightCallback* highlightCallback = NULL;

#ifndef _WIN32
static struct termios orig_termios; /* In order to restore at exit.*/
#endif
static int maskmode = 0;          /* Show "***" instead of input. For passwords. */
static int rawmode = 0;           /* For atexit() function to check if restore is needed*/
static int mlmode = 0;            /* Multi line mode. Default is single line. */
static int atexit_registered = 0; /* Register atexit just 1 time. */
static int history_max_len = LINENOISE_DEFAULT_HISTORY_MAX_LEN;
static int history_len = 0;
static char** history = NULL;

struct searchMatch {
    size_t history_index;
    size_t match_start;
    size_t match_end;
};

/* The linenoiseState structure represents the state during line editing.
 * We pass this state to functions implementing specific editing
 * functionalities. */
struct linenoiseState {
    int ifd;               /* Terminal stdin file descriptor. */
    int ofd;               /* Terminal stdout file descriptor. */
    char* buf;             /* Edited line buffer. */
    size_t buflen;         /* Edited line buffer size. */
    const char* prompt;    /* Prompt to display. */
    size_t plen;           /* Prompt length. */
    size_t pos;            /* Current cursor position. */
    size_t oldpos;         /* Previous refresh cursor position. */
    size_t len;            /* Current edited line length. */
    size_t totalUTF8Chars; /* Number of utf-8 chars in buffer. */
    size_t cols;           /* Number of columns in terminal. */
    size_t maxrows;        /* Maximum num of rows used so far (multiline mode) */
    int history_index;     /* The history index we are currently editing. */
    bool search;           /* Whether or not we are searching our history */
    std::string search_buf;                  //! The search buffer
    std::vector<searchMatch> search_matches; //! The set of search matches in our history
    std::string prev_search_match;           //! The previous search match
    int prev_search_match_history_index;     //! The previous search match history index
    size_t search_index;                     //! The current match index
};

enum KEY_ACTION {
    KEY_NULL = 0,   /* NULL */
    CTRL_A = 1,     /* Ctrl+a */
    CTRL_B = 2,     /* Ctrl-b */
    CTRL_C = 3,     /* Ctrl-c */
    CTRL_D = 4,     /* Ctrl-d */
    CTRL_E = 5,     /* Ctrl-e */
    CTRL_F = 6,     /* Ctrl-f */
    CTRL_G = 7,     /* Ctrl-g */
    CTRL_H = 8,     /* Ctrl-h */
    TAB = 9,        /* Tab */
    CTRL_K = 11,    /* Ctrl+k */
    CTRL_L = 12,    /* Ctrl+l */
    ENTER = 13,     /* Enter */
    CTRL_N = 14,    /* Ctrl-n */
    CTRL_P = 16,    /* Ctrl-p */
    CTRL_R = 18,    /* Ctrl-r */
    CTRL_S = 19,
    CTRL_T = 20,    /* Ctrl-t */
    CTRL_U = 21,    /* Ctrl+u */
    CTRL_W = 23,    /* Ctrl+w */
    ESC = 27,       /* Escape */
    BACKSPACE = 127 /* Backspace */
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

#ifdef _WIN32
#ifndef STDIN_FILENO
#define STDIN_FILENO (_fileno(stdin))
#endif
#ifndef STDOUT_FILENO
#define STDOUT_FILENO (_fileno(stdout))
#endif

HANDLE hOut;
HANDLE hIn;
DWORD consolemode;

/* ======================= Low level terminal handling ====================== */

static int win32read(char* c) {

    DWORD foo;
    INPUT_RECORD b;
    KEY_EVENT_RECORD e;

    while (1) {
        if (!ReadConsoleInput(hIn, &b, 1, &foo))
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
                if (*c)
                    return 1;
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
void linenoiseSetMultiLine(int ml) {
    mlmode = ml;
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
        /* Init windows console handles only once */
        hOut = GetStdHandle(STD_OUTPUT_HANDLE);
        if (hOut == INVALID_HANDLE_VALUE)
            goto fatal;

        if (!GetConsoleMode(hOut, &consolemode)) {
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

        GetConsoleMode(hIn, &consolemode);
        SetConsoleMode(hIn, ENABLE_PROCESSED_INPUT);

        /* Cleanup them at exit */
        atexit(linenoiseAtExit);
        atexit_registered = 1;
    }

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

/* ============================== Completion ================================ */

/* Free a list of completion option populated by linenoiseAddCompletion(). */
static void freeCompletions(linenoiseCompletions* lc) {
    size_t i;
    for (i = 0; i < lc->len; i++)
        free(lc->cvec[i]);
    if (lc->cvec != NULL)
        free(lc->cvec);
}

/* This is an helper function for linenoiseEdit() and is called when the
 * user types the <tab> key in order to complete the string currently in the
 * input.
 *
 * The state of the editing is encapsulated into the pointed linenoiseState
 * structure as described in the structure definition. */
static int completeLine(struct linenoiseState* ls) {
    linenoiseCompletions lc = {0, NULL};
    int nread, nwritten;
    char c = 0;

    completionCallback(ls->buf, &lc);
    if (lc.len == 0) {
        linenoiseBeep();
    } else {
        size_t stop = 0, i = 0;

        while (!stop) {
            /* Show completion or original buffer */
            if (i < lc.len) {
                struct linenoiseState saved = *ls;

                ls->len = ls->pos = strlen(lc.cvec[i]);
                ls->buf = lc.cvec[i];
                refreshLine(ls);
                ls->len = saved.len;
                ls->pos = saved.pos;
                ls->buf = saved.buf;
            } else {
                refreshLine(ls);
            }

            nread = read(ls->ifd, &c, 1);
            if (nread <= 0) {
                freeCompletions(&lc);
                return -1;
            }

            switch (c) {
            case 9: /* tab */
                i = (i + 1) % (lc.len + 1);
                if (i == lc.len)
                    linenoiseBeep();
                break;
            case 27: /* escape */
                /* Re-show original buffer */
                if (i < lc.len)
                    refreshLine(ls);
                stop = 1;
                break;
            default:
                /* Update buffer and return */
                if (i < lc.len) {
                    nwritten = snprintf(ls->buf, ls->buflen, "%s", lc.cvec[i]);
                    ls->len = ls->pos = nwritten;
                }
                stop = 1;
                break;
            }
        }
    }

    freeCompletions(&lc);
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
struct abuf {
    char* b;
    int len;
};

static void abInit(struct abuf* ab) {
    ab->b = NULL;
    ab->len = 0;
}

static void abAppend(struct abuf* ab, const char* s, int len) {
    char* new_entry = (char*)realloc(ab->b, ab->len + len);

    if (new_entry == NULL)
        return;
    memcpy(new_entry + ab->len, s, len);
    ab->b = new_entry;
    ab->len += len;
}

static void abFree(struct abuf* ab) {
    free(ab->b);
}

/* Helper of refreshSingleLine() and refreshMultiLine() to show hints
 * to the right of the prompt. */
void refreshShowHints(struct abuf* ab, struct linenoiseState* l, int plen) {
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
            abAppend(ab, seq, strlen(seq));
            abAppend(ab, hint, hintlen);
            if (color != -1 || bold != 0)
                abAppend(ab, "\033[0m", 4);
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

static void truncateText(char*& buf, size_t& len, size_t pos, size_t cols, size_t plen,
    bool highlight, size_t& render_pos, char* highlightBuf) {
    if (Utf8Proc::isValid(buf, len)) {
        // UTF8 in prompt, handle rendering.
        size_t remainingRenderWidth = cols - plen - 1;
        size_t startPos = 0;
        size_t charPos = 0;
        size_t prevPos = 0;
        size_t totalRenderWidth = 0;
        size_t renderWidth = 0;
        size_t posCounter = 0;
        while (charPos < len) {
            uint32_t charRenderWidth = Utf8Proc::renderWidth(buf, charPos);
            prevPos = charPos;
            charPos = utf8proc_next_grapheme(buf, len, charPos);
            posCounter++;
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
            if (posCounter <= pos) {
                renderWidth += charRenderWidth;
            }
            if (prevPos < pos) {
                render_pos += charRenderWidth;
            }
        }
        if (highlight) {
            highlightCallback(buf, highlightBuf, totalRenderWidth, renderWidth);
            len = strlen(highlightBuf);
        } else {
            buf = buf + startPos;
            len = charPos - startPos;
        }
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
    char seq[64];
    uint32_t plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
    int fd = l->ofd;
    char buf[LINENOISE_MAX_LINE];
    size_t len = l->len;
    size_t pos = l->pos;
    size_t chars = l->totalUTF8Chars;
    struct abuf ab;
    size_t renderPos = 0;

    truncateText(l->buf, len, pos, l->cols, plen, true, renderPos, buf);
    
    abInit(&ab);
    /* Cursor to left edge */
    snprintf(seq, 64, "\r");
    abAppend(&ab, seq, strlen(seq));
    /* Write the prompt and the current buffer content */
    abAppend(&ab, l->prompt, strlen(l->prompt));
    if (maskmode == 1) {
        while (chars--)
            abAppend(&ab, "*", 1);
    } else {
        abAppend(&ab, buf, len);
    }
    /* Show hits if any. */
    refreshShowHints(&ab, l, plen);
    /* Erase to right */
    snprintf(seq, 64, "\x1b[0K");
    abAppend(&ab, seq, strlen(seq));
    /* Move cursor to original position. */
    snprintf(seq, 64, "\r\x1b[%dC", (int)(renderPos + plen));
    abAppend(&ab, seq, strlen(seq));

    if (write(fd, ab.b, ab.len) == -1) {} /* Can't recover from write error. */
    abFree(&ab);
}

/* Multi line low level line refresh.
 *
 * Rewrite the currently edited line accordingly to the buffer content,
 * cursor position, and number of columns of the terminal. */
static void refreshMultiLine(struct linenoiseState* l) {
    char seq[64];
    uint32_t plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
    uint32_t totalLen = linenoiseComputeRenderWidth(l->buf, l->len);
    uint32_t cursorOldPos = linenoiseComputeRenderWidth(l->buf, l->oldpos);
    uint32_t cursorPos = linenoiseComputeRenderWidth(l->buf, l->pos);
    int rows = (plen + totalLen + l->cols - 1) / l->cols; /* Rows used by current buf. */
    int rpos = (plen + cursorOldPos + l->cols) / l->cols; /* Cursor relative row. */
    int rpos2;                                            /* rpos after refresh. */
    int col;                                              /* Colum position, zero-based. */
    int old_rows = l->maxrows;
    int fd = l->ofd, j;
    struct abuf ab;

    /* Update maxrows if needed. */
    if (rows > (int)l->maxrows)
        l->maxrows = rows;

    /* First step: clear all the lines used before. To do so start by
     * going to the last row. */
    abInit(&ab);
    if (old_rows - rpos > 0) {
        lndebug("go down %d", old_rows - rpos);
        snprintf(seq, 64, "\x1b[%dB", old_rows - rpos);
        abAppend(&ab, seq, strlen(seq));
    }

    /* Now for every row clear it, go up. */
    for (j = 0; j < old_rows - 1; j++) {
        lndebug("clear+up");
        snprintf(seq, 64, "\r\x1b[0K\x1b[1A");
        abAppend(&ab, seq, strlen(seq));
    }

    /* Clean the top line. */
    lndebug("clear");
    snprintf(seq, 64, "\r\x1b[0K");
    abAppend(&ab, seq, strlen(seq));

    /* Write the prompt and the current buffer content */
    abAppend(&ab, l->prompt, strlen(l->prompt));
    if (maskmode == 1) {
        unsigned int i;
        for (i = 0; i < l->len; i++)
            abAppend(&ab, "*", 1);
    } else {
        abAppend(&ab, l->buf, l->len);
    }

    /* Show hits if any. */
    refreshShowHints(&ab, l, plen);

    /* If we are at the very end of the screen with our prompt, we need to
     * emit a newline and move the prompt to the first column. */
    if (l->pos && l->pos == l->len && (cursorPos + plen) % l->cols == 0) {
        lndebug("<newline>");
        abAppend(&ab, "\n", 1);
        snprintf(seq, 64, "\r");
        abAppend(&ab, seq, strlen(seq));
        rows++;
        if (rows > (int)l->maxrows)
            l->maxrows = rows;
    }

    /* Move cursor to right position. */
    rpos2 = (plen + cursorPos + l->cols) / l->cols; /* current cursor relative row. */
    lndebug("rpos2 %d", rpos2);

    /* Go up till we reach the expected positon. */
    if (rows - rpos2 > 0) {
        lndebug("go-up %d", rows - rpos2);
        snprintf(seq, 64, "\x1b[%dA", rows - rpos2);
        abAppend(&ab, seq, strlen(seq));
    }

    /* Set column. */
    col = (plen + (int)cursorPos) % (int)l->cols;
    lndebug("set col %d", 1 + col);
    if (col)
        snprintf(seq, 64, "\r\x1b[%dC", col);
    else
        snprintf(seq, 64, "\r");
    abAppend(&ab, seq, strlen(seq));

    lndebug("\n");
    l->oldpos = l->pos;

    if (write(fd, ab.b, ab.len) == -1) {} /* Can't recover from write error. */
    abFree(&ab);
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

#ifndef _WIN32
    std::string matchedWord;
    auto searchPhrasePos = 0u;
    bool matching = false;
    bool matched = false;
    bool ansiCode = false;

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
#endif
    if (underlinedBuf.empty()) {
        strncpy(resultBuf, buf.c_str(), LINENOISE_MAX_LINE - 1);
    } else {
        strncpy(resultBuf, underlinedBuf.c_str(), LINENOISE_MAX_LINE - 1);
    }
}

static void refreshSearchMultiLine(struct linenoiseState* l, const char* searchPrompt, const char* searchText) {
    char seq[64];
    uint32_t plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
    int rows = 1;                                         /* Rows used by current buf. */
    int rpos = 1;                                         /* Cursor relative row. */
    int rpos2;                                            /* rpos after refresh. */
    int old_rows = l->maxrows;
    int fd = l->ofd, j;
    struct abuf ab;
    char highlightBuf[LINENOISE_MAX_LINE];
    char buf[LINENOISE_MAX_LINE];
    size_t len = l->len;
    size_t render_pos = 0;
    l->maxrows = 1;

    truncateText(l->buf, len, l->pos, l->cols, plen, true, render_pos, highlightBuf);
    if (strlen(highlightBuf) > 0) {
        highlightSearchMatch(highlightBuf, buf, searchText);
    } else {
        highlightSearchMatch(l->buf, buf, searchText);
    }
    len = strlen(buf);

    /* First step: clear all the lines used before. To do so start by
     * going to the last row. */
    abInit(&ab);
    if (old_rows - rpos > 0) {
        lndebug("go down %d", old_rows - rpos);
        snprintf(seq, 64, "\x1b[%dB", old_rows - rpos);
        abAppend(&ab, seq, strlen(seq));
    }

    /* Now for every row clear it, go up. */
    for (j = 0; j < old_rows - 1; j++) {
        lndebug("clear+up");
        snprintf(seq, 64, "\r\x1b[0K\x1b[1A");
        abAppend(&ab, seq, strlen(seq));
    }

    /* Clean the top line. */
    lndebug("clear");
    snprintf(seq, 64, "\r\x1b[0K");
    abAppend(&ab, seq, strlen(seq));

    /* Cursor to left edge */
    snprintf(seq, 64, "\r");
    abAppend(&ab, seq, strlen(seq));
    /* Write the prompt and the current buffer content */
    abAppend(&ab, l->prompt, strlen(l->prompt));
    if (maskmode == 1) {
        unsigned int i;
        for (i = 0; i < len; i++)
            abAppend(&ab, "*", 1);
    } else {
        abAppend(&ab, buf, len);
    }

    /* Erase to right */
    snprintf(seq, 64, "\x1b[0K");
    abAppend(&ab, seq, strlen(seq));
    if (strlen(searchPrompt) > 0) {
        lndebug("<newline>");
        abAppend(&ab, "\n", 1);
        /* Cursor to left edge */
        snprintf(seq, 64, "\r");
        abAppend(&ab, seq, strlen(seq));
        rows = 2;
        l->maxrows = 2;
        /* Write the search prompt content */
        abAppend(&ab, searchPrompt, strlen(searchPrompt));
        /* Erase to right */
        snprintf(seq, 64, "\x1b[0K");
        abAppend(&ab, seq, strlen(seq));
    }

    /* Show hits if any. */
    refreshShowHints(&ab, l, plen);

    /* Move cursor to right position. */
    rpos2 = 1;                        /* current cursor relative row. */
    lndebug("rpos2 %d", rpos2);

    /* Go up till we reach the expected positon. */
    if (strlen(searchPrompt) > 0) {
        lndebug("go-up %d", rows - rpos2);
        snprintf(seq, 64, "\x1b[%dA", rows - rpos2);
        abAppend(&ab, seq, strlen(seq));
    }

    /* Move cursor to original position. */
    snprintf(seq, 64, "\r\x1b[%dC", (int)(render_pos + plen));
    abAppend(&ab, seq, strlen(seq));

    l->oldpos = l->pos;

    if (write(fd, ab.b, ab.len) == -1) {} /* Can't recover from write error. */
    abFree(&ab);
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

        char* search_buf = (char*) search_text.c_str();
        size_t search_len = search_text.size();
        
        size_t cols = l->cols - search_prompt.length() - 1;

        size_t render_pos = 0;
        char emptyHighlightBuf[LINENOISE_MAX_LINE];
        truncateText(search_buf, search_len, search_len, cols, false, 0, render_pos, emptyHighlightBuf);
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
    char* tempBuf = l->buf;
    l->len = 0;
    l->pos = 0;
    l->buf = (char*)"";
    refreshSearchMultiLine(l, (char*)"", (char*)"");
    l->buf = tempBuf;
    l->len = strlen(tempBuf);
    l->pos = l->len;

    history_len--;
    free(history[history_len]);
    linenoiseHistoryAdd("");

    l->search = false;
    l->search_buf = std::string();
    l->search_matches.clear();
    l->search_index = 0;
    refreshLine(l);
}

static char acceptSearch(linenoiseState* l, char nextCommand) {
    int history_index = l->prev_search_match_history_index;
    if (l->search_index < l->search_matches.size()) {
        // if there is a match - copy it into the buffer
        auto match = l->search_matches[l->search_index];
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
        l->len = l->pos = strlen(l->buf);
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
    FD_SET(ifd, &readfds);
    int isPasted = select(0, &readfds, NULL, NULL, NULL);
    return (isPasted != 0 && isPasted != SOCKET_ERROR);
#else
    struct pollfd fd {
        ifd, POLLIN, 0
    };
    int isPasted = poll(&fd, 1, 0);
    return (isPasted != 0);
#endif
}

static char linenoiseSearch(linenoiseState *l, char c) {
    char seq[64];

	switch (c) {
	case ENTER: /* enter */
		// accept search and run
		return acceptSearch(l, ENTER);
	case CTRL_R:
		// move to the next match index
		searchNext(l);
		break;
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
        if (pastedInput(c)) {
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
	case CTRL_P:
		searchPrev(l);
		break;
	case CTRL_N:
		searchNext(l);
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
int linenoiseEditInsert(struct linenoiseState* l, char c) {
    if (l->len < l->buflen) {
        if (l->len == l->pos) {
            l->buf[l->pos] = c;
            l->pos++;
            l->len++;
            l->buf[l->len] = '\0';
            if ((!mlmode && l->plen + l->len < l->cols && !hintsCallback)) {
                /* Avoid a full update of the line in the
                 * trivial case. */
                if (write(l->ofd, &c, 1) == -1)
                    return -1;
            } else {
                refreshLine(l);
            }
        } else {
            memmove(l->buf + l->pos + 1, l->buf + l->pos, l->len - l->pos);
            l->buf[l->pos] = c;
            l->len++;
            l->pos++;
            l->buf[l->len] = '\0';
            refreshLine(l);
        }
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

/* This function is the core of the line editing capability of linenoise.
 * It expects 'fd' to be already in "raw mode" so that every key pressed
 * will be returned ASAP to read().
 *
 * The resulting string is put into 'buf' when the user type enter, or
 * when ctrl+d is typed.
 *
 * The function returns the length of the current buffer. */
static int linenoiseEdit(
    int stdin_fd, int stdout_fd, char* buf, size_t buflen, const char* prompt) {
    struct linenoiseState l;

    /* Populate the linenoise state that we pass to functions implementing
     * specific editing functionalities. */
    l.ifd = stdin_fd;
    l.ofd = stdout_fd;
    l.buf = buf;
    l.buflen = buflen;
    l.prompt = prompt;
    l.plen = strlen(prompt);
    l.oldpos = l.pos = 0;
    l.len = 0;
    l.totalUTF8Chars = 0;
    l.cols = getColumns(stdin_fd, stdout_fd);
    l.maxrows = 0;
    l.history_index = 0;
    l.search = false;

    /* Buffer starts empty. */
    l.buf[0] = '\0';
    l.buflen--; /* Make sure there is always space for the nulterm */

    const char ctrl_c = '\3';

    /* The latest history entry is always our current buffer, that
     * initially is just an empty string. */
    linenoiseHistoryAdd("");

    if (write(l.ofd, prompt, l.plen) == -1)
        return -1;
    while (1) {
        char c;
        int nread;
        char seq[5];

#ifdef _WIN32
        nread = win32read(&c);
#else
        nread = read(l.ifd, &c, 1);
#endif
        if (nread <= 0)
            return l.len;

        l.cols = getColumns(stdin_fd, stdout_fd);

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
            if (pastedInput(l.ifd)) {
                for (int i = 0; i < 4; i++) {
                    if (linenoiseEditInsert(&l, ' '))
                        return -1;
                }
                continue;
            }
            c = completeLine(&l);
            /* Return on errors */
            if (c < 0)
                return l.len;
            /* Read next character when 0 */
            if (c == 0)
                continue;
        }
        switch (c) {
        case CTRL_G:
        case CTRL_C: /* ctrl-c */
            if (mlmode) {
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
        case 10:
        case ENTER:  /* enter */
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
            }
            return (int)l.len;
        case BACKSPACE: /* backspace */
#ifdef _WIN32
            /* delete in _WIN32*/
            /* win32read() will send 127 for DEL and 8 for BS and Ctrl-H */
            if (l.pos < l.len && l.len > 0) {
                memmove(buf + l.pos, buf + l.pos + 1, l.len - l.pos);
                l.len--;
                buf[l.len] = '\0';
                refreshLine(&l);
            }
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
            linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_PREV);
            break;
        case CTRL_N: /* ctrl-n */
            linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_NEXT);
            break;
        case CTRL_S:
        case CTRL_R: /* ctrl-r */ {
            // initiate reverse search
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
            /* Read the next two bytes representing the escape sequence.
             * Use two calls to handle slow terminals returning the two
             * chars at different times. */
            if (read(l.ifd, seq, 1) == -1)
                break;

            if (seq[0] == 'b') {
                linenoiseEditMoveWordLeft(&l);
                break;
            } else if (seq[0] == 'f') {
                linenoiseEditMoveWordRight(&l);
                break;
            }

            if (read(l.ifd, seq + 1, 1) == -1)
                break;
            /* ESC [ sequences. */
            if (seq[0] == '[') {
                if (seq[1] >= '0' && seq[1] <= '9') {
                    /* Extended escape, read additional byte. */
                    if (read(l.ifd, seq + 2, 1) == -1)
                        break;
                    if (seq[2] == '~') {
                        switch (seq[1]) {
                        case '1': /* HOME key. */
                            linenoiseEditMoveHome(&l);
                            break;
                        case '3': /* Delete key. */
                            linenoiseEditDelete(&l);
                            break;
                        case '4': /* END key. */
                            linenoiseEditMoveEnd(&l);
                            break;
                        }
                    }
                    if (seq[2] == ';') {
                        /* read another 2 bytes */
                        if (read(l.ifd, seq + 3, 2) == -1)
                            break;
                        if (memcmp(seq, "[1;5C", 5) == 0) {
                            linenoiseEditMoveWordRight(&l);
                        }
                        if (memcmp(seq, "[1;5D", 5) == 0) {
                            linenoiseEditMoveWordLeft(&l);
                        }
                    }
                } else {
                    switch (seq[1]) {
                    case 'A': /* Up */
                        linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_PREV);
                        break;
                    case 'B': /* Down */
                        linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_NEXT);
                        break;
                    case 'C': /* Right */
                        linenoiseEditMoveRight(&l);
                        break;
                    case 'D': /* Left */
                        linenoiseEditMoveLeft(&l);
                        break;
                    case 'H': /* Home */
                        linenoiseEditMoveHome(&l);
                        break;
                    case 'F': /* End*/
                        linenoiseEditMoveEnd(&l);
                        break;
                    }
                }
            }

            /* ESC O sequences. */
            else if (seq[0] == 'O') {
                switch (seq[1]) {
                case 'H': /* Home */
                    linenoiseEditMoveHome(&l);
                    break;
                case 'F': /* End*/
                    linenoiseEditMoveEnd(&l);
                    break;
                }
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
            refreshLine(&l);
            break;
        case CTRL_W: /* ctrl+w, delete previous word */
            linenoiseEditDeletePrevWord(&l);
            break;
        default:
            if (linenoiseEditInsert(&l, c))
                return -1;
            if (strlen(l.buf) > buflen - 2 && pastedInput(l.ifd)) {
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
static int linenoiseRaw(char* buf, size_t buflen, const char* prompt) {
    int count;

    if (buflen == 0) {
        errno = EINVAL;
        return -1;
    }

    if (enableRawMode(STDIN_FILENO) == -1)
        return -1;
    count = linenoiseEdit(STDIN_FILENO, STDOUT_FILENO, buf, buflen, prompt);
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
char* linenoise(const char* prompt) {
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
        count = linenoiseRaw(buf, LINENOISE_MAX_LINE, prompt);
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

/* At exit we'll try to fix the terminal to the initial conditions. */
static void linenoiseAtExit(void) {
#ifdef _WIN32
    SetConsoleMode(hIn, consolemode);
    CloseHandle(hOut);
    CloseHandle(hIn);
#else
    disableRawMode(STDIN_FILENO);
#endif
    freeHistory();
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

    /* Add an heap allocated copy of the line in the history.
     * If we reached the max length, remove the older line. */
    linecopy = strdup(line);
    if (!linecopy)
        return 0;

    if (!mlmode) {
        std::string singleLineCopy = "";
        int spaceCount = 0;
        // replace all newlines and tabs with spaces
        for (auto i = 0u; i < strlen(linecopy); i++) {
            if (linecopy[i] == '\n' || linecopy[i] == '\r' || linecopy[i] == '\t' ||
                linecopy[i] == ' ') {
                spaceCount++;
                if (spaceCount >= 4) {
                    spaceCount = 0;
                    singleLineCopy += ' ';
                }
            } else {
                for (int j = 0; j < spaceCount; j++) {
                    singleLineCopy += ' ';
                }
                spaceCount = 0;
                singleLineCopy += linecopy[i];
            }
        }
        free(linecopy);
        linecopy = strdup(singleLineCopy.c_str());
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
        }
        // else we are parsing a Cypher statement
        result += buf;
        if (result.back() == ';') {
            // this line contains a full Cypher statement - add it to the history
            linenoiseHistoryAdd(result.c_str());
            result = std::string();
            continue;
        }
        // the result does not contain a full Cypher statement - add a newline deliminator and move on
        // to the next line
        result += "\r\n";
    }
    fclose(fp);

    return 0;
}
