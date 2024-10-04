
// Generated from Cypher.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, ANY = 46, ADD = 47, ALL = 48, ALTER = 49, AND = 50, AS = 51, 
    ASC = 52, ASCENDING = 53, ATTACH = 54, BEGIN = 55, BY = 56, CALL = 57, 
    CASE = 58, CAST = 59, CHECKPOINT = 60, COLUMN = 61, COMMENT = 62, COMMIT = 63, 
    COMMIT_SKIP_CHECKPOINT = 64, CONTAINS = 65, COPY = 66, COUNT = 67, CREATE = 68, 
    CYCLE = 69, DATABASE = 70, DBTYPE = 71, DEFAULT = 72, DELETE = 73, DESC = 74, 
    DESCENDING = 75, DETACH = 76, DISTINCT = 77, DROP = 78, ELSE = 79, END = 80, 
    ENDS = 81, EXISTS = 82, EXPLAIN = 83, EXPORT = 84, EXTENSION = 85, FALSE = 86, 
    FROM = 87, GLOB = 88, GRAPH = 89, GROUP = 90, HEADERS = 91, HINT = 92, 
    IMPORT = 93, IF = 94, IN = 95, INCREMENT = 96, INSTALL = 97, IS = 98, 
    JOIN = 99, KEY = 100, LIMIT = 101, LOAD = 102, MACRO = 103, MATCH = 104, 
    MAXVALUE = 105, MERGE = 106, MINVALUE = 107, MULTI_JOIN = 108, NO = 109, 
    NODE = 110, NOT = 111, NONE = 112, NULL_ = 113, ON = 114, ONLY = 115, 
    OPTIONAL = 116, OR = 117, ORDER = 118, PRIMARY = 119, PROFILE = 120, 
    PROJECT = 121, READ = 122, REL = 123, RENAME = 124, RETURN = 125, ROLLBACK = 126, 
    ROLLBACK_SKIP_CHECKPOINT = 127, SEQUENCE = 128, SET = 129, SHORTEST = 130, 
    START = 131, STARTS = 132, TABLE = 133, THEN = 134, TO = 135, TRANSACTION = 136, 
    TRUE = 137, TYPE = 138, UNION = 139, UNWIND = 140, USE = 141, WHEN = 142, 
    WHERE = 143, WITH = 144, WRITE = 145, XOR = 146, SINGLE = 147, DECIMAL = 148, 
    STAR = 149, L_SKIP = 150, INVALID_NOT_EQUAL = 151, MINUS = 152, FACTORIAL = 153, 
    COLON = 154, StringLiteral = 155, EscapedChar = 156, DecimalInteger = 157, 
    HexLetter = 158, HexDigit = 159, Digit = 160, NonZeroDigit = 161, NonZeroOctDigit = 162, 
    ZeroDigit = 163, RegularDecimalReal = 164, UnescapedSymbolicName = 165, 
    IdentifierStart = 166, IdentifierPart = 167, EscapedSymbolicName = 168, 
    SP = 169, WHITESPACE = 170, CypherComment = 171, Unknown = 172
  };

  explicit CypherLexer(antlr4::CharStream *input);

  ~CypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

