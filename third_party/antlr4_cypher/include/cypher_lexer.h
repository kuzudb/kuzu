
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
    T__44 = 45, CALL = 46, COMMENT = 47, MACRO = 48, GLOB = 49, COPY = 50, 
    FROM = 51, COLUMN = 52, EXPORT = 53, DATABASE = 54, NODE = 55, TABLE = 56, 
    GROUP = 57, RDFGRAPH = 58, DROP = 59, ALTER = 60, DEFAULT = 61, RENAME = 62, 
    ADD = 63, PRIMARY = 64, KEY = 65, REL = 66, TO = 67, EXPLAIN = 68, PROFILE = 69, 
    BEGIN = 70, TRANSACTION = 71, READ = 72, ONLY = 73, WRITE = 74, COMMIT = 75, 
    COMMIT_SKIP_CHECKPOINT = 76, ROLLBACK = 77, ROLLBACK_SKIP_CHECKPOINT = 78, 
    INSTALL = 79, EXTENSION = 80, UNION = 81, ALL = 82, LOAD = 83, HEADERS = 84, 
    OPTIONAL = 85, MATCH = 86, UNWIND = 87, CREATE = 88, MERGE = 89, ON = 90, 
    SET = 91, DETACH = 92, DELETE = 93, WITH = 94, RETURN = 95, DISTINCT = 96, 
    STAR = 97, AS = 98, ORDER = 99, BY = 100, L_SKIP = 101, LIMIT = 102, 
    ASCENDING = 103, ASC = 104, DESCENDING = 105, DESC = 106, WHERE = 107, 
    SHORTEST = 108, OR = 109, XOR = 110, AND = 111, NOT = 112, INVALID_NOT_EQUAL = 113, 
    MINUS = 114, FACTORIAL = 115, COLON = 116, IN = 117, STARTS = 118, ENDS = 119, 
    CONTAINS = 120, IS = 121, NULL_ = 122, TRUE = 123, FALSE = 124, COUNT = 125, 
    EXISTS = 126, CASE = 127, ELSE = 128, END = 129, WHEN = 130, THEN = 131, 
    StringLiteral = 132, EscapedChar = 133, DecimalInteger = 134, HexLetter = 135, 
    HexDigit = 136, Digit = 137, NonZeroDigit = 138, NonZeroOctDigit = 139, 
    ZeroDigit = 140, RegularDecimalReal = 141, UnescapedSymbolicName = 142, 
    IdentifierStart = 143, IdentifierPart = 144, EscapedSymbolicName = 145, 
    SP = 146, WHITESPACE = 147, Comment = 148, Unknown = 149
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

