
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
    FROM = 51, COLUMN = 52, EXPORT = 53, IMPORT = 54, DATABASE = 55, NODE = 56, 
    TABLE = 57, GROUP = 58, RDFGRAPH = 59, DROP = 60, ALTER = 61, DEFAULT = 62, 
    RENAME = 63, ADD = 64, PRIMARY = 65, KEY = 66, REL = 67, TO = 68, EXPLAIN = 69, 
    PROFILE = 70, BEGIN = 71, TRANSACTION = 72, READ = 73, ONLY = 74, WRITE = 75, 
    COMMIT = 76, COMMIT_SKIP_CHECKPOINT = 77, ROLLBACK = 78, ROLLBACK_SKIP_CHECKPOINT = 79, 
    INSTALL = 80, EXTENSION = 81, UNION = 82, ALL = 83, LOAD = 84, HEADERS = 85, 
    OPTIONAL = 86, MATCH = 87, UNWIND = 88, CREATE = 89, MERGE = 90, ON = 91, 
    SET = 92, DETACH = 93, DELETE = 94, WITH = 95, RETURN = 96, DISTINCT = 97, 
    STAR = 98, AS = 99, ORDER = 100, BY = 101, L_SKIP = 102, LIMIT = 103, 
    ASCENDING = 104, ASC = 105, DESCENDING = 106, DESC = 107, WHERE = 108, 
    SHORTEST = 109, OR = 110, XOR = 111, AND = 112, NOT = 113, INVALID_NOT_EQUAL = 114, 
    MINUS = 115, FACTORIAL = 116, COLON = 117, IN = 118, STARTS = 119, ENDS = 120, 
    CONTAINS = 121, IS = 122, NULL_ = 123, TRUE = 124, FALSE = 125, COUNT = 126, 
    EXISTS = 127, CASE = 128, ELSE = 129, END = 130, WHEN = 131, THEN = 132, 
    StringLiteral = 133, EscapedChar = 134, DecimalInteger = 135, HexLetter = 136, 
    HexDigit = 137, Digit = 138, NonZeroDigit = 139, NonZeroOctDigit = 140, 
    ZeroDigit = 141, RegularDecimalReal = 142, UnescapedSymbolicName = 143, 
    IdentifierStart = 144, IdentifierPart = 145, EscapedSymbolicName = 146, 
    SP = 147, WHITESPACE = 148, Comment = 149, Unknown = 150
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

