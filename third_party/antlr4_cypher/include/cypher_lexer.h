
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
    T__44 = 45, ADD = 46, ALL = 47, ALTER = 48, AND = 49, AS = 50, ASC = 51, 
    ASCENDING = 52, ATTACH = 53, BEGIN = 54, BY = 55, CALL = 56, CASE = 57, 
    CAST = 58, COLUMN = 59, COMMENT = 60, COMMIT = 61, COMMIT_SKIP_CHECKPOINT = 62, 
    CONTAINS = 63, COPY = 64, COUNT = 65, CREATE = 66, CYCLE = 67, DATABASE = 68, 
    DBTYPE = 69, DEFAULT = 70, DELETE = 71, DESC = 72, DESCENDING = 73, 
    DETACH = 74, DISTINCT = 75, DROP = 76, ELSE = 77, END = 78, ENDS = 79, 
    EXISTS = 80, EXPLAIN = 81, EXPORT = 82, EXTENSION = 83, EXTERNAL = 84, 
    FALSE = 85, FROM = 86, GLOB = 87, GRAPH = 88, GROUP = 89, HEADERS = 90, 
    IMPORT = 91, IF = 92, IN = 93, INCREMENT = 94, INSTALL = 95, IS = 96, 
    KEY = 97, LIMIT = 98, LOAD = 99, MACRO = 100, MATCH = 101, MAXVALUE = 102, 
    MERGE = 103, MINVALUE = 104, NO = 105, NODE = 106, NOT = 107, NULL_ = 108, 
    ON = 109, ONLY = 110, OPTIONAL = 111, OR = 112, ORDER = 113, PRIMARY = 114, 
    PROFILE = 115, PROJECT = 116, RDFGRAPH = 117, READ = 118, REL = 119, 
    RENAME = 120, RETURN = 121, ROLLBACK = 122, ROLLBACK_SKIP_CHECKPOINT = 123, 
    SEQUENCE = 124, SET = 125, SHORTEST = 126, START = 127, STARTS = 128, 
    TABLE = 129, THEN = 130, TO = 131, TRANSACTION = 132, TRUE = 133, TYPE = 134, 
    UNION = 135, UNWIND = 136, USE = 137, WHEN = 138, WHERE = 139, WITH = 140, 
    WRITE = 141, XOR = 142, DECIMAL = 143, STAR = 144, L_SKIP = 145, INVALID_NOT_EQUAL = 146, 
    MINUS = 147, FACTORIAL = 148, COLON = 149, StringLiteral = 150, EscapedChar = 151, 
    DecimalInteger = 152, HexLetter = 153, HexDigit = 154, Digit = 155, 
    NonZeroDigit = 156, NonZeroOctDigit = 157, ZeroDigit = 158, RegularDecimalReal = 159, 
    UnescapedSymbolicName = 160, IdentifierStart = 161, IdentifierPart = 162, 
    EscapedSymbolicName = 163, SP = 164, WHITESPACE = 165, CypherComment = 166, 
    Unknown = 167
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

