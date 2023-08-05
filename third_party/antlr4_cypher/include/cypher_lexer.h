
// Generated from Cypher.g4 by ANTLR 4.9

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
    T__44 = 45, T__45 = 46, T__46 = 47, CALL = 48, COMMENT = 49, MACRO = 50, 
    GLOB = 51, COPY = 52, FROM = 53, COLUMN = 54, NODE = 55, TABLE = 56, 
    GROUP = 57, RDF = 58, GRAPH = 59, DROP = 60, ALTER = 61, DEFAULT = 62, 
    RENAME = 63, ADD = 64, PRIMARY = 65, KEY = 66, REL = 67, TO = 68, EXPLAIN = 69, 
    PROFILE = 70, BEGIN = 71, TRANSACTION = 72, READ = 73, WRITE = 74, COMMIT = 75, 
    COMMIT_SKIP_CHECKPOINT = 76, ROLLBACK = 77, ROLLBACK_SKIP_CHECKPOINT = 78, 
    UNION = 79, ALL = 80, OPTIONAL = 81, MATCH = 82, UNWIND = 83, CREATE = 84, 
    MERGE = 85, ON = 86, SET = 87, DELETE = 88, WITH = 89, RETURN = 90, 
    DISTINCT = 91, STAR = 92, AS = 93, ORDER = 94, BY = 95, L_SKIP = 96, 
    LIMIT = 97, ASCENDING = 98, ASC = 99, DESCENDING = 100, DESC = 101, 
    WHERE = 102, SHORTEST = 103, OR = 104, XOR = 105, AND = 106, NOT = 107, 
    INVALID_NOT_EQUAL = 108, MINUS = 109, FACTORIAL = 110, STARTS = 111, 
    ENDS = 112, CONTAINS = 113, IS = 114, NULL_ = 115, TRUE = 116, FALSE = 117, 
    EXISTS = 118, CASE = 119, ELSE = 120, END = 121, WHEN = 122, THEN = 123, 
    StringLiteral = 124, EscapedChar = 125, DecimalInteger = 126, HexLetter = 127, 
    HexDigit = 128, Digit = 129, NonZeroDigit = 130, NonZeroOctDigit = 131, 
    ZeroDigit = 132, RegularDecimalReal = 133, UnescapedSymbolicName = 134, 
    IdentifierStart = 135, IdentifierPart = 136, EscapedSymbolicName = 137, 
    SP = 138, WHITESPACE = 139, Comment = 140, Unknown = 141
  };

  explicit CypherLexer(antlr4::CharStream *input);
  ~CypherLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

