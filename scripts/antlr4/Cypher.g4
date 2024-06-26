
/*
* OpenCypher grammar at "https://s3.amazonaws.com/artifacts.opencypher.org/legacy/Cypher.g4"
*/
grammar Cypher;

// provide ad-hoc error messages for common syntax errors
@parser::declarations {
    virtual void notifyQueryNotConcludeWithReturn(antlr4::Token* startToken) {};
    virtual void notifyNodePatternWithoutParentheses(std::string nodeName, antlr4::Token* startToken) {};
    virtual void notifyInvalidNotEqualOperator(antlr4::Token* startToken) {};
    virtual void notifyEmptyToken(antlr4::Token* startToken) {};
    virtual void notifyReturnNotAtEnd(antlr4::Token* startToken) {};
    virtual void notifyNonBinaryComparison(antlr4::Token* startToken) {};
}

ADD : ( 'A' | 'a' ) ( 'D' | 'd' ) ( 'D' | 'd' ) ;

ALL : ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ;

ALTER : ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'T' | 't' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ;

AND : ( 'A' | 'a' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ;

AS : ( 'A' | 'a' ) ( 'S' | 's' ) ;

ASC : ( 'A' | 'a' ) ( 'S' | 's' ) ( 'C' | 'c' ) ;

ASCENDING : ( 'A' | 'a' ) ( 'S' | 's' ) ( 'C' | 'c' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'G' | 'g' ) ;

ATTACH : ( 'A' | 'a' ) ( 'T' | 't' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'C' | 'c' ) ( 'H' | 'h' ) ;

BEGIN : ( 'B' | 'b' ) ( 'E' | 'e' ) ( 'G' | 'g' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ;

BY : ( 'B' | 'b' ) ( 'Y' | 'y' ) ;

CALL : ( 'C' | 'c' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ;

CASE : ( 'C' | 'c' ) ( 'A' | 'a' ) ( 'S' | 's' ) ( 'E' | 'e' ) ;

CAST : ( 'C' | 'c' ) ( 'A' | 'a' ) ( 'S' | 's' ) ( 'T' | 't' ) ;

COLUMN : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'L' | 'l' ) ( 'U' | 'u' ) ( 'M' | 'm' ) ( 'N' | 'n' ) ;

COMMENT : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'M' | 'm' ) ( 'M' | 'm' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'T' | 't' ) ;

COMMIT : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'M' | 'm' ) ( 'M' | 'm' ) ( 'I' | 'i' ) ( 'T' | 't' ) ;

COMMIT_SKIP_CHECKPOINT : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'M' | 'm' ) ( 'M' | 'm' ) ( 'I' | 'i' ) ( 'T' | 't' ) '_' ( 'S' | 's' ) ( 'K' | 'k' ) ( 'I' | 'i' ) ( 'P' | 'p' ) '_' ( 'C' | 'c' ) ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'C' | 'c' ) ( 'K' | 'k' ) ( 'P' | 'p' ) ( 'O' | 'o' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'T' | 't' ) ;

CONTAINS : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'N' | 'n' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'S' | 's' ) ;

COPY : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'P' | 'p' ) ( 'Y' | 'y' ) ;

COUNT : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'U' | 'u' ) ( 'N' | 'n' ) ( 'T' | 't' ) ;

CREATE : ( 'C' | 'c' ) ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'E' | 'e' ) ;

CYCLE : ( 'C' | 'c' ) ( 'Y' | 'y' ) ( 'C' | 'c' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ;

DATABASE : ( 'D' | 'd' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'B' | 'b' ) ( 'A' | 'a' ) ( 'S' | 's' ) ( 'E' | 'e' ) ;

DBTYPE : ( 'D' | 'd' ) ( 'B' | 'b' ) ( 'T' | 't' ) ( 'Y' | 'y' ) ( 'P' | 'p' ) ( 'E' | 'e' ) ;

DEFAULT : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'F' | 'f' ) ( 'A' | 'a' ) ( 'U' | 'u' ) ( 'L' | 'l' ) ( 'T' | 't' ) ;

DELETE : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'E' | 'e' ) ;

DESC : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'S' | 's' ) ( 'C' | 'c' ) ;

DESCENDING : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'S' | 's' ) ( 'C' | 'c' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'G' | 'g' ) ;

DETACH : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'C' | 'c' ) ( 'H' | 'h' ) ;

DISTINCT : ( 'D' | 'd' ) ( 'I' | 'i' ) ( 'S' | 's' ) ( 'T' | 't' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'C' | 'c' ) ( 'T' | 't' ) ;

DROP : ( 'D' | 'd' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'P' | 'p' ) ;

ELSE : ( 'E' | 'e' ) ( 'L' | 'l' ) ( 'S' | 's' ) ( 'E' | 'e' ) ;

END : ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ;

ENDS : ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ( 'S' | 's' ) ;

EXISTS : ( 'E' | 'e' ) ( 'X' | 'x' ) ( 'I' | 'i' ) ( 'S' | 's' ) ( 'T' | 't' ) ( 'S' | 's' ) ;

EXPLAIN : ( 'E' | 'e' ) ( 'X' | 'x' ) ( 'P' | 'p' ) ( 'L' | 'l' ) ( 'A' | 'a' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ;

EXPORT : ( 'E' | 'e' ) ( 'X' | 'x' ) ( 'P' | 'p' ) ( 'O' | 'o' ) ( 'R' | 'r' ) ( 'T' | 't' ) ;

EXTENSION : ( 'E' | 'e' ) ( 'X' | 'x' ) ( 'T' | 't' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'S' | 's' ) ( 'I' | 'i' ) ( 'O' | 'o' ) ( 'N' | 'n' ) ;

FALSE : ( 'F' | 'f' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'S' | 's' ) ( 'E' | 'e' ) ;

FROM : ( 'F' | 'f' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'M' | 'm' ) ;

GLOB : ( 'G' | 'g' ) ( 'L' | 'l' ) ( 'O' | 'o' ) ( 'B' | 'b' ) ;

GRAPH : ( 'G' | 'g' ) ( 'R' | 'r' ) ( 'A' | 'a' ) ( 'P' | 'p' ) ( 'H' | 'h' ) ;

GROUP : ( 'G' | 'g' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'U' | 'u' ) ( 'P' | 'p' ) ;

HEADERS : ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'A' | 'a' ) ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ( 'S' | 's' ) ;

IMPORT : ( 'I' | 'i' ) ( 'M' | 'm' ) ( 'P' | 'p' ) ( 'O' | 'o' ) ( 'R' | 'r' ) ( 'T' | 't' ) ;

IF : ( 'I' | 'i' ) ( 'F' | 'f' ) ;

IN : ( 'I' | 'i' ) ( 'N' | 'n' ) ;

INCREMENT : ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'C' | 'c' ) ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'M' | 'm' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'T' | 't' ) ;

INSTALL : ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'S' | 's' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ;

IS : ( 'I' | 'i' ) ( 'S' | 's' ) ;

KEY : ( 'K' | 'k' ) ( 'E' | 'e' ) ( 'Y' | 'y' ) ;

LIMIT : ( 'L' | 'l' ) ( 'I' | 'i' ) ( 'M' | 'm' ) ( 'I' | 'i' ) ( 'T' | 't' ) ;

LOAD : ( 'L' | 'l' ) ( 'O' | 'o' ) ( 'A' | 'a' ) ( 'D' | 'd' ) ;

MACRO : ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'C' | 'c' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ;

MATCH : ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'C' | 'c' ) ( 'H' | 'h' ) ;

MAXVALUE : ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'X' | 'x' ) ( 'V' | 'v' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'U' | 'u' ) ( 'E' | 'e' ) ;

MERGE : ( 'M' | 'm' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ( 'G' | 'g' ) ( 'E' | 'e' ) ;

MINVALUE : ( 'M' | 'm' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'V' | 'v' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'U' | 'u' ) ( 'E' | 'e' ) ;

NO : ( 'N' | 'n' ) ( 'O' | 'o' ) ;

NODE : ( 'N' | 'n' ) ( 'O' | 'o' ) ( 'D' | 'd' ) ( 'E' | 'e' ) ;

NOT : ( 'N' | 'n' ) ( 'O' | 'o' ) ( 'T' | 't' ) ;

NULL : ( 'N' | 'n' ) ( 'U' | 'u' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ;

ON : ( 'O' | 'o' ) ( 'N' | 'n' ) ;

ONLY : ( 'O' | 'o' ) ( 'N' | 'n' ) ( 'L' | 'l' ) ( 'Y' | 'y' ) ;

OPTIONAL : ( 'O' | 'o' ) ( 'P' | 'p' ) ( 'T' | 't' ) ( 'I' | 'i' ) ( 'O' | 'o' ) ( 'N' | 'n' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ;

OR : ( 'O' | 'o' ) ( 'R' | 'r' ) ;

ORDER : ( 'O' | 'o' ) ( 'R' | 'r' ) ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ;

PRIMARY : ( 'P' | 'p' ) ( 'R' | 'r' ) ( 'I' | 'i' ) ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'R' | 'r' ) ( 'Y' | 'y' ) ;

PROFILE : ( 'P' | 'p' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'F' | 'f' ) ( 'I' | 'i' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ;

PROJECT : ( 'P' | 'p' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'J' | 'j' ) ( 'E' | 'e' ) ( 'C' | 'c' ) ( 'T' | 't' ) ;

RDFGRAPH : ( 'R' | 'r' ) ( 'D' | 'd' ) ( 'F' | 'f' ) ( 'G' | 'g' ) ( 'R' | 'r' ) ( 'A' | 'a' ) ( 'P' | 'p' ) ( 'H' | 'h' ) ;

READ : ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'A' | 'a' ) ( 'D' | 'd' ) ;

REL : ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'L' | 'l' ) ;

RENAME : ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'A' | 'a' ) ( 'M' | 'm' ) ( 'E' | 'e' ) ;

RETURN : ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'U' | 'u' ) ( 'R' | 'r' ) ( 'N' | 'n' ) ;

ROLLBACK : ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ( 'B' | 'b' ) ( 'A' | 'a' ) ( 'C' | 'c' ) ( 'K' | 'k' ) ;

ROLLBACK_SKIP_CHECKPOINT : ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ( 'B' | 'b' ) ( 'A' | 'a' ) ( 'C' | 'c' ) ( 'K' | 'k' ) '_' ( 'S' | 's' ) ( 'K' | 'k' ) ( 'I' | 'i' ) ( 'P' | 'p' ) '_' ( 'C' | 'c' ) ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'C' | 'c' ) ( 'K' | 'k' ) ( 'P' | 'p' ) ( 'O' | 'o' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'T' | 't' ) ;

SEQUENCE : ( 'S' | 's' ) ( 'E' | 'e' ) ( 'Q' | 'q' ) ( 'U' | 'u' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'C' | 'c' ) ( 'E' | 'e' ) ;

SET : ( 'S' | 's' ) ( 'E' | 'e' ) ( 'T' | 't' ) ;

SHORTEST : ( 'S' | 's' ) ( 'H' | 'h' ) ( 'O' | 'o' ) ( 'R' | 'r' ) ( 'T' | 't' ) ( 'E' | 'e' ) ( 'S' | 's' ) ( 'T' | 't' ) ;

START : ( 'S' | 's' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'R' | 'r' ) ( 'T' | 't' ) ;

STARTS : ( 'S' | 's' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'R' | 'r' ) ( 'T' | 't' ) ( 'S' | 's' ) ;

TABLE : ( 'T' | 't' ) ( 'A' | 'a' ) ( 'B' | 'b' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ;

THEN : ( 'T' | 't' ) ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ;

TO : ( 'T' | 't' ) ( 'O' | 'o' ) ;

TRANSACTION : ( 'T' | 't' ) ( 'R' | 'r' ) ( 'A' | 'a' ) ( 'N' | 'n' ) ( 'S' | 's' ) ( 'A' | 'a' ) ( 'C' | 'c' ) ( 'T' | 't' ) ( 'I' | 'i' ) ( 'O' | 'o' ) ( 'N' | 'n' ) ;

TRUE : ( 'T' | 't' ) ( 'R' | 'r' ) ( 'U' | 'u' ) ( 'E' | 'e' ) ;

TYPE : ( 'T' | 't' ) ( 'Y' | 'y' ) ( 'P' | 'p' ) ( 'E' | 'e' ) ;

UNION : ( 'U' | 'u' ) ( 'N' | 'n' ) ( 'I' | 'i' ) ( 'O' | 'o' ) ( 'N' | 'n' ) ;

UNWIND : ( 'U' | 'u' ) ( 'N' | 'n' ) ( 'W' | 'w' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ;

USE : ( 'U' | 'u' ) ( 'S' | 's' ) ( 'E' | 'e' ) ;

WHEN : ( 'W' | 'w' ) ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ;

WHERE : ( 'W' | 'w' ) ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ( 'E' | 'e' ) ;

WITH : ( 'W' | 'w' ) ( 'I' | 'i' ) ( 'T' | 't' ) ( 'H' | 'h' ) ;

WRITE : ( 'W' | 'w' ) ( 'R' | 'r' ) ( 'I' | 'i' ) ( 'T' | 't' ) ( 'E' | 'e' ) ;

XOR : ( 'X' | 'x' ) ( 'O' | 'o' ) ( 'R' | 'r' ) ;



ku_Statements
    : oC_Cypher ( SP? ';' SP? oC_Cypher )* SP? EOF ;

oC_Cypher
    : oC_AnyCypherOption? SP? ( oC_Statement ) ( SP? ';' )?;

oC_Statement
    : oC_Query
        | kU_CreateNodeTable
        | kU_CreateRelTable
        | kU_CreateRelTableGroup
        | kU_CreateRdfGraph
        | kU_CreateSequence
        | kU_CreateType
        | kU_Drop
        | kU_AlterTable
        | kU_CopyFrom
        | kU_CopyFromByColumn
        | kU_CopyTO
        | kU_StandaloneCall
        | kU_CreateMacro
        | kU_CommentOn
        | kU_Transaction
        | kU_Extension
        | kU_ExportDatabase
        | kU_ImportDatabase
        | kU_AttachDatabase
        | kU_DetachDatabase
        | kU_UseDatabase;

kU_CopyFrom
    : COPY SP oC_SchemaName ( ( SP? kU_ColumnNames SP? ) | SP ) FROM SP kU_ScanSource ( SP? kU_ParsingOptions )? ;

kU_ColumnNames
     : '(' SP? oC_SchemaName ( SP? ',' SP? oC_SchemaName )* SP? ')';

kU_ScanSource
    : kU_FilePaths
        | '(' SP? oC_Query SP? ')'
        | oC_Variable
        | oC_Variable '.' SP? oC_SchemaName ;

kU_CopyFromByColumn
    : COPY SP oC_SchemaName SP FROM SP '(' SP? StringLiteral ( SP? ',' SP? StringLiteral )* ')' SP BY SP COLUMN ;

kU_CopyTO
    : COPY SP '(' SP? oC_Query SP? ')' SP TO SP StringLiteral ( SP? kU_ParsingOptions )? ;

kU_ExportDatabase
    : EXPORT SP DATABASE SP StringLiteral ( SP? kU_ParsingOptions )? ;

kU_ImportDatabase
    : IMPORT SP DATABASE SP StringLiteral;

kU_AttachDatabase
    : ATTACH SP StringLiteral (SP AS SP oC_SchemaName)? SP '(' SP? DBTYPE SP oC_SymbolicName (SP? ',' SP? kU_Options)? SP? ')' ;

kU_Option
    : oC_SymbolicName SP? '=' SP? oC_Literal ;

kU_Options
    : kU_Option ( SP? ',' SP? kU_Option )* ;

kU_DetachDatabase
    : DETACH SP oC_SchemaName;

kU_UseDatabase
    : USE SP oC_SchemaName;

kU_StandaloneCall
    : CALL SP oC_SymbolicName SP? '=' SP? oC_Expression;

kU_CommentOn
    : COMMENT SP ON SP TABLE SP oC_SchemaName SP IS SP StringLiteral ;

kU_CreateMacro
    : CREATE SP MACRO SP oC_FunctionName SP? '(' SP? kU_PositionalArgs? SP? kU_DefaultArg? ( SP? ',' SP? kU_DefaultArg )* SP? ')' SP AS SP oC_Expression ;

kU_PositionalArgs
    : oC_SymbolicName ( SP? ',' SP? oC_SymbolicName )* ;

kU_DefaultArg
    : oC_SymbolicName SP? ':' '=' SP? oC_Literal ;

kU_FilePaths
    : '[' SP? StringLiteral ( SP? ',' SP? StringLiteral )* ']'
        | StringLiteral
        | GLOB SP? '(' SP? StringLiteral SP? ')' ;

kU_ParsingOptions
    : '(' SP? kU_Options SP? ')' ;

kU_IfNotExists
    : IF SP NOT SP EXISTS ;

kU_CreateNodeTable
    : CREATE SP NODE SP TABLE SP (kU_IfNotExists SP)? oC_SchemaName SP? '(' SP? kU_PropertyDefinitionsDDL SP? ( ',' SP? kU_CreateNodeConstraint ) SP? ')' ;

kU_CreateRelTable
    : CREATE SP REL SP TABLE SP (kU_IfNotExists SP)? oC_SchemaName SP? '(' SP? kU_RelTableConnection SP? ( ',' SP? kU_PropertyDefinitionsDDL SP? )? ( ',' SP? oC_SymbolicName SP? )?  ')' ;

kU_CreateRelTableGroup
    : CREATE SP REL SP TABLE SP GROUP SP (kU_IfNotExists SP)? oC_SchemaName SP? '(' SP? kU_RelTableConnection ( SP? ',' SP? kU_RelTableConnection )+ SP? ( ',' SP? kU_PropertyDefinitionsDDL SP? )? ( ',' SP? oC_SymbolicName SP? )?  ')' ;

kU_RelTableConnection
    : FROM SP oC_SchemaName SP TO SP oC_SchemaName ;

kU_CreateRdfGraph
    : CREATE SP RDFGRAPH SP (kU_IfNotExists SP)? oC_SchemaName ;

kU_CreateSequence
    : CREATE SP SEQUENCE SP (kU_IfNotExists SP)? oC_SchemaName (SP kU_SequenceOptions)* ;

kU_CreateType
    : CREATE SP TYPE SP oC_SchemaName SP AS SP kU_DataType SP? ;

kU_SequenceOptions
    : kU_IncrementBy
        | kU_MinValue
        | kU_MaxValue
        | kU_StartWith
        | kU_Cycle;

kU_IncrementBy : INCREMENT SP ( BY SP )? MINUS? oC_IntegerLiteral ;

kU_MinValue : (NO SP MINVALUE) | (MINVALUE SP MINUS? oC_IntegerLiteral) ;

kU_MaxValue : (NO SP MAXVALUE) | (MAXVALUE SP MINUS? oC_IntegerLiteral) ;

kU_StartWith : START SP ( WITH SP )? MINUS? oC_IntegerLiteral ;

kU_Cycle : (NO SP)? CYCLE ;

kU_Drop
    : DROP SP (TABLE | RDFGRAPH | SEQUENCE) SP oC_SchemaName ;

kU_AlterTable
    : ALTER SP TABLE SP oC_SchemaName SP kU_AlterOptions ;

kU_AlterOptions
    : kU_AddProperty
        | kU_DropProperty
        | kU_RenameTable
        | kU_RenameProperty;

kU_AddProperty
    : ADD SP oC_PropertyKeyName SP kU_DataType ( SP kU_Default )? ;

kU_Default
    : DEFAULT SP oC_Expression ;

kU_DropProperty
    : DROP SP oC_PropertyKeyName ;

kU_RenameTable
    : RENAME SP TO SP oC_SchemaName ;

kU_RenameProperty
    : RENAME SP oC_PropertyKeyName SP TO SP oC_PropertyKeyName ;

// TODO(Xiyang): Remove/rename kU_PropertyDefinitions & kU_PropertyDefinitionsDDL
kU_PropertyDefinitions : kU_PropertyDefinition ( SP? ',' SP? kU_PropertyDefinition )* ;

kU_PropertyDefinition : oC_PropertyKeyName SP kU_DataType ;

kU_PropertyDefinitionsDDL : kU_PropertyDefinitionDDL ( SP? ',' SP? kU_PropertyDefinitionDDL )* ;

kU_PropertyDefinitionDDL : oC_PropertyKeyName SP kU_DataType ( SP kU_Default )? ;

kU_CreateNodeConstraint : PRIMARY SP KEY SP? '(' SP? oC_PropertyKeyName SP? ')' ;

DECIMAL: ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'C' | 'c' ) ( 'I' | 'i' ) ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ;

kU_DataType
    : oC_SymbolicName
        | kU_DataType kU_ListIdentifiers
        | UNION SP? '(' SP? kU_PropertyDefinitions SP? ')'
        | oC_SymbolicName SP? '(' SP? kU_PropertyDefinitions SP? ')'
        | oC_SymbolicName SP? '(' SP? kU_DataType SP? ',' SP? kU_DataType SP? ')'
        | DECIMAL SP? '(' SP? oC_IntegerLiteral SP? ',' SP? oC_IntegerLiteral SP? ')' ;

kU_ListIdentifiers : kU_ListIdentifier ( kU_ListIdentifier )* ;

kU_ListIdentifier : '[' oC_IntegerLiteral? ']' ;

oC_AnyCypherOption
    : oC_Explain
        | oC_Profile ;

oC_Explain
    : EXPLAIN ;

oC_Profile
    : PROFILE ;

kU_Transaction
    : BEGIN SP TRANSACTION
        | BEGIN SP TRANSACTION SP READ SP ONLY
        | COMMIT
        | COMMIT_SKIP_CHECKPOINT
        | ROLLBACK
        | ROLLBACK_SKIP_CHECKPOINT;

kU_Extension
    : kU_LoadExtension
        | kU_InstallExtension;

kU_LoadExtension
    : LOAD SP EXTENSION SP ( StringLiteral | oC_Variable ) ;

kU_InstallExtension
    : INSTALL SP oC_Variable ;

oC_Query
    : (kU_ProjectGraph SP? )? oC_RegularQuery ;

kU_ProjectGraph
    : PROJECT SP GRAPH SP oC_SchemaName SP? '(' SP? kU_GraphProjectionTableItems SP? ')' ;

kU_GraphProjectionTableItems
    : kU_GraphProjectionTableItem ( SP? ',' SP? kU_GraphProjectionTableItem )* ;

oC_RegularQuery
    : oC_SingleQuery ( SP? oC_Union )*
        | (oC_Return SP? )+ oC_SingleQuery { notifyReturnNotAtEnd($ctx->start); }
        ;

oC_Union
     :  ( UNION SP ALL SP? oC_SingleQuery )
         | ( UNION SP? oC_SingleQuery ) ;

oC_SingleQuery
    : oC_SinglePartQuery
        | oC_MultiPartQuery
        ;

oC_SinglePartQuery
    : ( oC_ReadingClause SP? )* oC_Return
        | ( ( oC_ReadingClause SP? )* oC_UpdatingClause ( SP? oC_UpdatingClause )* ( SP? oC_Return )? )
        | ( oC_ReadingClause SP? )+ { notifyQueryNotConcludeWithReturn($ctx->start); }
        ;

oC_MultiPartQuery
    : ( kU_QueryPart SP? )+ oC_SinglePartQuery;

kU_QueryPart
    : (oC_ReadingClause SP? )* ( oC_UpdatingClause SP? )* oC_With ;

oC_UpdatingClause
    : oC_Create
        | oC_Merge
        | oC_Set
        | oC_Delete
        ;

oC_ReadingClause
    : oC_Match
        | oC_Unwind
        | kU_InQueryCall
        | kU_LoadFrom
        ;

kU_LoadFrom
    :  LOAD ( SP WITH SP HEADERS SP? '(' SP? kU_PropertyDefinitions SP? ')' )? SP FROM SP kU_ScanSource (SP? kU_ParsingOptions)? (SP? oC_Where)? ;

kU_InQueryCall
    : ( kU_ProjectGraph SP? )? CALL SP oC_FunctionInvocation (SP? oC_Where)? ;

kU_GraphProjectionTableItem
    : oC_SchemaName ( SP? '{' SP? kU_GraphProjectionColumnItems SP? '}' )? ;

kU_GraphProjectionColumnItems
    : kU_GraphProjectionColumnItem ( SP? ',' SP? kU_GraphProjectionColumnItem )* ;

kU_GraphProjectionColumnItem
    : oC_PropertyKeyName ( SP kU_Default )? ( SP oC_Where )? ;

oC_Match
    : ( OPTIONAL SP )? MATCH SP? oC_Pattern ( SP oC_Where )? ;

oC_Unwind : UNWIND SP? oC_Expression SP AS SP oC_Variable ;

oC_Create
    : CREATE SP? oC_Pattern ;

// For unknown reason, openCypher use oC_PatternPart instead of oC_Pattern. There should be no difference in terms of planning.
// So we choose to be consistent with oC_Create and use oC_Pattern instead.
oC_Merge : MERGE SP? oC_Pattern ( SP oC_MergeAction )* ;

oC_MergeAction
    :  ( ON SP MATCH SP oC_Set )
        | ( ON SP CREATE SP oC_Set )
        ;

oC_Set
    : SET SP? oC_SetItem ( SP? ',' SP? oC_SetItem )* ;

oC_SetItem
    : ( oC_PropertyExpression SP? '=' SP? oC_Expression ) ;

oC_Delete
    : ( DETACH SP )? DELETE SP? oC_Expression ( SP? ',' SP? oC_Expression )*;

oC_With
    : WITH oC_ProjectionBody ( SP? oC_Where )? ;

oC_Return
    : RETURN oC_ProjectionBody ;

oC_ProjectionBody
    : ( SP? DISTINCT )? SP oC_ProjectionItems (SP oC_Order )? ( SP oC_Skip )? ( SP oC_Limit )? ;

oC_ProjectionItems
    : ( STAR ( SP? ',' SP? oC_ProjectionItem )* )
        | ( oC_ProjectionItem ( SP? ',' SP? oC_ProjectionItem )* )
        ;

STAR : '*' ;

oC_ProjectionItem
    : ( oC_Expression SP AS SP oC_Variable )
        | oC_Expression
        ;

oC_Order
    : ORDER SP BY SP oC_SortItem ( ',' SP? oC_SortItem )* ;

oC_Skip
    :  L_SKIP SP oC_Expression ;

L_SKIP : ( 'S' | 's' ) ( 'K' | 'k' ) ( 'I' | 'i' ) ( 'P' | 'p' ) ;

oC_Limit
    : LIMIT SP oC_Expression ;

oC_SortItem
    : oC_Expression ( SP? ( ASCENDING | ASC | DESCENDING | DESC ) )? ;

oC_Where
    : WHERE SP oC_Expression ;

oC_Pattern
    : oC_PatternPart ( SP? ',' SP? oC_PatternPart )* ;

oC_PatternPart
    :  ( oC_Variable SP? '=' SP? oC_AnonymousPatternPart )
        | oC_AnonymousPatternPart ;

oC_AnonymousPatternPart
    : oC_PatternElement ;

oC_PatternElement
    : ( oC_NodePattern ( SP? oC_PatternElementChain )* )
        | ( '(' oC_PatternElement ')' )
        ;

oC_NodePattern
    : '(' SP? ( oC_Variable SP? )? ( oC_NodeLabels SP? )? ( kU_Properties SP? )? ')' ;

oC_PatternElementChain
    : oC_RelationshipPattern SP? oC_NodePattern ;

oC_RelationshipPattern
    : ( oC_LeftArrowHead SP? oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash )
        | ( oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash SP? oC_RightArrowHead )
        | ( oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash )
        ;

oC_RelationshipDetail
    : '[' SP? ( oC_Variable SP? )? ( oC_RelationshipTypes SP? )? ( oC_RangeLiteral SP? )? ( kU_Properties SP? )? ']' ;

// The original oC_Properties definition is  oC_MapLiteral | oC_Parameter.
// We choose to not support parameter as properties which will be the decision for a long time.
// We then substitute with oC_MapLiteral definition. We create oC_MapLiteral only when we decide to add MAP type.
kU_Properties
    : '{' SP? ( oC_PropertyKeyName SP? ':' SP? oC_Expression SP? ( ',' SP? oC_PropertyKeyName SP? ':' SP? oC_Expression SP? )* )? '}';

oC_RelationshipTypes
    :  ':' SP? oC_RelTypeName ( SP? '|' ':'? SP? oC_RelTypeName )* ;

oC_NodeLabels
    :  oC_NodeLabel ( SP? oC_NodeLabel )* ;

oC_NodeLabel
    : ':' SP? oC_LabelName ;

oC_RangeLiteral
    :  '*' SP? ( SHORTEST | ALL SP SHORTEST )? SP? (oC_LowerBound? SP? '..' SP? oC_UpperBound? | oC_IntegerLiteral)? (SP? kU_RecursiveRelationshipComprehension)? ;

kU_RecursiveRelationshipComprehension
    : '(' SP? oC_Variable SP? ',' SP? oC_Variable ( SP? '|' SP? oC_Where SP? )? ( SP? '|' SP? kU_IntermediateRelProjectionItems SP? ',' SP? kU_IntermediateNodeProjectionItems SP? )? ')' ;

kU_IntermediateNodeProjectionItems
    : '{' SP? oC_ProjectionItems? SP? '}' ;

kU_IntermediateRelProjectionItems
    : '{' SP? oC_ProjectionItems? SP? '}' ;

oC_LowerBound
    : DecimalInteger ;

oC_UpperBound
    : DecimalInteger ;

oC_LabelName
    : oC_SchemaName ;

oC_RelTypeName
    : oC_SchemaName ;

oC_Expression
    : oC_OrExpression ;

oC_OrExpression
    : oC_XorExpression ( SP OR SP oC_XorExpression )* ;

oC_XorExpression
    : oC_AndExpression ( SP XOR SP oC_AndExpression )* ;

oC_AndExpression
    : oC_NotExpression ( SP AND SP oC_NotExpression )* ;

oC_NotExpression
    : ( NOT SP? )*  oC_ComparisonExpression ;

oC_ComparisonExpression
    : kU_BitwiseOrOperatorExpression ( SP? kU_ComparisonOperator SP? kU_BitwiseOrOperatorExpression )?
        | kU_BitwiseOrOperatorExpression ( SP? INVALID_NOT_EQUAL SP? kU_BitwiseOrOperatorExpression ) { notifyInvalidNotEqualOperator($INVALID_NOT_EQUAL); }
        | kU_BitwiseOrOperatorExpression SP? kU_ComparisonOperator SP? kU_BitwiseOrOperatorExpression ( SP? kU_ComparisonOperator SP? kU_BitwiseOrOperatorExpression )+ { notifyNonBinaryComparison($ctx->start); }
        ;

kU_ComparisonOperator : '=' | '<>' | '<' | '<=' | '>' | '>=' ;

INVALID_NOT_EQUAL : '!=' ;

kU_BitwiseOrOperatorExpression
    : kU_BitwiseAndOperatorExpression ( SP? '|' SP? kU_BitwiseAndOperatorExpression )* ;

kU_BitwiseAndOperatorExpression
    : kU_BitShiftOperatorExpression ( SP? '&' SP? kU_BitShiftOperatorExpression )* ;

kU_BitShiftOperatorExpression
    : oC_AddOrSubtractExpression ( SP? kU_BitShiftOperator SP? oC_AddOrSubtractExpression )* ;

kU_BitShiftOperator : '>>' | '<<' ;

oC_AddOrSubtractExpression
    : oC_MultiplyDivideModuloExpression ( SP? kU_AddOrSubtractOperator SP? oC_MultiplyDivideModuloExpression )* ;

kU_AddOrSubtractOperator : '+' | '-' ;

oC_MultiplyDivideModuloExpression
    : oC_PowerOfExpression ( SP? kU_MultiplyDivideModuloOperator SP? oC_PowerOfExpression )* ;

kU_MultiplyDivideModuloOperator : '*' | '/' | '%' ;

oC_PowerOfExpression
    : oC_UnaryAddSubtractOrFactorialExpression ( SP? '^' SP? oC_UnaryAddSubtractOrFactorialExpression )* ;

oC_UnaryAddSubtractOrFactorialExpression
    : ( MINUS SP? )* oC_StringListNullOperatorExpression (SP? FACTORIAL)? ;

MINUS : '-' ;

FACTORIAL : '!' ;

oC_StringListNullOperatorExpression
    : oC_PropertyOrLabelsExpression ( oC_StringOperatorExpression | oC_ListOperatorExpression+ | oC_NullOperatorExpression )? ;

oC_ListOperatorExpression
    : ( SP IN SP? oC_PropertyOrLabelsExpression )
        | ( '[' oC_Expression ']' )
        | ( '[' oC_Expression? COLON oC_Expression? ']' )
        ;

COLON : ':' ;

oC_StringOperatorExpression
    :  ( oC_RegularExpression | ( SP STARTS SP WITH ) | ( SP ENDS SP WITH ) | ( SP CONTAINS ) ) SP? oC_PropertyOrLabelsExpression ;

oC_RegularExpression
    :  SP? '=~' ;

oC_NullOperatorExpression
    : ( SP IS SP NULL )
        | ( SP IS SP NOT SP NULL ) ;

oC_PropertyOrLabelsExpression
    : oC_Atom ( SP? oC_PropertyLookup )* ;

oC_Atom
    : oC_Literal
        | oC_Parameter
        | oC_CaseExpression
        | oC_ParenthesizedExpression
        | oC_FunctionInvocation
        | oC_PathPatterns
        | oC_ExistSubquery
        | kU_CountSubquery
        | oC_Variable
        ;

oC_Literal
    : oC_NumberLiteral
        | StringLiteral
        | oC_BooleanLiteral
        | NULL
        | oC_ListLiteral
        | kU_StructLiteral
        ;

oC_BooleanLiteral
    : TRUE
        | FALSE
        ;

oC_ListLiteral
    :  '[' SP? ( oC_Expression SP? ( kU_ListEntry SP? )* )? ']' ;

kU_ListEntry
    : ',' SP? oC_Expression? ;

kU_StructLiteral
    :  '{' SP? kU_StructField SP? ( ',' SP? kU_StructField SP? )* '}' ;

kU_StructField
    :   ( oC_SymbolicName | StringLiteral ) SP? ':' SP? oC_Expression ;

oC_ParenthesizedExpression
    : '(' SP? oC_Expression SP? ')' ;

oC_FunctionInvocation
    : COUNT SP? '(' SP? '*' SP? ')'
        | CAST SP? '(' SP? kU_FunctionParameter SP? ( ( AS SP? kU_DataType ) | ( ',' SP? kU_FunctionParameter ) ) SP? ')'
        | oC_FunctionName SP? '(' SP? ( DISTINCT SP? )? ( kU_FunctionParameter SP? ( ',' SP? kU_FunctionParameter SP? )* )? ')' ;

oC_FunctionName
    : oC_SymbolicName ;

kU_FunctionParameter
    : ( oC_SymbolicName SP? ':' '=' SP? )? oC_Expression ;

oC_PathPatterns
    : oC_NodePattern ( SP? oC_PatternElementChain )+;

oC_ExistSubquery
    : EXISTS SP? '{' SP? MATCH SP? oC_Pattern ( SP? oC_Where )? SP? '}' ;

kU_CountSubquery
    : COUNT SP? '{' SP? MATCH SP? oC_Pattern ( SP? oC_Where )? SP? '}' ;

oC_PropertyLookup
    : '.' SP? ( oC_PropertyKeyName | STAR ) ;

oC_CaseExpression
    :  ( ( CASE ( SP? oC_CaseAlternative )+ ) | ( CASE SP? oC_Expression ( SP? oC_CaseAlternative )+ ) ) ( SP? ELSE SP? oC_Expression )? SP? END ;

oC_CaseAlternative
    :  WHEN SP? oC_Expression SP? THEN SP? oC_Expression ;

oC_Variable
    : oC_SymbolicName ;

StringLiteral
    : ( '"' ( StringLiteral_0 | EscapedChar )* '"' )
        | ( '\'' ( StringLiteral_1 | EscapedChar )* '\'' )
        ;

EscapedChar
    : '\\' ( '\\' | '\'' | '"' | ( 'B' | 'b' ) | ( 'F' | 'f' ) | ( 'N' | 'n' ) | ( 'R' | 'r' ) | ( 'T' | 't' ) | ( ( 'U' | 'u' ) ( HexDigit HexDigit HexDigit HexDigit ) ) | ( ( 'U' | 'u' ) ( HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit ) ) ) ;

oC_NumberLiteral
    : oC_DoubleLiteral
        | oC_IntegerLiteral
        ;

oC_Parameter
    : '$' ( oC_SymbolicName | DecimalInteger ) ;

oC_PropertyExpression
    : oC_Atom SP? oC_PropertyLookup ;

oC_PropertyKeyName
    : oC_SchemaName ;

oC_IntegerLiteral
    : DecimalInteger ;

DecimalInteger
    : ZeroDigit
        | ( NonZeroDigit ( Digit )* )
        ;

HexLetter
    : ( 'A' | 'a' )
        | ( 'B' | 'b' )
        | ( 'C' | 'c' )
        | ( 'D' | 'd' )
        | ( 'E' | 'e' )
        | ( 'F' | 'f' )
        ;

HexDigit
    : Digit
        | HexLetter
        ;

Digit
    : ZeroDigit
        | NonZeroDigit
        ;

NonZeroDigit
    : NonZeroOctDigit
        | '8'
        | '9'
        ;

NonZeroOctDigit
    : '1'
        | '2'
        | '3'
        | '4'
        | '5'
        | '6'
        | '7'
        ;

ZeroDigit
    : '0' ;

oC_DoubleLiteral
    : RegularDecimalReal ;

RegularDecimalReal
    : ( Digit )* '.' ( Digit )+ ;

oC_SchemaName
    : oC_SymbolicName ;

oC_SymbolicName
    : UnescapedSymbolicName
        | EscapedSymbolicName {if ($EscapedSymbolicName.text == "``") { notifyEmptyToken($EscapedSymbolicName); }}
        | HexLetter
        | kU_NonReservedKeywords
        ;

// example of BEGIN and END: TCKWith2.Scenario1
kU_NonReservedKeywords
    : COMMENT
        | ADD
        | ALTER
        | AS
        | ATTACH
        | BEGIN
        | BY
        | CALL
        | COMMENT
        | COMMIT
        | CONTAINS
        | COPY
        | COUNT
        | CYCLE
        | DATABASE
        | DECIMAL
        | DELETE
        | DETACH
        | DROP
        | EXPLAIN
        | EXPORT
        | EXTENSION
        | GRAPH
        | IF
        | IS
        | IMPORT
        | INCREMENT
        | KEY
        | LOAD
        | MATCH
        | MAXVALUE
        | MERGE
        | MINVALUE
        | NO
        | NODE
        | PROJECT
        | READ
        | REL
        | RENAME
        | RETURN
        | ROLLBACK
        | SEQUENCE
        | SET
        | START
        | TRANSACTION
        | TYPE
        | USE
        | WRITE
        ;

UnescapedSymbolicName
    : IdentifierStart ( IdentifierPart )* ;

IdentifierStart
    : ID_Start
        | Pc
        ;

IdentifierPart
    : ID_Continue
        | Sc
        ;

EscapedSymbolicName
    : ( '`' ( EscapedSymbolicName_0 )* '`' )+ ;

SP
  : ( WHITESPACE )+ ;

WHITESPACE
    : SPACE
        | TAB
        | LF
        | VT
        | FF
        | CR
        | FS
        | GS
        | RS
        | US
        | '\u1680'
        | '\u180e'
        | '\u2000'
        | '\u2001'
        | '\u2002'
        | '\u2003'
        | '\u2004'
        | '\u2005'
        | '\u2006'
        | '\u2008'
        | '\u2009'
        | '\u200a'
        | '\u2028'
        | '\u2029'
        | '\u205f'
        | '\u3000'
        | '\u00a0'
        | '\u2007'
        | '\u202f'
        | CypherComment
        ;

CypherComment
    : ( '/*' ( Comment_1 | ( '*' Comment_2 ) )* '*/' )
        | ( '//' ( Comment_3 )* CR? ( LF | EOF ) )
        ;

oC_LeftArrowHead
    : '<'
        | '\u27e8'
        | '\u3008'
        | '\ufe64'
        | '\uff1c'
        ;

oC_RightArrowHead
    : '>'
        | '\u27e9'
        | '\u3009'
        | '\ufe65'
        | '\uff1e'
        ;

oC_Dash
    : '-'
        | '\u00ad'
        | '\u2010'
        | '\u2011'
        | '\u2012'
        | '\u2013'
        | '\u2014'
        | '\u2015'
        | '\u2212'
        | '\ufe58'
        | '\ufe63'
        | '\uff0d'
        ;

fragment FF : [\f] ;

fragment EscapedSymbolicName_0 : ~[`] ;

fragment RS : [\u001E] ;

fragment ID_Continue : [\p{ID_Continue}] ;

fragment Comment_1 : ~[*] ;

fragment StringLiteral_1 : ~['\\] ;

fragment Comment_3 : ~[\n\r] ;

fragment Comment_2 : ~[/] ;

fragment GS : [\u001D] ;

fragment FS : [\u001C] ;

fragment CR : [\r] ;

fragment Sc : [\p{Sc}] ;

fragment SPACE : [ ] ;

fragment Pc : [\p{Pc}] ;

fragment TAB : [\t] ;

fragment StringLiteral_0 : ~["\\] ;

fragment LF : [\n] ;

fragment VT : [\u000B] ;

fragment US : [\u001F] ;

fragment ID_Start : [\p{ID_Start}] ;

// This is used to capture unknown lexer input (e.g. !) to avoid parser exception.
Unknown : .;
