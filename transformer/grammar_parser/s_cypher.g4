grammar S_Cypher;
import Cypher;

oC_Query : oC_RegularQuery
         | oC_StandaloneCall
         | t_TimeWindowLimit
         ;

oC_SinglePartQuery : ( ( oC_ReadingClause SP? )* oC_Return )
                   | ( ( oC_ReadingClause SP? )* oC_UpdatingClause ( SP? oC_UpdatingClause )* ( SP? t_AtTime )? ( SP? oC_Return )? )
                   ;

oC_MultiPartQuery : ( ( oC_ReadingClause SP? )* oC_With SP? ) + oC_SinglePartQuery
                  | ( ( oC_ReadingClause SP? )* oC_UpdatingClause ( SP? oC_UpdatingClause )* ( SP? t_AtTime )? SP? oC_With SP? ) + oC_SinglePartQuery
                  ;

oC_Match : ( OPTIONAL SP )? MATCH SP? oC_Pattern ( SP? ( t_AtTime | t_Between ) )? ( SP? oC_Where )? ;

oC_UpdatingClause : oC_Create
                  | oC_Merge
                  | oC_Delete
                  | oC_Set
                  | oC_Remove
                  | oC_Stale
                  ;

oC_SetItem : ( oC_Variable SP? t_AtTElement )
           | ( oC_Variable ( SP? t_AtTElement )? SP? '.' SP? oC_PropertyKeyName SP? t_AtTElement )
           | ( oC_Variable ( SP? t_AtTElement )? SP? '.' SP? oC_PropertyKeyName ( SP? t_AtTElement )? SP? PoundValue ( SP? t_AtTElement )? )
           | ( oC_Variable ( SP? t_AtTElement )? SP? '.' SP? oC_PropertyKeyName ( SP? t_AtTElement )? SP? '=' SP? ( ( oC_Expression ( SP? t_AtTElement )? ) | t_PropertiesPattern | NULL ) )
           | ( oC_Variable SP? oC_NodeLabels )
           | ( oC_Variable SP? '=' SP? ( oC_Parameter | oC_Variable ) )
           | ( oC_Variable SP? '+=' SP? t_PropertiesPattern )
           ;

oC_Stale : STALE SP? oC_StaleItem ( SP? ',' SP? oC_StaleItem )* ;

oC_StaleItem : oC_Variable ;

oC_Remove : ( DETACH SP )? REMOVE SP oC_RemoveItem ( SP? ',' SP? oC_RemoveItem )* ;

oC_RemoveItem : ( oC_Variable oC_NodeLabels )
              | ( oC_Variable SP? '.' SP? oC_PropertyKeyName )
              ;

oC_Delete :  ( DETACH SP )? DELETE SP? oC_DeleteItem ( SP? ',' SP? oC_DeleteItem )* ;

oC_DeleteItem : oC_Variable ( '.' oC_PropertyKeyName PoundValue? )? ;

t_AtTime : AT_TIME SP? ( 'date' | 'time' | 'datetime' ) SP? '(' SP? t_TimePointLiteral SP? ')' ;

t_Between : BETWEEN SP? 'interval' SP? '(' SP? t_TimePointLiteral SP? ',' SP? ( t_TimePointLiteral | NOW ) SP? ')' ;

t_TimeWindowLimit : SNAPSHOT SP? ( 'date' | 'time' | 'datetime' ) SP? '(' SP? t_TimePointLiteral SP? ')'
                  | SCOPE SP? 'interval' SP? '(' SP? t_TimePointLiteral SP? ',' SP? ( t_TimePointLiteral | NOW ) SP? ')'
                  ;

oC_PatternPart : oC_Variable SP? '=' SP? t_PathFunctionPattern
               | oC_Variable SP? '=' SP? oC_AnonymousPatternPart
               | oC_AnonymousPatternPart
               ;

t_PathFunctionPattern : t_PathFunctionName SP? '(' SP? t_SinglePathPattern SP? ')' ;

t_PathFunctionName : 'cPath'
                   | 'pairCPath'
                   | 'earliestPath'
                   | 'latestPath'
                   | 'fastestPath'
                   | 'shortestSPath'
                   ;

t_SinglePathPattern : oC_NodePattern SP? oC_RelationshipPattern SP? oC_NodePattern ;

oC_NodePattern : '(' SP? ( oC_Variable SP? )? ( oC_NodeLabels SP? )? ( t_AtTElement SP? )? ( oC_Properties SP? )? ')' ;

oC_Properties : t_PropertiesPattern
              | oC_Parameter
              ;

t_PropertiesPattern : '{' SP? ( oC_PropertyKeyName SP? ( t_AtTElement SP? )? ':' SP? oC_Expression SP? ( t_AtTElement SP? )? ( ',' SP? oC_PropertyKeyName SP? ( t_AtTElement SP? )? ':' SP? oC_Expression SP? ( t_AtTElement SP? )? )* )? '}' ;

oC_RelationshipDetail : '[' SP? ( oC_Variable SP? )? ( oC_RelationshipTypes SP? )? oC_RangeLiteral? ( t_AtTElement SP? )? ( oC_Properties SP? )? ']' ;

oC_StringListNullPredicateExpression : oC_AddOrSubtractExpression ( oC_TimePredicateExpression | oC_StringPredicateExpression | oC_ListPredicateExpression | oC_NullPredicateExpression )* ;

oC_TimePredicateExpression : SP ( DURING | OVERLAPS ) SP oC_AddOrSubtractExpression ;

oC_Atom : oC_Literal
        | oC_Parameter
        | oC_CaseExpression
        | ( COUNT SP? '(' SP? '*' SP? ')' )
        | oC_ListComprehension
        | oC_PatternComprehension
        | oC_Quantifier
        | oC_PatternPredicate
        | oC_ParenthesizedExpression
        | oC_FunctionInvocation
        | oC_ExistentialSubquery
        | oC_Variable ( SP? AtT )? ( SP? '.' SP? ( FROM | TO ) )?
        ;

t_AtTElement : AtT SP? '(' SP? t_TimePointLiteral SP? ',' SP? ( t_TimePointLiteral | NOW ) SP? ')';

oC_Literal : oC_BooleanLiteral
           | NULL
           | oC_NumberLiteral
           | t_StringLiteral
           | oC_ListLiteral
           | oC_MapLiteral
           ;

oC_SymbolicName : UnescapedSymbolicName
                | EscapedSymbolicName
                | t_FunctionName
                | HexLetter
                | COUNT
                | FILTER
                | EXTRACT
                | ANY
                | NONE
                | SINGLE
                ;

t_FunctionName : t_PathFunctionName
               | t_TimeFunctionName
               | 'content'
               | 'properties'
               ;

t_TimeFunctionName : 'now'
                   | 'date'
                   | 'time'
                   | 'datetime'
                   | 'interval'
                   | 'duration'
                   | 'intersection'
                   | 'range'
                   | 'elapsed_time'
                   ;

t_StringLiteral : StringLiteral;

t_TimePointLiteral : t_StringLiteral
                   | oC_MapLiteral
                   ;

AtT : '@T' | '@t' ;

PoundValue : '#' ( 'V' | 'v' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'U' | 'u' ) ( 'E' | 'e' ) ;

oC_ReservedWord : ALL
                | ASC
                | ASCENDING
                | BY
                | CREATE
                | DELETE
                | DESC
                | DESCENDING
                | DETACH
                | EXISTS
                | LIMIT
                | MATCH
                | MERGE
                | ON
                | OPTIONAL
                | ORDER
                | REMOVE
                | RETURN
                | SET
                | L_SKIP
                | WHERE
                | WITH
                | UNION
                | UNWIND
                | AND
                | AS
                | CONTAINS
                | DISTINCT
                | ENDS
                | IN
                | IS
                | NOT
                | OR
                | STARTS
                | XOR
                | FALSE
                | TRUE
                | NULL
                | CONSTRAINT
                | DO
                | FOR
                | REQUIRE
                | UNIQUE
                | CASE
                | WHEN
                | THEN
                | ELSE
                | END
                | MANDATORY
                | SCALAR
                | OF
                | ADD
                | DROP
                | NOW
                | FROM
                | TO
                | AT_TIME
                | SNAPSHOT
                | BETWEEN
                | SCOPE
                | STALE
                | DURING
                | OVERLAPS
                ;

NOW : 'NOW' ;

FROM : ( 'F' | 'f' ) ( 'R' | 'r') ( 'O' | 'o') ( 'M' | 'm') ;

TO : ( 'T' | 't' ) ( 'O' | 'o' ) ;

AT_TIME : ( 'A' | 'a' ) ( 'T' | 't' ) '_' ( 'T' | 't' ) ( 'I' | 'i' ) ( 'M' | 'm' ) ( 'E' | 'e' ) ;

SNAPSHOT : ( 'S' | 's' ) ( 'N' | 'n' ) ( 'A' | 'a' ) ( 'P' | 'p') ( 'S' | 's' ) ( 'H' | 'h' ) ( 'O' | 'o') ( 'T' | 't' ) ;

BETWEEN : ( 'B' | 'b' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'W' | 'w' ) ( 'E' | 'e' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ;

SCOPE : ( 'S' | 's' ) ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'P' | 'p') ( 'E' | 'e' ) ;

STALE : ( 'S' | 's' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ;

DURING : ( 'D' | 'd' ) ( 'U' | 'u' ) ( 'R' | 'r' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'G' | 'g' ) ;

OVERLAPS : ( 'O' | 'o' ) ( 'V' | 'v' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ( 'L' | 'l' ) ( 'A' | 'a' ) ( 'P' | 'p' ) ( 'S' | 's' ) ;

//UnescapedSymbolicName : [a-zA-Z] [a-zA-Z0-9_]* ;