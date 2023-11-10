grammar s_cypher;
import cypher;

oC_Query : oC_RegularQuery
         | oC_StandaloneCall
         | s_TimeWindowLimit
         ;

oC_Match : ( OPTIONAL SP )? MATCH SP? oC_Pattern ( SP? ( s_AtTime | s_Between ) )? ( SP? s_Where )? ;

oC_UpdatingClause : s_Create | s_Merge | s_Set | oC_Remove | s_Stale | s_Delete ;

s_Create : CREATE SP? oC_Pattern ( SP? s_AtTime )? ;

s_Merge : MERGE SP? oC_PatternPart ( SP oC_MergeAction )* ( SP? s_AtTime )? ;

s_Set : SET SP? oC_SetItem ( SP? ',' SP? oC_SetItem )* ( SP? s_AtTime )? ;

s_Stale : STALE SP? s_StaleItem ( SP? ',' SP? s_StaleItem )* ( SP? s_AtTime )? ;

oC_SetItem : ( oC_PropertyExpression SP? '=' SP? oC_Expression )
           | ( oC_Variable SP? '=' SP? oC_Expression )
           | ( oC_Variable SP? '+=' SP? oC_Expression )
           | ( oC_Variable SP? oC_NodeLabels )
           | ( oC_Variable SP? s_AtTElement )
           | ( oC_Variable ( SP? s_AtTElement )? '.' s_SetPropertyItemOne )
           | ( oC_Variable ( SP? s_AtTElement )? '.' s_SetPropertyItemTwo SP? s_SetValueItem )
           ;

s_SetPropertyItemOne : oC_PropertyKeyName SP? s_AtTElement ;

s_SetPropertyItemTwo : oC_PropertyKeyName ( SP? s_AtTElement )? ;

s_SetValueItem : PoundValue SP? s_AtTElement ;

s_StaleItem : oC_Expression ( '.' oC_PropertyKeyName SP? PoundValue )? ;

s_Delete : ( DETACH SP )? DELETE SP? s_DeleteItem ( SP? ',' SP? s_DeleteItem )* ;

s_DeleteItem : oC_Expression ( SP? s_PropertyValueAtTElement )? ;

oC_RemoveItem : ( oC_Variable oC_NodeLabels )
              | s_RemovePropertyExpression
              ;

s_RemovePropertyExpression : oC_Atom ( SP? oC_PropertyLookup )* SP? '.'  SP? oC_PropertyKeyName ;

s_AtTime : AT_TIME SP? oC_Expression;

s_Between : BETWEEN SP? oC_Expression;

s_TimeWindowLimit : s_Snapshot
                  | s_Scope
                  ;

oC_InQueryCall
           :  CALL SP oC_ExplicitProcedureInvocation ( SP? YIELD SP s_YieldItems )? ;

oC_StandaloneCall
              :  CALL SP ( oC_ExplicitProcedureInvocation | oC_ImplicitProcedureInvocation ) ( SP? YIELD SP ( '*' | s_YieldItems ) )? ;

s_YieldItems
          :  s_YieldItem ( SP? ',' SP? s_YieldItem )* ( SP? s_Where )? ;

s_YieldItem
         :  oC_ProcedureResultField ( SP AS SP oC_Variable )? ;

s_WithPartQuery
              : ( oC_ReadingClause SP? )* ( oC_UpdatingClause SP? )* s_With SP? ;

s_With
    :  WITH oC_ProjectionBody ( SP? s_Where )? ;

oC_ExistentialSubquery
                   :  EXISTS SP? '{' SP? ( oC_RegularQuery | ( oC_Pattern ( SP? s_Where )? ) ) SP? '}' ;

oC_FilterExpression
                :  oC_IdInColl ( SP? s_Where )? ;

s_Snapshot : SNAPSHOT SP? oC_Expression ;

s_Scope : SCOPE SP? oC_Expression ;

oC_PatternPart : oC_Variable SP? '=' SP? s_PathFunctionPattern
               | oC_Variable SP? '=' SP? oC_AnonymousPatternPart
               | oC_AnonymousPatternPart
               ;

s_PathFunctionPattern : oC_FunctionName SP? '(' SP? s_SinglePathPattern SP? ')' ;

s_SinglePathPattern : oC_NodePattern SP? oC_RelationshipPattern SP? oC_NodePattern ;

oC_NodePattern : '(' SP? ( oC_Variable SP? )? ( oC_NodeLabels SP? )? ( s_AtTElement SP? )? ( s_Properties SP? )? ')' ;

s_Properties : s_PropertiesPattern
              | oC_Parameter
              ;

s_PropertiesPattern : '{' SP? ( s_PropertyNode ':' s_ValueNode ( ',' SP? s_PropertyNode ':' s_ValueNode )* )? '}' ;

s_PropertyNode : oC_PropertyKeyName SP? ( s_AtTElement SP? )? ;

s_ValueNode : SP? oC_Expression SP? ( s_AtTElement SP? )? ;

oC_RelationshipDetail : '[' SP? ( oC_Variable SP? )? ( oC_RelationshipTypes SP? )? oC_RangeLiteral? ( s_AtTElement SP? )? ( oC_Properties SP? )? ']' ;

oC_StringListNullPredicateExpression : oC_AddOrSubtractExpression ( s_TimePredicateExpression | oC_StringPredicateExpression | oC_ListPredicateExpression | oC_NullPredicateExpression )? ;

oC_ListOperatorExpression : ( oC_PropertyOrLabelsExpression | s_AtTExpression ) ( s_SingleIndexExpression | s_DoubleIndexExpression )* ;

s_SingleIndexExpression : SP? '[' s_LeftExpression ']' ;

s_DoubleIndexExpression : SP? '[' s_LeftExpression? '..' s_RightExpression? ']' ;

s_LeftExpression : oC_Expression ;

s_RightExpression : oC_Expression ;

s_AtTExpression : oC_Atom ( ( SP? oC_PropertyLookup )* SP? s_PropertyValueAtTElement )? SP? s_PropertyLookupTime ;

s_PropertyValueAtTElement :  '.' SP? oC_PropertyKeyName ( SP? PoundValue | s_AtTElement)? ;

s_PropertyLookupTime: AtT ( SP? oC_PropertyLookup )* ;

oC_PropertyLookup : '.' SP? oC_PropertyKeyName ( SP? s_AtTElement )? ;

s_TimePredicateExpression : SP ( DURING | OVERLAPS ) SP oC_AddOrSubtractExpression ;

s_AtTElement : AtT SP? '(' ( SP? s_TimePointLiteral SP? ',' )? SP? ( s_TimePointLiteral | NOW ) SP? ')';

s_Where
     :  WHERE SP s_WhereExpression ;

s_WhereExpression
          :  s_OrWhereExpression ;

s_OrWhereExpression
            :  s_XorWhereExpression ( SP OR SP s_XorWhereExpression )* ;

s_XorWhereExpression
             :  s_AndWhereExpression ( SP XOR SP s_AndWhereExpression )* ;

s_AndWhereExpression
             :  s_NotWhereExpression ( SP AND SP s_NotWhereExpression )* ;

s_NotWhereExpression
             :  ( NOT SP? )* s_ComparisonWhereExpression ;

s_ComparisonWhereExpression
                    :  s_StringListNullPredicateWhereExpression ( SP? s_ComparisonWhereOperator SP? s_StringListNullPredicateWhereExpression )* ;

s_MultiplyDivideModuloWhereOperator : '*' | '/' | '%' ;

s_PowerOfWhereOperator : '^' ;

s_AddOrSubtractWhereOperator : '+' | '-' ;

s_ComparisonWhereOperator : '=' | '<>' | '<' | '<=' | '>' | '>=' ;

s_StringListNullPredicateWhereExpression : s_AddOrSubtractWhereExpression ( s_TimePredicateWhereExpression | s_StringPredicateWhereExpression | s_ListPredicateWhereExpression | s_NullPredicateWhereExpression )? ;

s_AddOrSubtractWhereExpression
                       :  s_MultiplyDivideModuloWhereExpression ( ( SP? s_AddOrSubtractWhereOperator SP? s_MultiplyDivideModuloWhereExpression ) | ( SP? s_AddOrSubtractWhereOperator SP? s_MultiplyDivideModuloWhereExpression ) )* ;

s_TimePredicateWhereExpression : SP ( DURING | OVERLAPS ) SP s_AddOrSubtractWhereExpression ;

s_StringPredicateWhereExpression
                         :  ( ( SP STARTS SP WITH ) | ( SP ENDS SP WITH ) | ( SP CONTAINS ) ) SP? s_AddOrSubtractWhereExpression ;

s_ListPredicateWhereExpression
                       :  SP IN SP? s_AddOrSubtractWhereExpression ;

s_NullPredicateWhereExpression
                       :  ( SP IS SP NULL )
                           | ( SP IS SP NOT SP NULL )
                           ;

s_MultiplyDivideModuloWhereExpression
                              :  s_PowerOfWhereExpression ( ( SP? s_MultiplyDivideModuloWhereOperator SP? s_PowerOfWhereExpression ) | ( SP? s_MultiplyDivideModuloWhereOperator SP? s_PowerOfWhereExpression ) | ( SP? s_MultiplyDivideModuloWhereOperator SP? s_PowerOfWhereExpression ) )* ;

s_PowerOfWhereExpression
                 :  s_UnaryAddOrSubtractWhereExpression ( SP? s_PowerOfWhereOperator SP? s_UnaryAddOrSubtractWhereExpression )* ;

s_UnaryAddOrSubtractWhereExpression
                            :  s_ListOperatorWhereExpression
                                | ( ( '+' | '-' ) SP? s_ListOperatorWhereExpression )
                                ;

s_ListOperatorWhereExpression : ( s_PropertyOrLabelsWhereExpression | s_AtTWhereExpression ) ( s_SingleIndexWhereExpression | s_DoubleIndexWhereExpression )* ;

s_PropertyOrLabelsWhereExpression
                          :  oC_Atom ( SP? oC_PropertyLookup )* ( SP? oC_NodeLabels )? ;


s_AtTWhereExpression : oC_Atom ( ( SP? oC_PropertyLookup )+ ( SP? PoundValue )? )? SP? s_PropertyLookupTime ;

s_SingleIndexWhereExpression : SP? '[' s_LeftWhereExpression ']' ;

s_DoubleIndexWhereExpression : SP? '[' s_LeftWhereExpression? '..' s_RightWhereExpression? ']' ;

s_LeftWhereExpression : oC_Expression ;

s_RightWhereExpression : oC_Expression ;

s_TimePointLiteral : StringLiteral
                   | oC_MapLiteral
                   ;

oC_SymbolicName : UnescapedSymbolicName
                | EscapedSymbolicName
                | HexLetter
                | COUNT
                | FILTER
                | EXTRACT
                | ANY
                | NONE
                | SINGLE
                | NOW
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
                | AT_TIME
                | SNAPSHOT
                | BETWEEN
                | SCOPE
                | STALE
                | DURING
                | OVERLAPS
                ;

NOW : 'NOW' ;

AT_TIME : ( 'A' | 'a' ) ( 'T' | 't' ) '_' ( 'T' | 't' ) ( 'I' | 'i' ) ( 'M' | 'm' ) ( 'E' | 'e' ) ;

SNAPSHOT : ( 'S' | 's' ) ( 'N' | 'n' ) ( 'A' | 'a' ) ( 'P' | 'p') ( 'S' | 's' ) ( 'H' | 'h' ) ( 'O' | 'o') ( 'T' | 't' ) ;

BETWEEN : ( 'B' | 'b' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'W' | 'w' ) ( 'E' | 'e' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ;

SCOPE : ( 'S' | 's' ) ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'P' | 'p') ( 'E' | 'e' ) ;

STALE : ( 'S' | 's' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ;

DURING : ( 'D' | 'd' ) ( 'U' | 'u' ) ( 'R' | 'r' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'G' | 'g' ) ;

OVERLAPS : ( 'O' | 'o' ) ( 'V' | 'v' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ( 'L' | 'l' ) ( 'A' | 'a' ) ( 'P' | 'p' ) ( 'S' | 's' ) ;
