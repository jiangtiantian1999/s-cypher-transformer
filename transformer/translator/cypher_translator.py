from transformer.translator.s_cypher_walker import SCypherWalker
from transformer.ir.s_cypher_clause import SCypherClause


class CypherTranslator:
    @staticmethod
    def translate_s_cypher_query(s_cypher_walker: SCypherWalker) -> SCypherClause:
        pass

    @staticmethod
    def translate_match_clause():
        pass
