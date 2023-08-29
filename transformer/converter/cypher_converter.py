from typing import List

from transformer.ir.s_cypher_clause import SCypherClause


class CypherConverter:
    count_num = 1000

    def get_random_variable(self, variables: List[str] = None):
        if variables is None:
            variables = []
        while 'var' + str(self.count_num) in variables:
            self.count_num = self.count_num + 1
        return 'var' + str(self.count_num)

    @staticmethod
    def convert_cypher_query(s_cypher_entity: SCypherClause) -> str:
        pass
