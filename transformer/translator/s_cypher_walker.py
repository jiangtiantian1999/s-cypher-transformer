from transformer.grammar_parser.s_cypherListener import s_cypherListener


# This class records important information about the query
class SCypherWalker(s_cypherListener):

    def enterOC_Pattern(self, ctx):
        patterns = ctx.oC_PatternPart()
        print("patterns:")
        for pattern in patterns:
            print(pattern.getText())

    def enterOC_Where(self, ctx):
        print("where:")
        print(ctx.oC_Expression().getText())

    def enterOC_Return(self, ctx):
        print("return:")
        items = ctx.oC_ProjectionBody().oC_ProjectionItems().oC_ProjectionItem()
        for item in items:
            print(item.getText())
