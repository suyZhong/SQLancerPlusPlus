package sqlancer.general.ast;

import sqlancer.common.ast.newast.Node;

public class GeneralExists implements Node<GeneralExpression> {

    private final GeneralSelect select;
    private final boolean negated;

    public GeneralExists(GeneralSelect select, boolean negated) {
        this.select = select;
        this.negated = negated;
    }

    public GeneralSelect getSelect() {
        return select;
    }

    public boolean isNegated() {
        return negated;
    }

}
