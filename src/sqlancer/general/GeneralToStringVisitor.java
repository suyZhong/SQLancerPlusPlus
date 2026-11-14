package sqlancer.general;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.general.ast.GeneralCast;
import sqlancer.general.ast.GeneralColumnReference;
import sqlancer.general.ast.GeneralConstant;
import sqlancer.general.ast.GeneralExists;
import sqlancer.general.ast.GeneralExpression;
import sqlancer.general.ast.GeneralJoin;
import sqlancer.general.ast.GeneralSelect;
import sqlancer.general.ast.GeneralSelect.GeneralSubquery;

public class GeneralToStringVisitor extends NewToStringVisitor<GeneralExpression> {

    @Override
    public void visitSpecific(Node<GeneralExpression> expr) {
        if (expr instanceof GeneralConstant) {
            visit((GeneralConstant) expr);
        } else if (expr instanceof GeneralSelect) {
            visit((GeneralSelect) expr);
        } else if (expr instanceof GeneralJoin) {
            visit((GeneralJoin) expr);
        } else if (expr instanceof GeneralColumnReference) {
            visit((GeneralColumnReference) expr);
        } else if (expr instanceof GeneralCast) {
            visit((GeneralCast) expr);
        } else if (expr instanceof GeneralSubquery) {
            visit((GeneralSubquery) expr);
        } else if (expr instanceof GeneralExists) {
            visit((GeneralExists) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(GeneralJoin join) {
        visit(join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");
        if (join.getOuterType() != null) {
            sb.append(join.getOuterType());
        }
        sb.append(" JOIN ");
        visit(join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(GeneralConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(GeneralSelect select) {
        sb.append("SELECT ");
        if (select.isDistinct()) {
            sb.append("DISTINCT ");
        }
        visit(select.getFetchColumns());
        sb.append(" FROM ");
        visit(select.getFromList());
        if (!select.getFromList().isEmpty() && !select.getJoinList().isEmpty()) {
            sb.append(", ");
        }
        if (!select.getJoinList().isEmpty()) {
            visit(select.getJoinList());
        }
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByExpressions());
        }
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    private void visit(GeneralColumnReference column) {
        if (column.getColumn().getTable() == null) {
            sb.append(column.getColumn().getName());
        } else {
            sb.append(column.getColumn().getFullQualifiedName());
        }
    }

    private void visit(GeneralCast cast) {
        if (cast.isFunc()) {
            sb.append("CAST(");
            visit(cast.getExpr());
            sb.append(" AS ");
            sb.append(cast.getType());
            sb.append(")");
        } else {
            sb.append('(');
            visit(cast.getExpr());
            sb.append(')');
            sb.append("::");
            sb.append(cast.getType());
        }
    }

    private void visit(GeneralSubquery subquery) {
        sb.append(" (");
        visit(subquery.getSelect());
        sb.append(")");
        sb.append(" AS ");
        sb.append(subquery.getName());
        sb.append(" ");
    }

    private void visit(GeneralExists exists) {
        sb.append("(");
        if (exists.isNegated()) {
            sb.append("NOT ");
        }
        sb.append("EXISTS (");
        visit(exists.getSelect());
        sb.append("))");
    }

    public static String asString(Node<GeneralExpression> expr) {
        GeneralToStringVisitor visitor = new GeneralToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
