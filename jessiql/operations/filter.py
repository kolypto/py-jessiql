from collections import abc
from typing import Any, Union

import sqlalchemy as sa
import sqlalchemy.sql.operators
import sqlalchemy.sql.functions

import sqlalchemy.dialects.postgresql as pg  # TODO: FIXME: hardcoded dependency on Postgres!

from .base import Operation

from jessiql.query_object.filter import FilterExpressionBase, FieldFilterExpression, BooleanFilterExpression
from jessiql import exc
from jessiql.sainfo.columns import resolve_column_by_name
from jessiql.typing import SAModelOrAlias
from jessiql.sainfo.version import SA_13


class FilterOperation(Operation):
    """ Filter: applies a filter condition

    Handles: QueryObject.filter
    When applied to a statement:
    * Adds the WHERE clause
    """

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Modify the Select statement: add the WHERE clause """
        # Compile the conditions
        conditions = (
            self._compile_condition(condition)
            for condition in self.query.filter.conditions
        )

        # Add the WHERE clause
        if SA_13:
            stmt = stmt.where(sa.and_(*conditions))
        else:
            stmt = stmt.filter(*conditions)

        # Done
        return stmt

    def _compile_condition(self, condition: FilterExpressionBase) -> sa.sql.ColumnElement:
        """ Generate a SQL filter expression for the condition

        Args:
            condition: a field expression (field == value) or a bool expression (x AND y AND z)
        """
        # Field expressions
        if isinstance(condition, FieldFilterExpression):
            return self._compile_field_condition(condition)
        # Boolean expressions
        elif isinstance(condition, BooleanFilterExpression):
            return self._compile_boolean_conditions(condition)
        # Surprised facial expressions
        else:
            raise NotImplementedError(repr(condition))

    def _compile_field_condition(self, condition: FieldFilterExpression) -> sa.sql.ColumnElement:
        """ Generate an SQL statement for a field condition: e.g. "field == value"

        A field expression is represented by a class that encapsulates the following syntax:

            field operator value
        """
        # Resolve column
        condition.property = get_field_for_filtering(condition, self.target_Model, where='filter')
        col, val = condition.property, condition.value

        # Step 1. Prepare the column and the operand.

        # Case 1. Both column and value are arrays
        if condition.is_array and _is_array(val):
            # Cast the value to ARRAY[] with the same type that the column has
            # Only in this case Postgres will be able to handle them both
            val = sa.cast(pg.array(val), pg.ARRAY(col.type.item_type))

        # Case 2. JSON column
        if condition.is_json:
            # This is the type to which JSON column is coerced: same as `value`
            # Doc: "Suggest a type for a `coerced` Python value in an expression."
            coerce_type = col.type.coerce_compared_value('=', val)  # HACKY: use sqlalchemy type coercion
            # Now, replace the `col` used in operations with this new coerced expression
            col = sa.cast(col, coerce_type)  # type: ignore[type-var, assignment]

        # Step 2. Apply the operator.
        return self.use_operator(
            condition,
            col,  # type: ignore[arg-type]  # column expression
            val,  # value expression
        )

    def _compile_boolean_conditions(self, condition: BooleanFilterExpression) -> sa.sql.ColumnElement:
        """ Generate an SQL statement for a boolean expression: e.g. "x AND y AND z"

        A boolean expression is represented by a class that encapsulates the following syntax:

            operator ( expr, expr, expr )
        """
        # "$not" is special
        if condition.operator == '$not':
            # AND all clauses together
            criterion = sql_anded_together([
                self._compile_condition(c)
                for c in condition.clauses
            ])
            # now negate all of them
            return sa.not_(criterion)
        # "$and", "$or", "$nor" share some steps so they're handled together
        else:
            # Compile expressions
            criteria = [self._compile_condition(c) for c in condition.clauses]

            # Build an expression for $or and $nor
            # "nor" will later be finalized with a negation
            if condition.operator in ('$or', '$nor'):
                cc = sa.or_(*criteria)
            # Build an expression for $and
            elif condition.operator == '$and':
                cc = sa.and_(*criteria)
            # Oops
            else:
                raise NotImplementedError(f'Unsupported boolean operator: {condition.operator}')

            # Put parentheses around it when there are multiple clauses
            cc = cc.self_group() if len(criteria) > 1 else cc  # type: ignore[assignment]

            # Finalize $nor: negate the result
            # We do it after it's enclosed into parentheses
            if condition.operator == '$nor':
                return ~cc

            # Done
            return cc

    def use_operator(self, condition: FieldFilterExpression, column_expression: sa.sql.ColumnElement, value: sa.sql.ColumnElement) -> sa.sql.ColumnElement:
        """ Given a field and a value, apply an operator

        Args:
            condition: The Field Expression class that we use
            column_expression: The left operand
            value: The right operand

        Note that `column_expression` and `value` are likely different from what you have in `condition`:
        this is because some they may be turned into expressions that support arrays and JSON fields!
        """
        # Validate: check that it makes sense
        self._validate_operator_argument(condition)

        # Get the callable for the operator
        operator_lambda = self._get_operator_lambda(condition.operator, use_array=condition.is_array)

        # Apply the operator
        return operator_lambda(
            column_expression,  # left operand
            value,  # right operand
            condition.value  # original value
        )

    def _get_operator_lambda(self, operator: str, *, use_array: bool) -> abc.Callable[[sa.sql.ColumnElement, sa.sql.ColumnElement, Any], sa.sql.ColumnElement]:
        """ Get a callable that implements the operator

        Args:
            operator: Operator name
            use_array: Shall we use the array version of this operator?
        """
        # Find the operator
        try:
            if use_array:
                return self.ARRAY_OPERATORS[operator]
            else:
                return self.SCALAR_OPERATORS[operator]
        # Operator not found
        except KeyError:
            raise exc.QueryObjectError(f'Unsupported operator: {operator}')

    def _validate_operator_argument(self, condition: FieldFilterExpression):
        """ Validate or fail: that the operation and its arguments make sense

        Raises:
            exc.QueryObjectError
        """
        operator = condition.operator

        # See if this operator requires array argument
        if operator in self.ARRAY_OPERATORS_WITH_ARRAY_ARGUMENT:
            if not _is_array(condition.value):
                raise exc.QueryObjectError(f'Filter: {operator} argument must be an array')

    # region Library

    # Operators for scalar (e.g. non-array) columns
    # Mapping:
    #   'operator-name': lambda column, value, original_value
    #   `original_value` is to be used in conditions, because `val` can be an SQL-expression!
    SCALAR_OPERATORS = {
        '$eq': lambda col, val, oval: col == val,
        # "IS DISTINCT FROM" is a better rendering that considers NULLs properly
        '$ne': lambda col, val, oval: col.is_distinct_from(val),
        '$lt': lambda col, val, oval: col < val,
        '$lte': lambda col, val, oval: col <= val,
        '$gt': lambda col, val, oval: col > val,
        '$gte': lambda col, val, oval: col >= val,
        '$prefix': lambda col, val, oval: col.startswith(val),
        '$in': lambda col, val, oval: col.in_(val),  # field IN(values)
        '$nin': lambda col, val, oval: col.notin_(val),  # field NOT IN(values)
        '$exists': lambda col, val, oval: col != None if oval else col == None,
    }

    # Operators for array columns
    ARRAY_OPERATORS = {
        # array value: Array equality
        # scalar value: ANY(array) = value
        '$eq': lambda col, val, oval: col == val if _is_array(oval) else col.any(val),
        # array value: Array inequality
        # scalar value: ALL(array) != value
        '$ne': lambda col, val, oval: col != val if _is_array(oval) else col.all(val, sa.sql.operators.ne),
        # field && ARRAY[values]
        '$in': lambda col, val, oval: col.overlap(val),
        # NOT( field && ARRAY[values] )
        # Implementation is Postgres-specific
        '$nin': lambda col, val, oval: ~ col.overlap(val),
        # is not NULL
        '$exists': lambda col, val, oval: col != None if oval else col == None,
        # contains all values
        # Implementation is Postgres-specific
        '$all': lambda col, val, oval: col.contains(val),
        # value == 0: ARRAY_LENGTH(field, 1) IS NULL
        # value != 0: ARRAY_LENGTH(field, 1) == value
        '$size': lambda col, val, oval: sa.sql.functions.func.array_length(col, 1) == (None if oval == 0 else val),
    }

    # List of operators that always require array argument
    ARRAY_OPERATORS_WITH_ARRAY_ARGUMENT = frozenset(('$all', '$in', '$nin'))

    # List of boolean operators that operate on multiple conditional clauses
    BOOLEAN_OPERATORS = frozenset(('$and', '$or', '$nor', '$not'))

    @classmethod
    def add_scalar_operator(cls, name: str, callable: abc.Callable[[sa.sql.ColumnElement, Any, Any], sa.sql.ColumnElement]):
        """ Add an operator that operates on scalar columns

        NOTE: This will add an operator that is effective application-wide, which is not good.
        The correct way to do it would be to subclass FilterOperation

        Args:
            name: Operator name. For instance: $search
            callable: A function that implements the operator.
                Accepts three arguments: column, processed_value, original_value
        """
        cls.SCALAR_OPERATORS[name] = callable

    @classmethod
    def add_array_operator(cls, name: str, callable: abc.Callable[[sa.sql.ColumnElement, Any, Any], sa.sql.ColumnElement]):
        """ Add an operator that operates on array columns """
        cls.ARRAY_OPERATORS[name] = callable

    # endregion


def get_field_for_filtering(condition: FieldFilterExpression, Model: SAModelOrAlias, *, where: str):
    return resolve_column_by_name(condition.field, Model, where=where)


def _is_array(value):
    """ Is the provided value an array of some sorts (list, tuple, set)? """
    return isinstance(value, (list, tuple, set, frozenset))


def sql_anded_together(conditions: list[sa.sql.ColumnElement]) -> Union[sa.sql.ColumnElement, bool]:
    """ Take a list of conditions and join them together using AND. """
    # No conditions: just return True, which is a valid sqlalchemy expression for filtering
    if not conditions:
        return True

    # AND them together
    cc = sa.and_(*conditions)

    # Put parentheses around it, if necessary
    return cc.self_group() if len(conditions) > 1 else cc
