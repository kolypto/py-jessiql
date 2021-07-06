import fastapi
import json
from typing import Optional, Any

from jessiql import QueryObject
from jessiql import exc

try:
    # Optional dependency; this module is only usable when it's installed
    import yaml
except ImportError:
    yaml = None


def query_object(*,
        select: Optional[str] = fastapi.Query(
            None,
            title='The list of fields to select.',
            description='Example: `[id, login, { users: {... } }]`. JSON or YAML.',
        ),
        join: Optional[str] = fastapi.Query(
            None,
            title='The list of relations to select.',
            description='Example: `[users: {...}]`. JSON or YAML.',
         ),
        filter: Optional[str] = fastapi.Query(
            None,
            title='Filter criteria.',
            description='MongoDB format. Example: `{ age: { $gt: 18 } }`. JSON or YAML.'
        ),
        sort: Optional[str] = fastapi.Query(
            None,
            title='Sorting order',
            description='List of columns with `+` or `-`. Example: `[ "login", "ctime-" ]`. JSON or YAML.',
        ),
        skip: Optional[int] = fastapi.Query(
            None,
            title='Pagination. The number of items to skip.'
        ),
        limit: Optional[int] = fastapi.Query(
            None,
            title='Pagination. The number of items to include.'
        ),
) -> Optional[QueryObject]:
    """ Get the JessiQL Query Object from the request parameters

    Example:
        /api/?select=[a, b, c]&filter={ age: { $gt: 18 } }

    Raises:
        exc.QueryObjectError
    """
    # Empty?
    if not select and not filter and not sort and not skip and not limit:
        return None

    # Query Object dict
    try:
        query_object_dict = dict(
            select=parse_serialized_argument('select', select),
            # join=parse_serialized_argument('join', join),
            filter=parse_serialized_argument('filter', filter),
            sort=parse_serialized_argument('sort', sort),
            skip=skip,
            limit=limit,
        )
    except ArgumentValueError as e:
        raise exc.QueryObjectError(f'Query Object `{e.argument_name}` parsing failed: {e}') from e

    # Parse
    query_object = QueryObject.from_query_object(query_object_dict)

    # Convert
    return query_object


class ArgumentValueError(ValueError):
    """ Query object field parse error """
    def __init__(self, argument_name: str, error: str):
        self.argument_name = argument_name
        super().__init__(error)


def _parse_yaml_argument(name: str, value: Optional[str]) -> Any:
    """ Parse a flattened QueryObject field as YAML """
    # None passthrough
    if value is None:
        return None

    # Parse the string
    try:
        return yaml.load(value, Loader=yaml.SafeLoader)
    except yaml.YAMLError as e:
        raise ArgumentValueError(name, str(e))


def _parse_json_argument(name: str, value: Optional[str]) -> Any:
    """ Parse a flattened QueryObject field as YAML """
    # None passthrough
    if value is None:
        return None

    # Parse the string
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise ArgumentValueError(name, f'Malformed JSON: {e}')

# Which parser to use?
if yaml:
    parse_serialized_argument = _parse_yaml_argument
else:
    parse_serialized_argument = _parse_json_argument
