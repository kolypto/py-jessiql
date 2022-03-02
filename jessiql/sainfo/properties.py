""" SqlAlchemy model properties: annotate, list """

import dis
import inspect
from functools import lru_cache
from collections import abc
from typing import Optional, TypeVar

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.hybrid

from jessiql import exc
from jessiql.sainfo.models import unaliased_class
from jessiql.sainfo.names import model_name
from jessiql.typing import SAModelOrAlias, saproperty

SameFunction = TypeVar('SameFunction')


def resolve_property_by_name(field_name: str, Model: SAModelOrAlias, *, where: str) -> saproperty:
    attribute = get_all_model_properties(unaliased_class(Model))[field_name]  # raises: KeyError

    # Sanity check
    assert isinstance(attribute, (property, sa.ext.hybrid.hybrid_property))

    # We only support properties annotated with @loads_attributes()
    if not is_annotated_with_loads(attribute):
        raise exc.InvalidColumnError(model_name(Model), field_name, where=where) from PropertyNotAnnotated(field_name)

    # Done
    return attribute


def loads_attributes(*attribute_names: str, check: bool = True) -> abc.Callable[[SameFunction], SameFunction]:
    """ Mark a @property with the list of attributes that it uses.

    Loaders that support it will take the information into account and try to avoid numerous lazy-loads.
    For instance, JessiQL only allow you to select properties that are explicitly annotated with @loads_attributes

    Example:
        class User(Base):
            ...

            @property
            @loads_attributes('age')
            def age_in_100_years(self):
                return self.age + 100

    Args:
        attribute_names: The names of attributes that the property touches.
        check: Check arguments by reading the code? Disable if gives errors.
            This increases your start-up time, but only by about 0.01ms per property
    """
    def wrapper(fget: SameFunction) -> SameFunction:
        # Check by reading the code
        if check:
            code_uses = tuple(func_uses_attributes(fget))  # type: ignore[arg-type]
            mismatched_attribute = set(code_uses).symmetric_difference(attribute_names)
            assert not mismatched_attribute, (
                f'Your @property uses different attributes from what it has described. '
                f'Mismatch: {mismatched_attribute}'
            )

        # Remember
        setattr(fget, '_loads_attributes', tuple(attribute_names))
        return fget
    return wrapper


def loads_attributes_readcode(*extra_attribute_names: str) -> abc.Callable[[SameFunction], SameFunction]:
    """ Mark a @property with @loads_attributes(), read those attributes' names from the code

    This decorator will extract all accessed attribute names from the code; you won't have to maintain the list.
    Wasted start-up time: about 0.01ms per property

    Args:
        *extra_attribute_names: Additional attribute names (e.g. from invisible nested functions)
    """
    # TODO: postpone reading code until it's actually needed. Save some start-up time.
    def wrapper(fget: SameFunction) -> SameFunction:
        return loads_attributes(
            *func_uses_attributes(fget),  # type: ignore[arg-type]
            *extra_attribute_names,
            check=False
        )(fget)
    return wrapper


def is_annotated_with_loads(prop: property) -> bool:
    """ Is the property annotated with @loads_attributes(prop, )ads_attributes? """
    return hasattr(prop.fget, '_loads_attributes')


def get_property_loads_attribute_names(prop: saproperty) -> Optional[tuple[str, ...]]:
    """ Get the list of attributes that a property requires """
    try:
        return prop.fget._loads_attributes  # type: ignore[union-attr]
    except AttributeError:
        return None


class PropertyNotAnnotated(AttributeError):
    """ Exception used to tell the developer that the problem is simply in the attribute itself """


@lru_cache(typed=True)
def get_all_model_properties(Model: type) -> dict[str, property]:
    """ Get all model properties """
    mapper: sa.orm.Mapper = sa.orm.class_mapper(Model)

    # Find all attributes
    properties = {}
    for name in dir(Model):
        # Ignore all protected properties.
        # Nothing good may come by exposing them!
        if name.startswith('_'):
            continue

        # @hybrid_property are special. They're descriptors.
        # This means that if we just getattr() it, it will get executed and generate an expression for us.
        # This is now what we want: we want the `hybridproperty` object itself.
        # So we either have to get it from class __dict__, or use this:
        if isinstance(mapper.all_orm_descriptors.get(name), sa.ext.hybrid.hybrid_property):
            properties[name] = mapper.all_orm_descriptors[name]
            continue

        # Ignore all known SqlAlchemy attributes here: they can't be @property-ies.
        if name in mapper.all_orm_descriptors:
            continue

        # Get the value
        attr = getattr(Model, name)

        # @property? Go for it.
        properties[name] = attr

    # Done
    return properties


def is_property(Model: SAModelOrAlias, attribute_name: str) -> bool:
    """ Is the provided value a @property? """
    return attribute_name in get_all_model_properties(unaliased_class(Model))


def func_uses_attributes(func: abc.Callable) -> abc.Iterator[str]:
    """ Find all patterns of `self.attribute` and return those attribute names

    Supports both methods (`self`), class methods (`cls`), and weirdos (any name for `self`)
    """
    first_arg_name = next(iter(inspect.signature(func).parameters))
    return code_uses_attributes(func, first_arg_name)


def code_uses_attributes(code, object_name: str = 'self') -> abc.Iterator[str]:
    """ Find all patterns of `object_name.attribute` and return those attribute names """
    # Look for the following patterns:
    #   Instruction(opname='LOAD_FAST', argval='self') followed by
    #   Instruction(opname='LOAD_ATTR', argval='<attr-name>')
    # or
    #   Instruction(opname='LOAD_FAST', argval='self') followed by
    #   Instruction(opname='STORE_ATTR', argval='<attr-name>')
    prev_instruction: Optional[dis.Instruction] = None
    for instruction in dis.get_instructions(code):
        if (
            instruction.opname in ('LOAD_ATTR', 'STORE_ATTR') and
            prev_instruction and
            prev_instruction.opname == 'LOAD_FAST' and
            prev_instruction.argval == object_name
            ):
            yield instruction.argval
        prev_instruction = instruction
