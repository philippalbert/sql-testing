import random
import string
import uuid
from typing import Union


def get_random_list(specification: dict, length: int) -> list:
    """Get a list of random elements of a specific type

    Function generates a random list specified by a specification dictionary.
    Depending on the type of random value the dictionary can vary a bit.

    :Example:

        specification={
            type: Integer, values: random, value_range: [0, 5], unique: True
        }

    :param specification: dictionary with information about random type generation
    :type specification: dict
    :param length: length of list with random values
    :type length: int
    :return: list with random values of a specified type
    :rtype: list
    """

    if specification.get("type").lower() in ["boolean", "bool"]:
        return get_random_bool_list(specification.get("values"), length)

    elif specification.get("type").lower() in ["integer", "int"]:
        return get_random_int_list(
            specification.get("values"),
            specification.get("value_range"),
            length,
            specification.get("unique"),
        )

    elif specification.get("type").lower() in ["string", "str"]:
        return get_random_str_list(
            specification.get("values"),
            length,
            specification.get("unique"),
            specification.get("ignore"),
        )


def get_random_bool_list(values: Union[str, list], length: int, ignore=False) -> list:
    """Get a random list of boolean values

    :param values: list of values or random identifier
    :param length: identifier for list length
    :param ignore: whether to ignore or not (if True will be filled with True)
    """
    if ignore:
        return [True for _ in range(length)]

    if values == "random":
        return random.choices([True, False], k=length)

    return values


def get_random_int_list(values, value_range, length, is_unique, ignore=False):
    if ignore:
        return [0 for _ in range(length)]

    if values == "random":
        if value_range is not None:
            if is_unique:
                return random.sample(range(value_range[0], value_range[1]), k=length)
            else:
                return random.choices(range(value_range[0], value_range[1]), k=length)
        else:
            if is_unique:
                return random.sample(range(1, length + 1), length)
            else:
                return random.sample(range(1, int(length / 2)), length)
    else:
        return values


def get_random_str_list(values, length, is_unique, ignore=False):
    if ignore:
        return ["NULL" for _ in range(length)]

    if values == "random":

        if is_unique:
            return [uuid.uuid4().hex[:6] for _ in range(length)]
        else:
            return [
                "".join(random.sample(string.ascii_letters, 3)) for _ in range(length)
            ]


def get_random_suffix(length=5):
    """Create a random db object suffix"""
    chars = string.ascii_lowercase + string.digits
    random_choice = "".join(random.choice(chars) for _ in range(length))
    return random_choice
