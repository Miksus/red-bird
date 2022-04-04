
import re

def to_camelcase(string):
    "snake_case --> camelCase"
    # TODO: Split using regex (with also space)
    if is_camel_case(string):
        return string
    elif is_pascal_case(string) and not string.isupper():
        return string[0].lower() + string[1:]
    words = re.sub(r"[^A-Za-z0-9]+", "_", string).split("_")
    return ''.join(
        [
            word.capitalize() if i > 0 else word.lower()
            for i, word in enumerate(
                words
            )
        ]
    )


def to_snakecase(string):
    "--> snake_case"
    if is_snake_case(string):
        # Examples: snake_case
        return string
    elif string.isupper():
        # Examples: TITLE
        return string.lower()
    if is_camel_case(string) or is_pascal_case(string):
        # Put "_" before each capitalized letter (if not preceded with one like "TML" in "HTML")
        string = re.sub(r'(?!^)([A-Z][A-Z]*)', r'_\1', string)
    else:
        string = re.sub(r"[^A-Za-z0-9]+", "_", string)
    return (
        string
        .lower()
    )

def is_camel_case(string):
    return bool(re.match(r"^[a-z]+([A-Z][a-z]*)*$", string))

def is_snake_case(string):
    return bool(re.match(r"^[a-z_][0-9a-z_]*$", string))

def is_pascal_case(string):
    return bool(re.match(r"^([A-Z][a-z]*)+$", string))

def to_case(value:str, case:str):
    func = {
        "snake": to_snakecase,
        "camel": to_camelcase,
    }[case]
    return func(value)