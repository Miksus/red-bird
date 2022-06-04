import warnings

class KeyFoundError(Exception):
    """Typically raised in insertion.
    
    Raised when an item for given key/id field
    is found when it was not expected.
    
    """

class ItemToDataError(ValueError):
    "Raise when converting item to data failed"

class DataToItemError(ValueError):
    "Raise when converting data to item failed"


class ConversionWarning(UserWarning):
    "Converting data to item or item to data failed non-fatally"


def _handle_conversion_error(repo, data):
    errors_query = repo.errors_query
    if errors_query == "raise":
        raise
    elif errors_query == "warn":
        warnings.warn(f'Converting data to item failed: \n{data}', ConversionWarning)