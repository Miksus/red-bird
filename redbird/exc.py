
class KeyFoundError(Exception):
    """Typically raised in insertion.
    
    Raised when an item for given key/id field
    is found when it was not expected.
    
    """

class ItemToDataError(ValueError):
    "Raise when converting item to data failed"

class DataToItemError(ValueError):
    "Raise when converting data to item failed"
