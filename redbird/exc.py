
class KeyFoundError(Exception):
    """Typically raised in insertion.
    
    Raised when an item for given key/id field
    is found when it was not expected.
    
    """

class ItemToDataError(Exception):
    "Raise when converting item to data failed"

class DataToItemError(Exception):
    "Raise when converting data to item failed"
