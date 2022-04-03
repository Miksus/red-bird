
class KeyFoundError(Exception):
    """Typically raised in insertion.
    
    Raised when an item for given key/id field
    is found when it was not expected.
    
    """
