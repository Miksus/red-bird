
Filtering
=========


Operations
----------

Sometimes more sophisticated filtering operations
are needed such as getting items that are greater
than, less than etc. by specific field. For such
purpose, Red Bird provide operations.

.. code-block:: python

    from redbird.oper import greater_than, less_than, not_equal

    repo.filter_by(age=greater_than(30))

    repo.filter_by(age=less_than(30))

    repo.filter_by(age=not_equal(30))