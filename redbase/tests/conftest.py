
import pytest
from pathlib import Path
import configparser

def get_node_id(request):
    components = request.node.nodeid.split("::")
    filename = components[0]
    test_class = components[1] if len(components) == 3 else None
    test_func_with_params = components[-1]
    test_func = test_func_with_params.split('[')[0]

    filename = filename.replace(".py", "").replace("/", "-")
    if test_class:
        return f'{filename}-{test_class}-{test_func_with_params}'
    else:
        return f'{filename}-{test_func_with_params}'

@pytest.fixture(scope="function")
def mongo_uri():
    config = configparser.ConfigParser()
    config.read("redbase/tests/private.ini")
    pytest.importorskip("pymongo")
    return config["connection"]["mongodb"]

@pytest.fixture(scope="function")
def collection(request, mongo_client):
    
    db_name = "pytest"
    col_name = get_node_id(request)

    collection = mongo_client[db_name][col_name]

    # Empty the collection
    collection.delete_many({})

    yield collection