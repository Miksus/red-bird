
import pytest
from pathlib import Path
import os
from dotenv import load_dotenv

def pytest_addoption(parser):
    parser.addoption(
        '--no-build',
        action='store_false',
        dest="is_build",
        default=True,
        help='Expect the package is not built.'
    )

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
    load_dotenv()
    pytest.importorskip("pymongo")
    if "MONGO_CONN" not in os.environ:
        pytest.skip()
    return os.environ["MONGO_CONN"]

@pytest.fixture(scope="function")
def collection(request, mongo_client):
    
    db_name = "pytest"
    col_name = get_node_id(request)

    collection = mongo_client[db_name][col_name]

    # Empty the collection
    collection.delete_many({})

    yield collection