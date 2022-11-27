import pytest
import redbird

def test_build(request):
    expect_build = request.config.getoption('is_build')
    if not expect_build:
        assert redbird.version == '0.0.0.UNKNOWN'
    else:
        assert redbird.version != '0.0.0.UNKNOWN'