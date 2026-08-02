from pa.packaging.service_env import service_values_equal


def test_list_service_values_compare_semantically() -> None:
    assert service_values_equal(
        "PA_SUBSCRIBED_REALMS", '["work", "home"]', "home, work"
    )
    assert service_values_equal(
        "PA_PEERS",
        '["HTTP://PEER-A:80/", "http://peer-b"]',
        'http://peer-b/, http://peer-a',
    )


def test_real_service_environment_drift_remains_detectable() -> None:
    assert not service_values_equal(
        "PA_PEERS", '["http://peer-a"]', '["http://peer-b"]'
    )
