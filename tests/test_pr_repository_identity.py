import pytest

from pa.pr_supervisor.models import (
    GitHubCapability,
    PRWatch,
    canonical_repository_name,
)


@pytest.mark.parametrize(
    "repository",
    [
        "PeterSky/Eschaton",
        " PeterSky/Eschaton.git/ ",
        "https://github.com/PeterSky/Eschaton.git",
        "HTTPS://GITHUB.COM/PeterSky/Eschaton.GIT/",
        "https://github.com:443/PeterSky/Eschaton",
        "ssh://github.com/PeterSky/Eschaton",
        "ssh://git@github.com/PeterSky/Eschaton.git",
        "SSH://git@GITHUB.COM:22/PeterSky/Eschaton.GIT/",
        "git@github.com:PeterSky/Eschaton.git",
        "github.com:PeterSky/Eschaton",
    ],
)
def test_supported_github_transports_share_repository_identity(repository):
    assert canonical_repository_name(repository) == "petersky/eschaton"
    watch = PRWatch(
        repository=repository,
        pr_number=2,
        pr_url="https://github.com/petersky/eschaton/pull/2",
    )
    assert watch.repository == "PeterSky/Eschaton"
    capability = GitHubCapability(
        instance_id="worker",
        authenticated=True,
        allowed_repositories=[repository],
    )
    assert capability.supports("petersky/eschaton")
    assert not capability.supports("petersky/pa")
    assert not capability.supports("other/eschaton")


@pytest.mark.parametrize(
    "repository",
    [
        "https://gitlab.com/petersky/eschaton",
        "ssh://git@gitlab.com/petersky/eschaton",
        "git@gitlab.com:petersky/eschaton",
        "https://github.com.evil.test/petersky/eschaton",
        "ssh://github.com.evil.test/petersky/eschaton",
        "git@github.com.evil.test:petersky/eschaton",
        "https://github.com@evil.test/petersky/eschaton",
        "https://user@github.com/petersky/eschaton",
        "ssh://git:password@github.com/petersky/eschaton",
        "http://github.com/petersky/eschaton",
        "file://github.com/petersky/eschaton",
        "https://github.com:8443/petersky/eschaton",
        "ssh://github.com:2222/petersky/eschaton",
        "ssh://github.com:invalid/petersky/eschaton",
        "https://github.com/petersky/eschaton?other/repo",
        "ssh://github.com/petersky/eschaton#other/repo",
        "petersky/eschaton?query",
        "petersky/eschaton#fragment",
        "petersky/%65schaton",
        "petersky/../eschaton",
        "petersky//eschaton",
        "https://github.com//petersky/eschaton",
        "petersky/eschaton/pull/2",
        "peter sky/eschaton",
        "petersky/esch\naton",
        "petersky/.",
        "petersky/..",
        "petersky/.git",
        "petersky/",
        "/eschaton",
        "",
    ],
)
def test_repository_identity_rejects_other_hosts_and_ambiguous_paths(repository):
    with pytest.raises(ValueError):
        canonical_repository_name(repository)


def test_repository_identity_preserves_valid_repository_punctuation():
    assert canonical_repository_name("Owner-1/repo.name_2-3") == "owner-1/repo.name_2-3"
