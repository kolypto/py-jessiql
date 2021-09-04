import nox.sessions


PYTHON_VERSIONS = ['3.7', '3.8', '3.9']
SQLALCHEMY_VERSIONS = [
    *(f'1.3.{x}' for x in range(0, 1 + 20)),
]


nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'tests',
    # 'tests_sqlalchemy',
]


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.sessions.Session, *, versions: list[str] = None):
    """ Run all tests """
    session.install('poetry')
    session.run('poetry', 'install')
    
    # Specific package versions
    if versions:
        session.install(*versions)

    # Test
    session.run('pytest', 'tests/', '--ignore=tests/test_mypy.py', '--cov=myproject')


@nox.session(python=PYTHON_VERSIONS[-1])
@nox.parametrize('sqlalchemy', SQLALCHEMY_VERSIONS)
def tests_sqlalchemy(session: nox.sessions.Session, sqlalchemy):
    """ Test against a specific SqlAlchemy version """
    tests(session, versions=[f'sqlalchemy=={sqlalchemy}'])
