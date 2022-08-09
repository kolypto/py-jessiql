import nox.sessions

# Nox
nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'tests',
    'tests_sqlalchemy',
    'tests_graphql',
    'tests_fastapi',
]

# Versions
PYTHON_VERSIONS = ['3.9', '3.10']
SQLALCHEMY_VERSIONS = [
    # Selective: major releases
    # NOTE: keep major versions with breaking changes. Skip versions with minor bugfix changes.
    *(f'1.3.{x}' for x in (8, 11, 16, 24)),
    *(f'1.4.{x}' for x in (24, 26, 27, 28, 27, 29, 31, 32, 37, 39)),
]
GRAPHQL_CORE_VERSIONS = [
    '3.1.0', '3.1.1', '3.1.2', '3.1.3', '3.1.4', '3.1.5', '3.1.6', '3.1.7',
    '3.2.0', '3.2.1',
]
FASTAPI_VERSIONS = [
    #'0.51.0', '0.52.0', '0.53.2', '0.54.2', '0.56.1', '0.56.1', '0.57.0', '0.58.1', '0.59.0',  # all versions
    '0.51.0', '0.53.2', '0.54.2', '0.56.1', '0.59.0',
    #'0.60.2', '0.61.2', '0.62.0', '0.63.0', '0.64.0', '0.65.3', '0.66.1', '0.67.0', '0.68.2', '0.69.0',  # all versions
    '0.60.2', '0.62.0', '0.65.3', '0.68.2', '0.69.0',
    '0.70.1', '0.71.0', '0.72.0', '0.73.0', '0.74.1', '0.75.2', '0.76.0', '0.77.1', '0.78.0', '0.79.0',
]


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.sessions.Session, *, overrides: dict[str, str] = {}):
    """ Run all tests """
    # This approach works ok on GitHub but fails locally because we have Poetry within Poetry
    # session.install('poetry')
    # session.run('poetry', 'install')

    # This approach works better locally: install from requirements.txt
    session.install(*requirements_txt, '.')

    if overrides:
        session.install(*(f'{name}=={version}' for name, version in overrides.items()))

    # Test
    args = ['-k', 'not extra']
    if not overrides:
        args.append('--cov=jessiql')

    session.run('pytest', 'tests/', *args)


@nox.session(python=PYTHON_VERSIONS[-1])
@nox.parametrize('sqlalchemy', SQLALCHEMY_VERSIONS)
def tests_sqlalchemy(session: nox.sessions.Session, sqlalchemy):
    """ Test against a specific SqlAlchemy version """
    tests(session, overrides={'sqlalchemy': sqlalchemy})


@nox.session(python=PYTHON_VERSIONS[-1])
@nox.parametrize('graphql_core', GRAPHQL_CORE_VERSIONS)
def tests_graphql(session: nox.sessions.Session, graphql_core):
    """ Test against a specific GraphQL version """
    tests(session, overrides={'graphql-core': graphql_core})


@nox.session(python=PYTHON_VERSIONS[-1])
@nox.parametrize('fastapi', FASTAPI_VERSIONS)
def tests_fastapi(session: nox.sessions.Session, fastapi):
    """ Test against a specific FastAPI version """
    tests(session, overrides={'fastapi': fastapi})



# Get requirements.txt from poetry
import tempfile, subprocess
with tempfile.NamedTemporaryFile('w+') as f:
    subprocess.run(f'poetry export --no-interaction --dev --format requirements.txt --without-hashes --output={f.name}', shell=True, check=True)
    f.seek(0)
    requirements_txt = [line.split(';', 1)[0] for line in f.readlines()]  # after ";" go some Python version specifiers
