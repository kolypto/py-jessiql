import nox.sessions

# Nox
nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'tests',
    # 'tests_sqlalchemy',
]

# Versions
PYTHON_VERSIONS = ['3.7', '3.8', '3.9', '3.10']
SQLALCHEMY_VERSIONS = [
    *(f'1.3.{x}' for x in range(7, 1 + 24)),
    *(f'1.4.{x}' for x in range(14, 1 + 25)),
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
        args.append('--cov=myproject')

    session.run('pytest', 'tests/', *args)


@nox.session(python=PYTHON_VERSIONS[-1])
@nox.parametrize('sqlalchemy', SQLALCHEMY_VERSIONS)
def tests_sqlalchemy(session: nox.sessions.Session, sqlalchemy):
    """ Test against a specific SqlAlchemy version """
    tests(session, overrides={'sqlalchemy': sqlalchemy})



# Get requirements.txt from external poetry
import tempfile, subprocess
with tempfile.NamedTemporaryFile('w+') as f:
    subprocess.run(f'poetry export --no-interaction --dev --format requirements.txt --without-hashes --output={f.name}', shell=True, check=True)
    f.seek(0)
    requirements_txt = [line.split(';', 1)[0] for line in f.readlines()]  # after ";" go some Python version specifiers
