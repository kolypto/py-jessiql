import os.path

# Load GraphQL definitions from the file
pwd = os.path.dirname(__file__)

# Get this schema
with open(os.path.join(pwd, './schema.graphql'), 'rt') as f:
    graphql_jessiql_schema = f.read()
