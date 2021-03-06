# Dynamesa: A Simple DynamoDB client

Makes the simplest operations simple, and smoothes over boto3's quirks.

Finding tables:

```python
import dynamesa

# boto3 resource kwargs
dynamesa.configure(region_name="us-east-1")

# Finds tables automatically
table = dynamesa.tables.MyAppUsers

# allows for weird table names
table = dynamesa.tables["My-App-Users"]

# Get Tables with a dataclass to get back items as instances instead of dictionaries 
table = dynamesa.tables.get("My-App-Users", MyDataClass)

# You can also instantiate a table yourself (requires it's own configuration)
table = dynamesa.Table("myapp-users", region_name="us-east-1")
```

Example use:
```python
import dynamesa

table = dynamesa.tables.create(
    "User",
    ("id", "S", "ts", "N"),
    gsis={"AgeIndex": ("age", "N")},
)

table.put({"id": 1, "name": "Jack Frost", "age": "I'll never tell"})
table.get(id=1)

updated_item = table.update({
  "id": 1,
  "email": "jfrost@northpole.io",
  "age": dynamesa.REMOVE_KEY
})

# updated_item == {"id": 1, "name": "Jack Frost", "email": "jfrost@northpole.io"}

# returns iterator, uses an index, name isn't in the index so also uses a filter expression
table.find("UserAgeIndex", age="74", name="King Cole")

# Use the primary key index
table.find(dynamesa.PRIMARY_KEY, id=1)

# Query or scan using boto3 Key expressions 
table.find(dynamesa.PRIMARY_KEY, dynamesa.Key("id").eq(1))

# will scan (returns iterator)
table.find(email="jfrost@northpole.io")

# delete users who are 74 years old (using the age index)
table.clear("UserAgeIndex", age=74)

# delete everything
table.clear()

# drop the table by reference
dynamesa.tables.delete(table)
```

# Example with Typed items

```python
import dataclasses
import time
import dynamesa


@dataclasses.dataclass
class UserModel:
    id: str
    name: str
    email: str
    age: int = None

dynamesa.tables.create(
    "User",
    ("id", "S", "ts", "N"),
    gsis={"AgeIndex": ("age", "N")},
)
table = dynamesa.tables.get('User', UserModel)
jack = UserModel(1, "Jack Frost", "jfrost@northpole.net")
table.put(jack)

user = table.get(id=1)
user.name == "Jack Frost"

# drop the table by name
dynamesa.tables.delete("User")
```

# Writing tests

```python
import unittest
import dynamesa

class TestMixinTests(dynamesa.DynamoUnitTestMixin, unittest.TestCase):
    dynamesa_table_name_prefix = "myapp-testsuite-"
    dynamesa_tables = [
        ("User", ("id", "S", "ts", "N")),
        {
            "table_name": "Blog",
            "pk": ("id", "S"),
            "gsis": {"ByOwner": ("owner", "S")},
        },
    ]
    dynamesa_configure = dict(
        region_name="localhost",
        endpoint_url="http://127.0.0.1:2808",
        aws_access_key_id="AKLOCAL",
        aws_secret_access_key="SKLOCAL",
    )

    def setUp(self) -> None:
        super().setUp()  # dynamesa creates/resets your tables here
        self.User = self.tables.get("User")

    def test_tables_created_with_name_prefix(self):
        self.assertEqual(self.User.table.name, "myapp-testsuite-User")

    def test_only_prefixed_tables_are_visible(self):
        # only tables with the name prefix are visible
        self.assertEqual(len(dynamesa.tables), 1)
```
