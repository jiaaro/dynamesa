# A Simple DynamoDB client

Makes the simplest operations simple, and smoothes over boto3's quirks.

Example use:

```python
import dynamesa

# boto3 resource kwargs
dynamesa.configure(region_name="us-east-1")

# Finds tables automatically
table = dynamesa.tables.MyAppUsers

# allows for weird table names
table = dynamesa.tables["My-App-Users"]

# You can also instantiate a table yourself (requires it's own configuration)
table = dynamesa.Table("myapp-users", region_name="us-east-1")

table.put({"id": 1, "name": "Jack Frost", "age": "I'll never tell"})

updated_item = table.update({
  "id": 1,
  "email": "jfrost@northpole.io",
  "age": dynamesa.REMOVE_KEY
})

# updated_item == {"id": 1, "name": "Jack Frost", "email": "jfrost@northpole.io"}

# returns iterator, uses an index, name isn't in the index so also uses a filter expression
table.find("UserAgeIndex", age="74", name="King Cole")

# will scan (returns iterator)
table.find(email="jfrost@northpole.io")

# delete everything
table.clear()
```