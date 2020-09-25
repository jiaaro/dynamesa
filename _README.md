# A Simple DynamoDB client

The main idea here is to make the simplest operations simple and smooth over boto3's quirks.

Example use:

```python
import dynamesa

# boto3 resource kwargs
dynamesa.configure(region_name="us-east-1")

# Finds tables automatically
table = dynamesa.tables.MyAppUsers

# allows for weird table names
table = dynamesa.tables["My-App-Users"]

# You can also instantiate a table yourself
table = dynamesa.Table("myapp-users")

table.put({"id": 1, "name": "Jack Frost", "age": "I'll never tell"})

updated_item = table.update({
  "id": 1,
  "email": "jfrost@northpole.io",
  "age": dynamesa.REMOVE_KEY
})

# updated_item == {"id": 1, "name": "Jack Frost", "email": "jfrost@northpole.io"}

# returns iterator, uses primary key index
table.find(id=1)

# will scan (returns iterator)
table.find(email="jfrost@northpole.io")

# delete everything
table.clear()
```