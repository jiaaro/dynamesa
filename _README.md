# Simple DynamoDB client

The main idea here is to make the simplest operations simple and smooth over boto3's quirks.

Example use:

```python
from dynamo_client import Table, REMOVE_KEY

table = Table("myapp-users")
table.put({"id": 1, "name": "Jack Frost", "age": "I'll never tell"})

updated_item = table.update({
  "id": 1,
  "email": "jfrost@northpole.io",
  "age": REMOVE_KEY
})

# updated_item == {"id": 1, "name": "Jack Frost", "email": "jfrost@northpole.io"}

# returns iterator, uses primary key index
table.find(id=1)

# will scan (returns iterator)
table.find(email="jfrost@northpole.io")

# delete everything
table.clear()
```