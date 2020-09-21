# Simple DynamoDB client

The main idea here is to make the simplest operations simple and smooth over boto3's quirks.

Example use:

```python
from dynamo_client import Table, REMOVE_KEY

table = Table("myapp-users")
table.put_item({"id": 1, "name": "Jack Frost"})

updated_item = table.update_item({"id": 1, "email": "jfrost@northpole.io"})
```