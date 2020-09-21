# sentinal value
REMOVE_KEY = object()

class Table:
	def __init__(self, table_name, **kwargs):
		dynamodb = boto3.resource("dynamodb", **kwargs)
		self.table = dynamodb.Table(table_name)
		
	def put_item(self, item):
		self.table.put_item(Item=item)

	def update_item(self, update, return_values="ALL_NEW"):
		"""
		Takes a table and a dictionary of updates, extracts the primary key from the
		update dict and applies the remaining keys as an update to the record.
		"""
		table = self.table
		update = update.copy()
		pk = {}
		for k in table.key_schema:
			key = k["AttributeName"]
			if key not in update:
				raise ValueError(
					f"Couldn't update {table.table_name} because update dict is missing the {k['KeyType']} key, {key:!r}"
				)
			pk[key] = update.pop(key)

		if not update:
			raise ValueError("There were no updates to apply, update dict contained only the primary key")

		expression_attrs = {}
		expression_vals = {}
		set_parts = []
		remove_parts = []
		for i, (key, val) in enumerate(update.items()):
			expression_attrs[f"#a{i}"] = key
			if val is REMOVE_KEY:
				remove_parts.append(f"#a{i}")
			else:
				expression_vals[f":v{i}"] = val
				set_parts.append(f"#a{i} = :v{i}")

		update_expression = ""
		if set_parts:
			update_expression += "SET " + ", ".join(set_parts)
		if remove_parts:
			update_expression += " REMOVE" + ", ".join(remove_parts)

		kwargs = {}
		if expression_attrs:
			kwargs["ExpressionAttributeNames"] = expression_attrs
		if expression_vals:
			kwargs["ExpressionAttributeValues"] = expression_vals

		res = table.update_item(
			Key=pk,
			ReturnValues=return_values,
			UpdateExpression=update_expression,
			**kwargs,
		)
		return res.get("Attributes")
