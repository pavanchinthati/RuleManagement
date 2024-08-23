def format_as_sql_structure(data):
    # Initialize an empty dictionary to build the hierarchical structure
    nested_structures = {}

    # Parse each field and build a nested dictionary structure
    for key in data:
        parts = key.split('.')
        current = nested_structures
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = data[key]  # Assign the specified type

    def build_sql_structure(nested, level=0):
        indent = '    ' * level  # Manage indentation for readability
        entries = []
        for key, value in sorted(nested.items()):
            if isinstance(value, dict):
                # Recursive call to handle nested structures
                row_content = build_sql_structure(value, level + 1)
                entry = f"`{key}` row(\n{row_content}\n{indent})"
            elif isinstance(value, list):
                # Special handling for arrays
                entry = f"`{key}` array<{value[0]}>"
            else:
                # Simple types
                entry = f"`{key}` {value}"
            entries.append(indent + entry)
        return ',\n'.join(entries)

    # Build the complete SQL structure from the nested dictionary
    return build_sql_structure(nested_structures)

# Example usage with your provided fields and their corresponding data types
fields = {
    "uuid": "varchar",
    "organization.id": "varchar",
    "agent.type": "varchar",
    "tags": ["varchar"],
    "hostname": "varchar",
    "rule.id": "varchar",
    "rule.name": "varchar",
    "rule.category": "varchar",
    "related.hosts": ["varchar"],
    "related.hashes": ["varchar"],
    "related.user": ["varchar"],
    "related.ip": ["varchar"],
    "related.domains": ["varchar"],
    "related.emails": ["varchar"],
    "related.urls": ["varchar"],
    "related.ja3": ["varchar"],
    "observer.type": "varchar",
    "threatintel.malware.malware": "varchar",
    "host.name": "varchar",
    "event.module": "varchar",
    "event.provider": "varchar",
    "event.kind": "varchar",
    "event.outcome": "varchar",
    "network.transport": "varchar"
}

# Generate the SQL structure
sql_structure = format_as_sql_structure(fields)
print(sql_structure)


After Generating change "event.created" datatype timestamp to VARCHAR 
because when you are trying to replace it accepts only VARCHAR.
