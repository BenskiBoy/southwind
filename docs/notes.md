# custom faker
- increment (auto incrementing id)
- table_random(<table>, <field>, <default>) use an existing value in a table. If none yet created, use the default
- random([<value1>, <value2>])
- static(<value>)

# action types
An action will be performed on a random row
- create
- delete
- set
    - constraint

# Where Condition
for now very simple and relies on spaces
<table>.<field> [==,!=,>=,<=, >, <] val

# deletes
In order to actually capture deletes, deletes will simply be marked by setting the change_type to 'D'
Can have two types of behaviour set in the config field delete_behaviour
If set to 'HARD' - after handling the update and exporting the value, the deleted record(s) will be hard deleted
If set to 'SOFT' - after handling will leave the record in the backend, however subsequent updates and deletes will be filtered out
This is done for all tables.

# self notes
- need to ingest all of table definitions
- then need to confirm custom faker
- then need to ingest actions (and confirm actions)