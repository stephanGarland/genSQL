from textwrap import dedent


def make_skeleton() -> str:
    msg = """
        {
            "user_id": {
                "type": "bigint unsigned",
                "nullable": "false",
                "auto_increment": "true",
                "primary_key": "true"
            },
            "full_name": {
                "type": "varchar",
                "width": "255",
                "nullable": "false"
            },
            "external_id": {
                "type": "bigint unsigned",
                "nullable": "false",
                "unique": "true",
                "default": "0"
            },
            "last_modified": {
                "type": "timestamp",
                "nullable": "false",
                "default": "now()"
            }
        }
    """
    return msg


def extended_help():
    msg = f"""
    GenSQL expects a JSON input schema, of the format:

        {{
            "col_name": {{
                "col_type": "type",
                "col_option_0": "option",
                "col_option_n": "option"
            }}
        }}

    Valid column types <sizes> are:
        * smallint [unsigned]
        * int [unsigned]
        * bigint [unsigned]
        * decimal
        * double
        * char: <1 - 2^8-1>
        * varchar: <1 - 2^16-1>
        * timestamp
        * text
        * json

        NOTE: for char and varchar, you must also specify a size.
        NOTE: unsigned is only valid for MySQL.

    Valid column options are:
        * [var]char
            * width
        * integers
            * auto_increment
        * json, text
            * max_length: float <0.01 - 1.00>
              determines the maximum length of JSON arrays and TEXT columns
              percentage - defaults to 0.15 which gives 4-wide JSON arrays
              and 4 paragraphs of lorem ipsum text columns (~2900 chars)
        * all
            * default
            * invisible - NOTE: Only valid for MySQL
            * is_id
                * provides hints to gensql on whether or not to create integers
                  for a column if it cannot be automatically inferred
            * nullable - NOTE: absence implies true
            * primary_key
            * unique
    Valid default values are:
        * any constant
        * array()
            * creates a json array
        * now()
            * for a timestamp column, creates a default null value, and
              automatically updates with the current time if the row is updated
        * static_now()
            * for a timestamp column, creates a default value of the current time
              when the schema is loaded into a database
    e.g.
        {make_skeleton()}
    """
    print(dedent(msg))
    raise SystemExit(0)
