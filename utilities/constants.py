DEFAULT_INSERT_CHUNK_SIZE = 10000
DEFAULT_MAX_FIELD_PCT = 0.15
DEFAULT_VARYING_LENGTH = True

JSON_OBJ_MAX_KEYS = 3
JSON_OBJ_MAX_VALS = 25

MYSQL_INT_MIN_MAX = {
    "MYSQL_MIN_TINYINT_SIGNED": -(2**7),
    "MYSQL_MAX_TINYINT_SIGNED": ~-(2**7),
    "MYSQL_MIN_SMALLINT_SIGNED": -(2**15),
    "MYSQL_MAX_SMALLINT_SIGNED": ~-(2**15),
    "MYSQL_MIN_MEDINT_SIGNED": -(2**23),
    "MYSQL_MAX_MEDINT_SIGNED": ~-(2 * 23),
    "MYSQL_MIN_INT_SIGNED": -(2**31),
    "MYSQL_MAX_INT_SIGNED": ~-(2**31),
    "MYSQL_MIN_BIGINT_SIGNED": -(2**63),
    "MYSQL_MAX_BIGINT_SIGNED": ~-(2 * 63),
    "MYSQL_MAX_TINYINT_UNSIGNED": ~-(2**8),
    "MYSQL_MAX_SMALLINT_UNSIGNED": ~-(2**16),
    "MYSQL_MAX_MEDINT_UNSIGNED": ~-(2**24),
    "MYSQL_MAX_INT_UNSIGNED": ~-(2**32),
    "MYSQL_MAX_BIGINT_UNSIGNED": ~-(2**64),
}

PHONE_NUMBER = {
    "au": lambda x: f"+61 02 {x[0:4]} {x[5:9]}",
    "de": lambda x: f"+49 030 {x[0:6]}-{x[6:8]}",
    "fr": lambda x: f"+33 01 {x[0:2]} {x[2:4]} {x[4:6]} {x[6:8]}",
    "ke": lambda x: f"+254 20 {x[0:3]} {x[3:6]}",
    "jp": lambda x: f"+81 03 {x[0:4]}-{x[4:8]}",
    "mx": lambda x: f"+52 55 {x[0:4]} {x[4:8]}",
    "ua": lambda x: f"+380 32 {x[0:3]}-{x[3:5]}-{x[5:7]}",
    "uk": lambda x: f"+44 0131 {x[0:4]} {x[4:8]}",
    "us": lambda x: f"+1 {x[0:3]}-{x[3:6]}-{x[6:10]}",
}
