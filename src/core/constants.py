"""Project-wide constants."""

# SCD operation codes
OP_INSERT = "I"
OP_UPDATE = "U"
OP_DELETE = "D"

# SCD sentinel date for "no end date"
SCD_HIGH_DATE = "9999-12-31 23:59:59"

# Column names (defaults — overridable per table in app.yaml)
COL_CURRENT_ROW  = "current_row"
COL_EFFECTIVE_FROM = "effective_from"
COL_EFFECTIVE_TO   = "effective_to"
COL_IS_DELETED     = "is_deleted"
