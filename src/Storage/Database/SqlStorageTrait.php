<?php

namespace Dkan\Datastore\Storage\Database;

trait SqlStorageTrait
{

    /**
     * Table schema.
     *
     * @var array[]
     */
    protected $schema;

    /**
     * Returns the maximum length of a MySQL table column name.
     *
     * @return int
     *   Max length of a MySQL table column name.
     */
    protected static function getMaxColumnLength(): int
    {
        return 64;
    }

    /**
     * Returns a list of reserved words in MySQL 5.6-8 and MariaDB.
     *
     * @return string[]
     *   Reserved words list.
     */
    protected static function getReservedWords(): array
    {
        return ['accessible', 'add', 'all', 'alter', 'analyze', 'and', 'as',
            'asc', 'asensitive', 'before', 'between', 'bigint', 'binary',
            'blob', 'both', 'by', 'call', 'cascade', 'case', 'change', 'char',
            'character', 'check', 'collate', 'column', 'condition', 'constraint',
            'continue', 'convert', 'create', 'cross', 'cube', 'cume_dist',
            'current_date', 'current_role', 'current_time', 'current_timestamp',
            'current_user', 'cursor', 'database', 'databases', 'day_hour',
            'day_microsecond', 'day_minute', 'day_second', 'dec', 'decimal', 'declare',
            'default', 'delayed', 'delete', 'dense_rank', 'desc', 'describe',
            'deterministic', 'distinct', 'distinctrow', 'div', 'do_domain_ids',
            'double', 'drop', 'dual', 'each', 'else', 'elseif', 'empty', 'enclosed',
            'escaped', 'except', 'exists', 'exit', 'explain', 'false', 'fetch',
            'first_value', 'float', 'float4', 'float8', 'for', 'force', 'foreign',
            'from', 'fulltext', 'function', 'general', 'generated', 'get', 'grant',
            'group', 'grouping', 'groups', 'having', 'high_priority', 'hour_microsecond',
            'hour_minute', 'hour_second', 'if', 'ignore', 'ignore_domain_ids',
            'ignore_server_ids', 'in', 'index', 'infile', 'inner', 'inout',
            'insensitive', 'insert', 'int', 'int1', 'int2', 'int3', 'int4', 'int8',
            'integer', 'intersect', 'interval', 'into', 'io_after_gtids',
            'io_before_gtids', 'is', 'iterate', 'join', 'json_table', 'key', 'keys',
            'kill', 'lag', 'last_value', 'lateral', 'lead', 'leading', 'leave', 'left',
            'like', 'limit', 'linear', 'lines', 'load', 'localtime', 'localtimestamp',
            'lock', 'long', 'longblob', 'longtext', 'loop', 'low_priority',
            'master_bind', 'master_heartbeat_period', 'master_ssl_verify_server_cert',
            'match', 'maxvalue', 'mediumblob', 'mediumint', 'mediumtext', 'middleint',
            'minute_microsecond', 'minute_second', 'mod', 'modifies', 'natural', 'not',
            'no_write_to_binlog', 'nth_value', 'ntile', 'null', 'numeric', 'of',
            'offset', 'on', 'optimize', 'optimizer_costs', 'option', 'optionally',
            'or', 'order', 'out', 'outer', 'outfile', 'over', 'page_checksum',
            'parse_vcol_expr', 'partition', 'percent_rank', 'position', 'precision',
            'primary', 'procedure', 'purge', 'range', 'rank', 'read', 'reads',
            'read_write', 'real', 'recursive', 'references', 'ref_system_id', 'regexp',
            'release', 'rename', 'repeat', 'replace', 'require', 'resignal',
            'restrict', 'return', 'returning', 'revoke', 'right', 'rlike', 'row',
            'row_number', 'rows', 'schema', 'schemas', 'second_microsecond', 'select',
            'sensitive', 'separator', 'set', 'show', 'signal', 'slow', 'smallint',
            'spatial', 'specific', 'sql', 'sql_big_result', 'sql_calc_found_rows',
            'sqlexception', 'sql_small_result', 'sqlstate', 'sqlwarning', 'ssl',
            'starting', 'stats_auto_recalc', 'stats_persistent', 'stats_sample_pages',
            'stored', 'straight_join', 'system', 'table', 'terminated', 'then',
            'tinyblob', 'tinyint', 'tinytext', 'to', 'trailing', 'trigger', 'true',
            'undo', 'union', 'unique', 'unlock', 'unsigned', 'update', 'usage', 'use',
            'using', 'utc_date', 'utc_time', 'utc_timestamp', 'values', 'varbinary',
            'varchar', 'varcharacter', 'varying', 'virtual', 'when', 'where', 'while',
            'window', 'with', 'write', 'xor', 'year_month', 'zerofill',
        ];
    }

    /**
     * Accessor for schema property.
     *
     * @return array
     *  Schema property value.
     */
    public function getSchema(): array
    {
        return $this->schema;
    }

    /**
     * Mutator for schema property.
     */
    public function setSchema(array $schema): void
    {
        $this->schema = $this->cleanSchema($schema);
    }

    /**
     * Clean up and set the schema for SQL storage.
     *
     * @param array $header
     *   Header row from a CSV or other tabular data source.
     *
     * @param int $limit
     *   Maximum length of a column header in the target database. Defaults to
     *   64, the max length in MySQL.
     */
    protected static function cleanSchema(array $schema): array
    {
        $clean_schema = $schema;
        $clean_schema['fields'] = [];

        $updated_fields = [];
        foreach ($schema as $field => $info) {
            // Sanitize the supplied table header to generate a unique column name;
            // null-coalesce potentially NULL column names to empty strings.
            $header = self::sanitizeHeader($field ?? '');

            if (is_numeric($header) || in_array($header, self::getReservedWords())) {
                // Prepend "_" to column name that are not allowed in MySQL
                // This can be dropped after move to Drupal 9.
                // @see https://github.com/GetDKAN/dkan/issues/3606
                $header = '_' . $header;
            }

            // Truncate the generated table column name, if necessary, to fit the max
            // column length.
            $header = self::truncateHeader($header);

            // Generate unique numeric suffix for the header if a header already
            // exists with the same name.
            for ($i = 2; isset($headers[$header]); $i++) {
                $suffix = '_' . $i;
                $header = substr($header, 0, self::getMaxColumnLength() - strlen($suffix)) . $suffix;
            }

            $updated_fields[$header] = $info;
        }

        $clean_schema['fields'] = $updated_fields;

        return $clean_schema;
    }

    /**
     * Sanitize table column name according to the MySQL supported characters.
     *
     * @param string $column
     *   The column name being sanitized.
     *
     * @return string
     *   Sanitized column name.
     */
    protected static function sanitizeHeader(string $column): string
    {
        // Replace all spaces with underscores since spaces are not a supported
        // character.
        $column = str_replace(' ', '_', $column);
        // Strip unsupported characters from the header.
        $column = preg_replace('/[^A-Za-z0-9_ ]/', '', $column);
        // Trim underscores from the beginning and end of the column name.
        $column = trim($column, '_');
        // Convert the column name to lowercase.
        $column = strtolower($column);

        return $column;
    }

    /**
     * Truncate column name if longer than the max column length for the database.
     *
     * @param string $column
     *   The column name being truncated.
     *
     * @return string
     *   Truncated column name.
     */
    protected static function truncateHeader(string $column): string
    {
        // If the supplied table column name is longer than the max column length,
        // truncate the column name to 5 characters under the max length and
        // substitute the truncated characters with a unique hash.
        if (strlen($column) > self::getMaxColumnLength()) {
            $field = substr($column, 0, self::MAX_COLUMN_LENGTH - 5);
            $hash = self::generateToken($column);
            $column = $field . '_' . $hash;
        }

        return $column;
    }

    /**
     * Generate unique 4 character token based on supplied seed.
     *
     * @param string $seed
     *   Seed to use for string generation.
     *
     * @return string
     *   Unique 4 character token.
     */
    public static function generateToken(string $seed): string
    {
        return substr(md5($seed), 0, 4);
    }
}
