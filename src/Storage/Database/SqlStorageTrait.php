<?php

namespace Dkan\Datastore\Storage\Database;

/**
 * Trait used for managing datastore SQL table schema.
 *
 * This trait is dependent on the StorageInterface interface.
 *
 * @see \Dkan\Datastore\Storage\StorageInterface
 */
trait SqlStorageTrait
{

    /**
     * Table schema.
     *
     * @var array[]
     */
    protected $schema;

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

            if (is_numeric($header) || in_array($header, self::RESERVED_WORDS)) {
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
                $header = substr($header, 0, self::MAX_COLUMN_LENGTH - strlen($suffix)) . $suffix;
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
    protected static function generateToken(string $seed): string
    {
        return substr(md5($seed), 0, 4);
    }
}
