<?php

namespace Dkan\Datastore\Storage\Database;

trait SqlStorageTrait
{
    private $schema;

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
    private function cleanSchema()
    {
        $cleanSchema = $this->schema;
        $cleanSchema['fields'] = [];
        foreach ($this->schema['fields'] as $field => $info) {
            $new = preg_replace("/[^A-Za-z0-9_ ]/", '', $field);
            $new = trim($new);
            $new = strtolower($new);
            $new = str_replace(" ", "_", $new);

            $mysqlMaxColLength = 64;
            if (strlen($new) >= $mysqlMaxColLength) {
                $strings = str_split($new, $mysqlMaxColLength - 5);
                $token = $this->generateToken($field);
                $new = $strings[0] . "_{$token}";
            }

            if ($field != $new) {
                $info['description'] = $field;
            }

            $cleanSchema['fields'][$new] = $info;
        }

        $this->schema = $cleanSchema;
    }

    public function setSchema($schema)
    {
        $this->schema = $schema;
        $this->cleanSchema();
    }

    public function getSchema()
    {
        return $this->schema;
    }

    public function generateToken($field) {
      $md5 = md5($field);
      return substr($md5, 0, 4);
    }
}
