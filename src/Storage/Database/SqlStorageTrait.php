<?php

namespace Dkan\Datastore\Storage\Database;

/**
 * Trait used for managing datastore SQL table schema.
 */
trait SqlStorageTrait
{
    /**
     * Resource table schema.
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
     *
     * @param array[]
     *   New schema property value.
     */
    public function setSchema(array $schema): void
    {
        $this->schema = $schema;
    }
}
