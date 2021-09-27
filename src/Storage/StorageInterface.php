<?php


namespace Dkan\Datastore\Storage;

use Contracts\BulkRetrieverInterface;
use Contracts\BulkStorerInterface;
use Contracts\CountableInterface;

interface StorageInterface extends
    BulkStorerInterface,
    CountableInterface,
    BulkRetrieverInterface
{
    /**
     * Accessor for schema property.
     *
     * @return array[]
     *  Schema property value.
     */
    public function getSchema(): array;

    /**
     * Mutator for schema property.
     *
     * @param array[]
     *   New schema property value.
     */
    public function setSchema(array $schema): void;
}
