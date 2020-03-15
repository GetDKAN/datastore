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
    public function setSchema($schema);
    public function getSchema();
}
