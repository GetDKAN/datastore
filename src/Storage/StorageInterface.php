<?php


namespace Dkan\Datastore\Storage;

use Contracts\BulkRetrieverInterface;
use Contracts\CountableInterface;
use Contracts\StorerInterface;

interface StorageInterface extends
    StorerInterface,
    CountableInterface,
    BulkRetrieverInterface
{
    public function setSchema($schema);
    public function getSchema();
}
