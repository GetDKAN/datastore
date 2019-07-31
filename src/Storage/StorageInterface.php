<?php


namespace Dkan\Datastore\Storage;

use Contracts\BulkRetriever;
use Contracts\Countable;
use Contracts\StorerInterface;

interface StorageInterface extends
    StorerInterface,
    Countable,
    BulkRetriever
{
    public function setSchema($schema);

    public function getSchema();
}
