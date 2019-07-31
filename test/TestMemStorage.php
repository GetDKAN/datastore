<?php

namespace Dkan\DatastoreTest;

use Contracts\Mock\Storage\Memory;
use Dkan\Datastore\Storage\StorageInterface;

class TestMemStorage extends Memory implements StorageInterface
{

    use \Dkan\Datastore\Storage\Database\SqlStorageTrait;

    public function count(): int
    {
        return count($this->storage);
    }
}
