<?php

namespace Dkan\DatastoreTest;

use Contracts\Mock\Storage\Memory;
use Dkan\Datastore\Storage\StorageInterface;

class TestMemStorageBad extends Memory implements StorageInterface, \JsonSerializable
{

    use \Dkan\Datastore\Storage\Database\SqlStorageTrait;

    public function count(): int
    {
        return count($this->storage);
    }

    public function jsonSerialize()
    {
        return [];
    }
}
