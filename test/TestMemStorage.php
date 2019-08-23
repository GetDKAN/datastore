<?php

namespace Dkan\DatastoreTest;

use Contracts\Mock\Storage\Memory;
use Dkan\Datastore\Storage\StorageInterface;

class TestMemStorage extends Memory implements StorageInterface, \JsonSerializable
{

    use \Dkan\Datastore\Storage\Database\SqlStorageTrait;

    public function count(): int
    {
        return count($this->storage);
    }

    public function jsonSerialize()
    {
        return (object) ['storage' => $this->storage];
    }

    public static function hydrate(string $json)
    {
        $class = new \ReflectionClass(self::class);
        $instance = $class->newInstanceWithoutConstructor();

        $property = $class->getParentClass()->getProperty('storage');
        $property->setAccessible(true);
        $property->setValue($instance, (array) (json_decode($json))->storage);

        return $instance;
    }
}
