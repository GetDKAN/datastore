<?php

namespace Dkan\DatastoreTest;

use Dkan\Datastore\Storage\StorageInterface;

/**
 * @todo Modify the storage class in Contracts to allow the necessary changes.
 */
class TestMemStorage implements StorageInterface, \JsonSerializable
{

    use \Dkan\Datastore\Storage\Database\SqlStorageTrait;

    protected $storage = [];

    public function retrieve(string $id)
    {
        if (isset($this->storage[$id])) {
            return $this->storage[$id];
        }
        return null;
    }

    public function retrieveAll(): array
    {
        return $this->storage;
    }

    public function store($data, string $id = null): string
    {
        if (!isset($id)) {
            $ids = array_keys($this->storage);
            if (empty($ids)) {
                $id = 0;
            }
            else {
                $id = array_unshift($ids) + 1;
            }
        }

        if (!isset($this->storage[$id])) {
            $this->storage[$id] = $data;
            return $id;
        }

        $this->storage[$id] = $data;

        return true;
    }

    public function remove(string $id)
    {
        if (isset($this->storage[$id])) {
            unset($this->storage[$id]);
            return true;
        }
        return false;
    }

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
