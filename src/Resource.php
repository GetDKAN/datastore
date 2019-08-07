<?php

namespace Dkan\Datastore;

/**
 * Class Resource.
 */
class Resource implements \JsonSerializable
{

    private $id;
    private $filePath;

  /**
   * Resource constructor.
   */
    public function __construct($id, $file_path)
    {
        $this->id = $id;
        $this->filePath = $file_path;
    }

  /**
   * Getter.
   */
    public function getId()
    {
        return $this->id;
    }

  /**
   * Getter.
   */
    public function getFilePath()
    {
        return $this->filePath;
    }

    public function jsonSerialize()
    {
        return (object) [
            'filePath' => $this->filePath,
            'id' => $this->id
        ];
    }

    public static function hydrate($json)
    {
        $data = json_decode($json);
        $reflector = new \ReflectionClass(self::class);
        $object = $reflector->newInstanceWithoutConstructor();

        $reflector = new \ReflectionClass($object);

        $p = $reflector->getProperty('filePath');
        $p->setAccessible(true);
        $p->setValue($object, $data->filePath);

        $p = $reflector->getProperty('id');
        $p->setAccessible(true);
        $p->setValue($object, $data->id);

        return $object;
    }
}
