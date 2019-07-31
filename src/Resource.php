<?php

namespace Dkan\Datastore;

/**
 * Class Resource.
 */
class Resource
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
}
