<?php

namespace Dkan\Datastore;

use Contracts\ParserInterface;
use Contracts\Schemed;
use Dkan\Datastore\Storage\StorageInterface;
use Procrastinator\Job\Job;
use Procrastinator\Result;

class Importer extends Job
{
    private $storage;
    private $parser;
    private $resource;

    public function __construct(Resource $resource, StorageInterface $storage, ParserInterface $parser)
    {
        parent::__construct();

        $this->storage = $storage;
        $this->parser = $parser;
        $this->resource = $resource;
    }

    public function getStorage()
    {
        return $this->storage;
    }

  /**
   * {@inheritdoc}
   */
    public function runIt()
    {
        $chunksProcessed = $this->getStateProperty('chunksProcessed', 0);
        $result = $this->getResult();

        $size = @filesize($this->resource->getFilePath());
        if (!$size) {
            $result->setStatus(Result::ERROR);
            $result->setError("Can't get size from file {$this->resource->getFilePath()}");
            return $result;
        }
        $bytes = $chunksProcessed * 32;
        if (!$size || ($size <= $bytes)) {
            return $result;
        }

        $maximum_execution_time = $this->getTimeLimit() ? (time() + $this->getTimeLimit()) : PHP_INT_MAX;

        try {
            $h = fopen($this->resource->getFilePath(), 'r');
            fseek($h, ($chunksProcessed) * 32);
            while (time() < $maximum_execution_time) {
                $chunk = fread($h, 32);

                if (!$chunk) {
                    $result->setStatus(Result::DONE);
                    $this->parser->finish();
                    break;
                }
                $this->parser->feed($chunk);
                $chunksProcessed++;
                $result->setStatus(Result::STOPPED);

                $this->store();
                $this->setStateProperty('chunksProcessed', $chunksProcessed);
            }

            fclose($h);
        } catch (\Exception $e) {
            $result->setStatus(Result::ERROR);
        }

      // Flush the parser.
        $this->store();

        return $result;
    }

    public function drop()
    {
        $results = $this->storage->retrieveAll();
        foreach ($results as $id => $data) {
            $this->storage->remove($id);
        }
        $this->getResult()->setStatus(Result::STOPPED);
    }

    private function store()
    {
        $recordNumber = $this->getStateProperty('recordNumber', 0);
        while ($record = $this->parser->getRecord()) {
          // Skip the first record. It is the header.
            if ($recordNumber != 0) {
                $this->storage->store(json_encode($record), $recordNumber);
            } else {
                $this->setStorageSchema($record);
            }
            $recordNumber++;
        }
        $this->setStateProperty('recordNumber', $recordNumber);
    }

    private function setStorageSchema($header)
    {
        $schema = [];
        foreach ($header as $field) {
            $schema['fields'][$field] = [
            'type' => "text",
            ];
        }
        $this->storage->setSchema($schema);
    }

    public function getParser(): ParserInterface
    {
        return $this->parser;
    }

    public function jsonSerialize()
    {
        return (object) [
            'timeLimit' => $this->getTimeLimit(),
            'result' => $this->getResult()->jsonSerialize(),
            'parser' => $this->getParser()->jsonSerialize(),
            'parserClass' => get_class($this->getParser()),
            'resource' => $this->resource->jsonSerialize(),
            'storage' => $this->getStorage()->jsonSerialize(),
            'storageClass' => get_class($this->getStorage())
        ];
    }

    public static function hydrate($json): Importer
    {
        $data = parent::hydrate($json);

        $reflector = new \ReflectionClass(self::class);
        $object = $reflector->newInstanceWithoutConstructor();

        $reflector = new \ReflectionClass($object);

        $p = $reflector->getParentClass()->getProperty('timeLimit');
        $p->setAccessible(true);
        $p->setValue($object, $data->timeLimit);

        $p = $reflector->getProperty('resource');
        $p->setAccessible(true);
        $p->setValue($object, Resource::hydrate(json_encode($data->resource)));

        $p = $reflector->getParentClass()->getProperty('result');
        $p->setAccessible(true);
        $p->setValue($object, Result::hydrate(json_encode($data->result)));

        $classes = ['parser' => $data->parserClass, 'storage' => $data->storageClass];

        foreach ($classes as $property => $class_name) {
            if (class_exists($class_name) && method_exists($class_name, 'hydrate')) {
                $p = $reflector->getProperty($property);
                $p->setAccessible(true);
                $p->setValue($object, $class_name::hydrate(json_encode($data->{$property})));
            } else {
                throw new \Exception("Invalid {$property} class '{$class_name}'");
            }
        }

        return $object;
    }
}
