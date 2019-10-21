<?php

namespace Dkan\Datastore;

use Contracts\ParserInterface;
use Dkan\Datastore\Storage\StorageInterface;
use Procrastinator\Job\AbstractPersistentJob;
use Procrastinator\Result;

class Importer extends AbstractPersistentJob
{
    private $dataStorage;
    private $parser;
    private $resource;
    
    protected function __construct(
        string $identifier,
        $storage,
        array $config = null
    ) {
        parent::__construct($identifier, $storage, $config);

        $this->dataStorage = $config['storage'];

        if (!($this->dataStorage instanceof StorageInterface)) {
            $storageInterfaceClass = StorageInterface::class;
            throw new \Exception("Storage must be an instance of {$storageInterfaceClass}");
        }

        $this->parser = $config['parser'];
        $this->resource = $config['resource'];
    }

    public function getStorage()
    {
        return $this->dataStorage;
    }

  /**
   * {@inheritdoc}
   */
    protected function runIt()
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
        if ($size <= $bytes) {
            return $result;
        }

        $maximum_execution_time = $this->getTimeLimit() ? (time() + $this->getTimeLimit()) : PHP_INT_MAX;

        try {
            $h = fopen($this->resource->getFilePath(), 'r');
            fseek($h, ($chunksProcessed) * 32);

            $this->parseAndStore($h, $maximum_execution_time, $result, $chunksProcessed);

            fclose($h);
        } catch (\Exception $e) {
            $result->setStatus(Result::ERROR);
        }

        // Flush the parser.
        $this->store();

        return $result;
    }

    private function parseAndStore($fileHandler, $maximumExecutionTime, Result $result, $chunksProcessed)
    {
        while (time() < $maximumExecutionTime) {
            $chunk = fread($fileHandler, 32);

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
    }

    public function drop()
    {
        $results = $this->dataStorage->retrieveAll();
        foreach ($results as $id => $data) {
            $this->dataStorage->remove($id);
        }
        $this->getResult()->setStatus(Result::STOPPED);
    }

    private function store()
    {
        $recordNumber = $this->getStateProperty('recordNumber', 0);
        while ($record = $this->parser->getRecord()) {
          // Skip the first record. It is the header.
            if ($recordNumber != 0) {
                $this->dataStorage->store(json_encode($record), $recordNumber);
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
        $this->dataStorage->setSchema($schema);
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
            'dataStorage' => $this->getStorage()->jsonSerialize(),
            'dataStorageClass' => get_class($this->getStorage())
        ];
    }

    public static function hydrate(string $json, $instance = null)
    {
        $data = json_decode($json);

        $reflector = new \ReflectionClass(self::class);

        $object = isset($instance) ? $instance : $reflector->newInstanceWithoutConstructor();

        self::hydrateJob($reflector, $object, $data);

        $p = $reflector->getProperty('resource');
        $p->setAccessible(true);
        $p->setValue($object, Resource::hydrate(json_encode($data->resource)));

        $classes = ['parser' => $data->parserClass, 'dataStorage' => $data->dataStorageClass];

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

    private static function hydrateJob($reflector, $object, $data)
    {
        $job = $reflector->getParentClass()->getParentClass();

        $p = $job->getProperty('timeLimit');
        $p->setAccessible(true);
        $p->setValue($object, $data->timeLimit);

        $p = $job->getProperty('result');
        $p->setAccessible(true);
        $p->setValue($object, Result::hydrate(json_encode($data->result)));
    }
}
