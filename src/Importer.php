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

  // State.
    private $numberOfChunksProcessed = 0;
    private $recordNumber = 0;

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
        $maximum_execution_time = $this->getTimeLimit() ? (time() + $this->getTimeLimit()) : PHP_INT_MAX;
        $result = $this->getResult();
        try {
            $h = fopen($this->resource->getFilePath(), 'r');
            print "\nNumChunks: " . $this->numberOfChunksProcessed;
            fseek($h, ($this->numberOfChunksProcessed)*32);
            while (time() < $maximum_execution_time) {
                $chunk = fread($h, 32);
                print "\nCHUNK: $chunk\n";
                if (!$chunk) {
                    $result->setStatus(Result::DONE);
                    // break;
                }
                $this->parser->feed($chunk);
                $this->numberOfChunksProcessed++;
                $result->setStatus(Result::STOPPED);

                $this->store();
                $this->setStateProperty('chunks_processed', $this->numberOfChunksProcessed);
            }
            // print "\nCHUNK END: $chunk\n";

            fclose($h);
        } catch (\Exception $e) {
            $result->setStatus(Result::ERROR);
        }

      // Flush the parser.
        $this->parser->finish();
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
        while ($record = $this->parser->getRecord()) {
          // Skip the first record. It is the header.
            if ($this->recordNumber != 0) {
                $this->storage->store(json_encode($record), $this->recordNumber);
            } else {
                $this->setStorageSchema($record);
            }
            $this->recordNumber++;
        }
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
            'numberOfChunksProcessed' => $this->numberOfChunksProcessed,
            'recordNumber' => $this->recordNumber,
            'result' => $this->getResult()->jsonSerialize(),
            'parser' => $this->getParser()->jsonSerialize(),
            'parserClass' => get_class($this->getParser()),
            'resource' => $this->resource->jsonSerialize(),
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

        $p = $reflector->getProperty('numberOfChunksProcessed');
        $p->setAccessible(true);
        $p->setValue($object, $data->numberOfChunksProcessed);

        $p = $reflector->getProperty('recordNumber');
        $p->setAccessible(true);
        $p->setValue($object, $data->recordNumber);

        if (class_exists($data->parserClass) && method_exists($data->parserClass, 'hydrate')) {
            $p = $reflector->getProperty('parser');
            $p->setAccessible(true);
            $p->setValue($object, $data->parserClass::hydrate(json_encode($data->parser)));
        } else {
            throw new \Exception("Invalid parser class '{$data->parserClass}'");
        }

        $p = $reflector->getProperty('storage');
        $p->setAccessible(true);
        $p->setValue($object, new $data->storageClass);

        return $object;
    }
}
