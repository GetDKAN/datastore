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
        $size = @filesize($this->resource->getFilePath());
        if (!$size) {
            return $this->setResultError("Can't get size from file {$this->resource->getFilePath()}");
        }

        if ($size <= $this->getBytesProcessed()) {
            return $this->getResult();
        }

        $maximum_execution_time = $this->getTimeLimit() ? (time() + $this->getTimeLimit()) : PHP_INT_MAX;

        try {
            $h = fopen($this->resource->getFilePath(), 'r');
            fseek($h, $this->getBytesProcessed());

            $this->parseAndStore($h, $maximum_execution_time);

            fclose($h);
        } catch (\Exception $e) {
            return $this->setResultError($e->getMessage());
        }

        // Flush the parser.
        $this->store();

        if ($this->getBytesProcessed() >= $size) {
            $this->getResult()->setStatus(Result::DONE);
        } else {
            $this->getResult()->setStatus(Result::STOPPED);
        }

        return $this->getResult();
    }

    private function setResultError($message): Result
    {
        $this->getResult()->setStatus(Result::ERROR);
        $this->getResult()->setError($message);
        return $this->getResult();
    }

    private function getBytesProcessed()
    {
        $chunksProcessed = $this->getStateProperty('chunksProcessed', 0);
        return $chunksProcessed * 32;
    }

    private function parseAndStore($fileHandler, $maximumExecutionTime)
    {
        $chunksProcessed = $this->getStateProperty('chunksProcessed', 0);
        while (time() < $maximumExecutionTime) {
            $chunk = fread($fileHandler, 32);

            if (!$chunk) {
                $this->getResult()->setStatus(Result::DONE);
                $this->parser->finish();
                break;
            }
            $this->parser->feed($chunk);
            $chunksProcessed++;

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
                // @todo Ideintify if we need to pass an id to the storage.
                $this->dataStorage->store(json_encode($record));
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

    protected function serializeIgnoreProperties(): array
    {
        $ignore = parent::serializeIgnoreProperties();
        $ignore[] = "dataStorage";
        $ignore[] = "resource";
        return $ignore;
    }
}
