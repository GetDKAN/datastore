<?php

namespace Dkan\Datastore;

use Contracts\Parser;
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
  private $timeLimit;

  public function __construct(Resource $resource, StorageInterface $storage, Parser $parser)
  {
    parent::__construct();

    $this->storage = $storage;
    $this->parser = $parser;
    $this->resource = $resource;
  }
  
  public function getStorage() {
    return $this->storage;
  }

  /**
   * {@inheritdoc}
   */
  public function runIt() {
    $maximum_execution_time = isset($this->timeLimit) ? (time() + $this->timeLimit) : PHP_INT_MAX;
    $result = $this->getResult();

    try {
      $h = fopen($this->resource->getFilePath(), 'r');
      while (time() < $maximum_execution_time) {
        $chunk = fread($h, 32);
  
        if (!$chunk) {
          break;
        }
        $this->parser->feed($chunk);
        $this->numberOfChunksProcessed++;
        $result->setStatus(Result::DONE);

        $this->store();
        $this->setStateProperty('chunks_processed', $this->numberOfChunksProcessed++);
      }
  
      fclose($h);
    }
    catch(\Exception $e) {
      $result->setStatus(Result::ERROR);
    }

    // Flush the parser.
    $this->parser->finish();
    $this->store();

    return $result;
  }

  public function drop() {
    $results = $this->storage->retrieveAll();
    foreach ($results as $id => $data) {
      $this->storage->remove($id);
    }
    $this->getResult()->setStatus(Result::STOPPED);
  }

  private function store() {
    while ($record = $this->parser->getRecord()) {
      // Skip the first record. It is the header.
      if ($this->recordNumber != 0) {
        $this->storage->store(json_encode($record), $this->recordNumber);
      }
      else {
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

  public function getParser(): Parser
  {
    return $this->parser;
  }

}