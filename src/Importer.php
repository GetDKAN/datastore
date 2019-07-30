<?php

namespace Dkan\Datastore;

use Contracts\Parser;
use Contracts\Schemed;
use Dkan\Datastore\Storage\Storage;
use Dkan\Datastore\Storage\TabularInterface;
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

  public function __construct(Resource $resource, Storage $storage, Parser $parser)
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

  private function setStorageSchema($header) {
    if ($this->storage instanceof TabularInterface) {
      $this->storage->setSchema($this->getTableSchema($header));
    }
  }

  private function getTableSchema($header) {
    $counter = 0;
    foreach ($header as $key => $field) {
      $new = preg_replace("/[^A-Za-z0-9_ ]/", '', $field);
      $new = trim($new);
      $new = strtolower($new);
      $new = str_replace(" ", "_", $new);

      if (strlen($new) >= 64) {
        $strings = str_split($new, 59);
        $new = $strings[0] . "_{$counter}";
        $counter++;
      }

      $header[$key] = $new;
    }

    $schema = [];
    foreach ($header as $field) {
      $schema['fields'][$field] = [
        'type' => "text",
      ];
    }
    return $schema;
  }

  public function getParser(): Parser
  {
    return $this->parser;
  }

}