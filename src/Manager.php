<?php

namespace Dkan\Datastore;

use Contracts\Parser;
use Dkan\Datastore\Storage\Storage;

class Manager
{
  const DATA_IMPORT_UNINITIALIZED = 1;
  const DATA_IMPORT_READY = 2;
  const DATA_IMPORT_IN_PROGRESS = 3;
  const DATA_IMPORT_PAUSED = 4;
  const DATA_IMPORT_DONE = 5;
  const DATA_IMPORT_ERROR = 6;

  private $storage;
  private $parser;
  private $resource;

  // State.
  private $numberOfChunksProcessed = 0;
  private $recordNumber = 0;
  private $timeLimit;
  private $status = ['data_import' => self::DATA_IMPORT_UNINITIALIZED];

  public function __construct(Resource $resource, Storage $storage, Parser $parser)
  {
    $this->storage = $storage;
    $this->parser = $parser;
    $this->resource = $resource;
  }

  public function setTimeLimit(int $seconds) {
    $this->timeLimit = $seconds;
  }

  public function unsetTimeLimit() {
    unset($this->timeLimit);
  }

  public function getStorage() {
    return $this->storage;
  }

  /**
   * {@inheritdoc}
   */

  public function import() {
    $maximum_execution_time = isset($this->timeLimit) ? (time() + $this->timeLimit) : PHP_INT_MAX;

    $h = fopen($this->resource->getFilePath(), 'r');

    while (time() < $maximum_execution_time) {
      $chunk = fread($h, 32);

      if (!$chunk) {
        break;
      }

      try {
        $this->parser->feed($chunk);
        $this->numberOfChunksProcessed++;
      }
      catch(\Exception $e) {}

      $this->store();
    }

    fclose($h);

    // Flush the parser.
    $this->parser->finish();
    $this->store();

    $this->status['data_import'] = self::DATA_IMPORT_DONE;
  }

  public function drop() {
    $results = $this->storage->retrieveAll();
    foreach ($results as $id => $data) {
      $this->storage->remove($id);
    }
    $this->status['data_import'] = self::DATA_IMPORT_UNINITIALIZED;
  }

  private function store() {
    while ($record = $this->parser->getRecord()) {
      // Skip the first record. It is the header.
      if ($this->recordNumber != 0) {
        $this->storage->store(json_encode($record), $this->recordNumber);
      }
      $this->recordNumber++;
    }
  }

  public function getStatus() {
    return $this->status;
  }

}