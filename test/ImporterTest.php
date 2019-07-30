<?php

use Contracts\Mock\Storage\Memory;
use Dkan\Datastore\Resource;
use Locker\Locker;
use Procrastinator\Job\Job;
use Procrastinator\Result;
  
class SimpleImportTest extends \PHPUnit\Framework\TestCase {

  private $database;

  /**
   * This method is called before each test.
   */
  protected function setUp(): void
  {
    $this->database = new TestMemStorage();
  }

  private function getDatastore(Resource $resource) {
    return new \Dkan\Datastore\Importer($resource, $this->database, \CsvParser\Parser\Csv::getParser());
  }

  public function testBasics() {
    $resource = new Resource(1, __DIR__ . "/data/countries.csv");

    $datastore = $this->getDatastore($resource);

    $this->assertEquals(Result::STOPPED, $datastore->getResult()->getStatus());

    $datastore->runIt();

    $status = $datastore->getResult()->getStatus();
    $this->assertEquals(Result::DONE, $status);

    $this->assertEquals(4, $datastore->getStorage()->count());

    $datastore->drop();

    $status = $datastore->getResult()->getStatus();
    $this->assertEquals(Result::STOPPED, $status);
  }

  public function testError() {
    $resource = new Resource(1, __DIR__ . "/data/fake.csv");
    $datastore = $this->getDatastore($resource);
    $datastore->runIt();

    $this->assertEquals(Result::ERROR, $datastore->getResult()->getStatus());
  }

  public function testOver1000() {
    $resource = new Resource(1, __DIR__ . "/data/Bike_Lane.csv");

    $datastore = $this->getDatastore($resource);
    $datastore->runIt();

    $this->assertEquals(2969, $datastore->getStorage()->count());

    $results = $datastore->getStorage()->retrieveAll();
    $values = array_values($results);

    $expected = '["2049","75000403","R","1","DESIGNATED","0.076","0.364","463.2487"]';
    $this->assertEquals($expected, $values[0]);

    $expected = '["2048","75000402","R","1","DESIGNATED","0.769","1.713","1528.0913"]';
    $this->assertEquals($expected, $values[2968]);
  }
}

class TestMemStorage extends Contracts\Mock\Storage\Memory implements \Dkan\Datastore\Storage\Storage {
  public function count(): int
  {
    return count($this->storage);
  }
}
