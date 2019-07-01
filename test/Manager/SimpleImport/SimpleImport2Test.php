<?php

use Contracts\Mock\Storage\Memory;
use Dkan\Datastore\Manager\SimpleImport\SimpleImport;
use Dkan\Datastore\Resource;
use Locker\Locker;

class SimpleImport2Test extends \PHPUnit\Framework\TestCase {

  private $database;

  /**
   * This method is called before each test.
   */
  protected function setUp()
  {
    $this->database = new TestMemStorage2();
  }

  private function getDatastore(Resource $resource) {
    return new \Dkan\Datastore\Manager($resource, $this->database, \CsvParser\Parser\Csv::getParser());
  }

  public function testBasics() {
    $resource = new Resource(1, __DIR__ . "/../../data/countries.csv");

    $datastore = $this->getDatastore($resource);

    $status = $datastore->getStatus();
    $this->assertEquals($datastore::DATA_IMPORT_UNINITIALIZED, $status['data_import']);

    $datastore->import();

    $status = $datastore->getStatus();
    $this->assertEquals($datastore::DATA_IMPORT_DONE, $status['data_import']);

    $this->assertEquals(4, $datastore->getStorage()->count());

    $status = $datastore->getStatus();
    $this->assertEquals($datastore::DATA_IMPORT_DONE, $status['data_import']);

    $datastore->drop();

    $status = $datastore->getStatus();
    $this->assertEquals($datastore::DATA_IMPORT_UNINITIALIZED, $status['data_import']);
  }

  public function testOver1000() {
    $resource = new Resource(1, __DIR__ . "/../../data/Bike_Lane.csv");

    $datastore = $this->getDatastore($resource);
    $datastore->import();

    $this->assertEquals(2969, $datastore->getStorage()->count());

    $results = $datastore->getStorage()->retrieveAll();
    $values = array_values($results);

    $expected = '["2049","75000403","R","1","DESIGNATED","0.076","0.364","463.2487"]';
    $this->assertEquals($expected, $values[0]);

    $expected = '["2048","75000402","R","1","DESIGNATED","0.769","1.713","1528.0913"]';
    $this->assertEquals($expected, $values[2968]);
  }
}

class TestMemStorage2 extends Contracts\Mock\Storage\Memory implements \Dkan\Datastore\Storage\Storage {
  public function count(): int
  {
    return count($this->storage);
  }
}
