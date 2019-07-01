<?php
use Dkan\Datastore\Resource;

class ManagerTest extends \PHPUnit\Framework\TestCase
{
  public function testBasics() {
    $resource = new Resource(1, __DIR__ . "/data/countries.csv");
    $storage = new TestMemStorage();

    $datastore = new \Dkan\Datastore\Manager($resource, $storage,\CsvParser\Parser\Csv::getParser());
    $datastore->import();

    $this->assertEquals(4, $storage->count());
  }

}

class TestMemStorage extends Contracts\Mock\Storage\Memory implements \Dkan\Datastore\Storage\Storage {
  public function count(): int
  {
    return count($this->storage);
  }
}