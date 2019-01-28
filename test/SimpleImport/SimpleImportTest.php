<?php

use Dkan\Datastore\Manager\SimpleImport\SimpleImport;
use Dkan\Datastore\Resource;

class SimpleImportTest extends \PHPUnit\Framework\TestCase {

  public function test() {

    $database = new \Dkan\Datastore\Storage\Database\Memory();

    $resource = new Resource(1, __DIR__ . "/data/countries.csv");

    $provider = new \Dkan\Datastore\Manager\InfoProvider();
    $provider->addInfo(new \Dkan\Datastore\Manager\Info(SimpleImport::class, "simple_import", "SimpleImport"));

    $bin_storage = new \Dkan\Datastore\LockableBinStorage("dkan_datastore", new \Dkan\Datastore\Locker("dkan_datastore"), new \Dkan\Datastore\Storage\KeyValue\Memory());

    $factory = new \Dkan\Datastore\Manager\Factory($resource, $provider, $bin_storage, $database);

    /* @var $datastore \Dkan\Datastore\Manager\SimpleImport\SimpleImport */
    $datastore = $factory->get();

    $status = $datastore->getStatus();
    $this->assertEquals(SimpleImport::STORAGE_UNINITIALIZED, $status['storage']);
    $this->assertEquals(SimpleImport::DATA_IMPORT_UNINITIALIZED, $status['data_import']);

    $datastore->import();

    $status = $datastore->getStatus();
    $this->assertEquals(SimpleImport::STORAGE_INITIALIZED, $status['storage']);
    $this->assertEquals(SimpleImport::DATA_IMPORT_DONE, $status['data_import']);


    /*$query = db_select($datastore->getTableName(), "d");
    $query->fields("d");
    $results = $query->execute();
    $results = $results->fetchAllAssoc("country");
    $json = json_encode($results);
    $this->assertEquals(
      "{\"US\":{\"country\":\"US\",\"population\":\"315209000\",\"id\":\"1\",\"timestamp\":\"1359062329\"},\"CA\":{\"country\":\"CA\",\"population\":\"35002447\",\"id\":\"2\",\"timestamp\":\"1359062329\"},\"AR\":{\"country\":\"AR\",\"population\":\"40117096\",\"id\":\"3\",\"timestamp\":\"1359062329\"},\"JP\":{\"country\":\"JP\",\"population\":\"127520000\",\"id\":\"4\",\"timestamp\":\"1359062329 \"}}",
      $json);*/

    $this->assertEquals(4, $datastore->numberOfRecordsImported());

    $status = $datastore->getStatus();
    $this->assertEquals(SimpleImport::STORAGE_INITIALIZED, $status['storage']);
    $this->assertEquals(SimpleImport::DATA_IMPORT_DONE, $status['data_import']);

    $datastore->drop();
    $this->assertFalse($database->tableExist($datastore->getTableName()));

    $status = $datastore->getStatus();
    $this->assertEquals(SimpleImport::STORAGE_UNINITIALIZED, $status['storage']);
    $this->assertEquals(SimpleImport::DATA_IMPORT_UNINITIALIZED, $status['data_import']);
  }

}
