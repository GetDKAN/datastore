<?php

namespace Dkan\DatastoreTest;

use Dkan\Datastore\Resource;
use Dkan\Datastore\Importer;
use Dkan\Datastore\Storage\StorageInterface;
use Procrastinator\Result;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for Importer class.
 */
class ImporterTest extends TestCase
{

    private $database;

  /**
   * This method is called before each test.
   */
    protected function setUp(): void
    {
        $this->database = new TestMemStorage();
        $this->assertTrue($this->database instanceof StorageInterface);
    }

    private function getDatastore(Resource $resource)
    {
        return new Importer($resource, $this->database, \CsvParser\Parser\Csv::getParser());
    }

    public function testBasics()
    {
        $resource = new Resource(1, __DIR__ . "/data/countries.csv");
        $this->assertEquals($resource->getID(), 1);

        $datastore = $this->getDatastore($resource);

        $this->assertTrue($datastore->getParser() instanceof \Contracts\ParserInterface);
        $this->assertEquals(Result::STOPPED, $datastore->getResult()->getStatus());

        $datastore->runIt();

        $schema = $datastore->getStorage()->getSchema();
        $this->assertTrue(is_array($schema['fields']));

        $status = $datastore->getResult()->getStatus();
        $this->assertEquals(Result::DONE, $status);

        $this->assertEquals(4, $datastore->getStorage()->count());

        $datastore->drop();

        $status = $datastore->getResult()->getStatus();
        $this->assertEquals(Result::STOPPED, $status);
    }

    public function testError()
    {
        $resource = new Resource(1, __DIR__ . "/data/fake.csv");
        $datastore = $this->getDatastore($resource);
        $datastore->runIt();

        $this->assertEquals(Result::ERROR, $datastore->getResult()->getStatus());
    }

    public function testLongColumnName()
    {
        $resource = new Resource(1, __DIR__ . "/data/longcolumn.csv");
        $datastore = $this->getDatastore($resource);
        $truncatedLongFieldName = 'extra_long_column_name_with_tons_of_characters_that_will_ne_0';

        $datastore->runIt();
        $schema = $datastore->getStorage()->getSchema();
        $fields = array_keys($schema['fields']);

        $this->assertEquals($truncatedLongFieldName, $fields[2]);
    }

    public function testColumnNameSpaces()
    {
        $resource = new Resource(1, __DIR__ . "/data/columnspaces.csv");
        $datastore = $this->getDatastore($resource);
        $noMoreSpaces = 'column_name_with_spaces_in_it';

        $datastore->runIt();
        $schema = $datastore->getStorage()->getSchema();
        $fields = array_keys($schema['fields']);
        $this->assertEquals($noMoreSpaces, $fields[2]);
    }

    public function testSerialization()
    {
        $timeLimit = 40;
        $resource = new Resource(1, __DIR__ . "/data/countries.csv");
        $this->assertEquals($resource->getID(), 1);

        $datastore = $this->getDatastore($resource);
        $datastore->setTimeLimit($timeLimit);
        $datastore->runIt();
        $json = json_encode($datastore->jsonSerialize());

        $datastore2 = Importer::hydrate($json);

        $this->assertEquals(Result::DONE, $datastore2->getResult()->getStatus());
        $this->assertEquals($timeLimit, $datastore2->getTimeLimit());
    }

    public function testMultiplePasses()
    {
        $resource = new Resource(1, __DIR__ . "/data/Bike_Lane.csv");
        $datastore = $this->getDatastore($resource);

        // Hard to know, but unlikely that the file can be parsed in under one
        // second.
        $datastore->setTimeLimit(1);

        $datastore->runIt();
        $json = json_encode($datastore->jsonSerialize());
        // How many passes does it take to get through the data?
        $passes = 1;
        $results = $datastore->getStorage()->retrieveAll();

        while ($datastore->getResult()->getStatus() != Result::DONE) {
            $datastore = Importer::hydrate($json);
            $recordNumber = $datastore->getStateProperty('recordNumber', 0);
            $datastore->runIt();
            $json = json_encode($datastore);
            $results += $datastore->getStorage()->retrieveAll();
            $passes++;
        }
        // There needs to have been more than one pass for this test to be valid.
        $this->assertGreaterThan(1, $passes);

        // $results = $datastore->getStorage()->retrieveAll();
        $values = array_values($results);

        $expected = '["2049","75000403","R","1","DESIGNATED","0.076","0.364","463.2487"]';
        $this->assertEquals($expected, $values[0]);

        $expected = '["2048","75000402","R","1","DESIGNATED","0.769","1.713","1528.0913"]';
        $this->assertEquals($expected, $values[2968]);
    }
}
