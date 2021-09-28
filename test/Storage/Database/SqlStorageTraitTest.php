<?php


namespace Dkan\DatastoreTest\Storage\Database;


use Dkan\Datastore\Storage\Database\SqlStorageTrait;
use Dkan\Datastore\Storage\StorageInterface;
use PHPUnit\Framework\TestCase;

class SqlStorageTraitTest extends TestCase
{
    /**
     * Re-cleaning an already clean schema should have not effects on it.
     */
    public function testBugCleaningSchemaTwiceModifiesIt() {

        $schema = [
            'fields' => [
                'footnotes_but_not_really_this_is_so_much_more_than_that_and_366e' => [
                    'description' => 'Footnotes but not really this is so much more than that and we are proud to say that this column name could possibly be longer than what is allowed so we will see some truncation'
                ]
            ]
        ];

        $object = new class() implements StorageInterface {
            use SqlStorageTrait;
        };

        $object->setSchema($schema);

        $this->assertEquals($schema, $object->getSchema());
    }

}
