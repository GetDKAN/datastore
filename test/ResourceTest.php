<?php

namespace Dkan\DatastoreTest;

use Dkan\Datastore\Resource;
use PHPUnit\Framework\TestCase;

class ResourceTest extends TestCase
{
    public function test()
    {
        $resource = new Resource("3", "http://google.com", "text/csv");
        $serialized = json_encode($resource);
        $resource2 = Resource::hydrate($serialized);
        $this->assertEquals($resource->getId(), $resource2->getId());
        $this->assertEquals($resource->getFilePath(), $resource2->getFilePath());
        $this->assertEquals($resource->getMimeType(), $resource2->getMimeType());
    }
}
