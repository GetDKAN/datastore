<?php

use Locker\Locker;

class LockableBinStorageTest extends \PHPUnit\Framework\TestCase
{
  public function test() {
    $store = new \Dkan\Datastore\Storage\KeyValue\Memory();
    $locker = new Locker('test');
    $lockable = new \Dkan\Datastore\LockableBinStorage("test", $locker, $store);

    $lockable->set("1", "Hello World!!");
    $lockable->set("2", "Good bye World!!!");

    $this->assertEquals("Hello World!!", $lockable->get("1"));
    $this->assertEquals("Good bye World!!!", $lockable->get("2"));


    $bin = $lockable->borrowBin();

    $bin[1] = "Hola Mundo!!";

    $lockable->returnBin($bin);

    $this->assertEquals("Hola Mundo!!", $lockable->get("1"));
    $this->assertEquals("Good bye World!!!", $lockable->get("2"));
  }
}