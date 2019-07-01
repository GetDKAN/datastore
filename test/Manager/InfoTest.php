<?php


class InfoTest extends \PHPUnit\Framework\TestCase
{
  public function test() {
    $info = new \Dkan\Datastore\Manager\Info("Class\\Class", "class", "Class");
    $this->assertEquals("Class\\Class", $info->getClass());
    $this->assertEquals("Class", $info->getLabel());
    $this->assertEquals("class", $info->getMachineName());
    $this->assertEquals("{\"class\":\"Class\\\\Class\",\"machine_name\":\"class\",\"label\":\"Class\"}", json_encode($info));
  }
}