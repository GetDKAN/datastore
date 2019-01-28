<?php

namespace Dkan\Datastore\Storage\KeyValue;

class Memory {
  private $storage = [];

  public function set($key, $value) {
    $this->storage[$key] = $value;
  }

  public function get($key, $default = NULL) {
    if (isset($this->storage[$key])) {
      return $this->storage[$key];
    }
    else {
      if ($default) {
        $this->set($key, $default);
        return $this->get($key);
      }
      else {
        return NULL;
      }
    }
  }

}