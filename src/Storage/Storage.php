<?php


namespace Dkan\Datastore\Storage;

use Contracts\BulkRetriever;
use Contracts\Storage as CStorage;
use Contracts\Countable;

interface Storage extends CStorage, Countable, BulkRetriever
{

}