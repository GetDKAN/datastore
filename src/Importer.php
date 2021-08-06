<?php

namespace Dkan\Datastore;

use Contracts\ParserInterface;
use Dkan\Datastore\Storage\StorageInterface;
use Procrastinator\Job\AbstractPersistentJob;
use Procrastinator\Result;
use \ForceUTF8\Encoding;

class Importer extends AbstractPersistentJob
{
    protected $dataStorage;
    protected $parser;
    protected $resource;
    public const BYTES_PER_CHUNK = 8192;

    protected function __construct(
        string $identifier,
        $storage,
        array $config = null
    ) {
        parent::__construct($identifier, $storage, $config);

        $this->dataStorage = $config['storage'];

        if (!($this->dataStorage instanceof StorageInterface)) {
            $storageInterfaceClass = StorageInterface::class;
            throw new \Exception("Storage must be an instance of {$storageInterfaceClass}");
        }

        $this->parser = $config['parser'];
        $this->resource = $config['resource'];
    }

    public function getStorage()
    {
        return $this->dataStorage;
    }

    /**
     * {@inheritdoc}
     */
    protected function runIt()
    {
        $filename = $this->resource->getFilePath();
        $size = @filesize($filename);
        if (!$size) {
            return $this->setResultError("Can't get size from file {$filename}");
        }

        if ($size <= $this->getBytesProcessed()) {
            return $this->getResult();
        }

        $maximum_execution_time = $this->getTimeLimit() ? (time() + $this->getTimeLimit()) : PHP_INT_MAX;

        try {
            $this->assertTextFile($filename);

            $h = fopen($filename, 'r');
            fseek($h, $this->getBytesProcessed());

            $this->parseAndStore($h, $maximum_execution_time);

            fclose($h);
        } catch (\Exception $e) {
            return $this->setResultError($e->getMessage());
        }

        // Flush the parser.
        $this->store();

        if ($this->getBytesProcessed() >= $size) {
            $this->getResult()->setStatus(Result::DONE);
        } else {
            $this->getResult()->setStatus(Result::STOPPED);
        }

        return $this->getResult();
    }

    protected function assertTextFile(string $filename)
    {
        $mimeType = mime_content_type($filename);
        if ("text" != substr($mimeType, 0, 4)) {
            throw new \Exception("Invalid mime type: {$mimeType}");
        }
    }

    protected function setResultError($message): Result
    {
        $this->getResult()->setStatus(Result::ERROR);
        $this->getResult()->setError($message);
        return $this->getResult();
    }

    protected function getBytesProcessed()
    {
        $chunksProcessed = $this->getStateProperty('chunksProcessed', 0);
        return $chunksProcessed * self::BYTES_PER_CHUNK;
    }

    protected function parseAndStore($fileHandler, $maximumExecutionTime)
    {
        $chunksProcessed = $this->getStateProperty('chunksProcessed', 0);
        while (time() < $maximumExecutionTime) {
            $chunk = fread($fileHandler, self::BYTES_PER_CHUNK);

            if (!$chunk) {
                $this->getResult()->setStatus(Result::DONE);
                $this->parser->finish();
                break;
            }
            $chunk = Encoding::toUTF8($chunk);
            $this->parser->feed($chunk);
            $chunksProcessed++;

            $this->store();
            $this->setStateProperty('chunksProcessed', $chunksProcessed);
        }
    }

    public function drop()
    {
        $results = $this->dataStorage->retrieveAll();
        foreach ($results as $id => $data) {
            $this->dataStorage->remove($id);
        }
        $this->getResult()->setStatus(Result::STOPPED);
    }

    protected function store()
    {
        $recordNumber = $this->getStateProperty('recordNumber', 0);
        $records = [];
        foreach ($this->parser->getRecords() as $record) {
            // Skip the first record. It is the header.
            if ($recordNumber != 0) {
                // @todo Identify if we need to pass an id to the storage.
                $records[] = json_encode($record);
            } else {
                $this->setStorageSchema($record);
            }
            $recordNumber++;
        }
        if (!empty($records)) {
            $this->dataStorage->storeMultiple($records);
        }
        $this->setStateProperty('recordNumber', $recordNumber);
    }

    /**
     * Determine whether the supplied array is an associative array.
     *
     * @param array $arr
     *   Array being analyzed.
     *
     * @return bool
     *   TRUE if the array is associative and FALSE if it's sequential.
     */
    protected static function isAssoc(array $arr): bool
    {
        return array_keys($arr) !== range(0, count($arr) - 1);
    }

    /**
     * Set datastorage schema using the given table headers.
     *
     * @param array $headers
     *   Either an associative array of table columns keyed by header name or a
     *   sequential array of table columns without header names.
     *
     * @return void
     */
    protected function setStorageSchema(array $headers): void
    {
        // Ensure the supplied table fields are unique before proceeding.
        $this->assertUniqueHeaders($headers);

        // Determine whether the supplied table fields have formatted column
        // names specified as well.
        $hasNames = self::isAssoc($headers);

        // Generate schema array using the supplied table fields.
        $schema = [];
        foreach ($headers as $name => $field) {
            $schema['fields'][$field] = ['type' => 'text'];
            // If column names were supplied, set the field's description to
            // it's corresponding column name.
            if ($hasNames) {
                $schema['fields'][$field]['description'] = $name;
            }
        }

        $this->dataStorage->setSchema($schema);
    }

    /**
     * Verify headers are unique.
     *
     * @param $header
     *   List of strings
     *
     * @throws \Exception
     *   When a duplicate header is found.
     */
    protected function assertUniqueHeaders($header)
    {
        if (count($header) != count(array_unique($header))) {
            $duplicates = array_keys(array_filter(array_count_values($header), function ($i) {
                return $i > 1;
            }));
            throw new \Exception("Duplicate headers error: " . implode(', ', $duplicates));
        }
    }

    public function getParser(): ParserInterface
    {
        return $this->parser;
    }

    protected function serializeIgnoreProperties(): array
    {
        $ignore = parent::serializeIgnoreProperties();
        $ignore[] = "dataStorage";
        $ignore[] = "resource";
        return $ignore;
    }
}
