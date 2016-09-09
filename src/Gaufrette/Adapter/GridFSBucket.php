<?php

namespace Gaufrette\Adapter;

use Gaufrette\Adapter;
use Gaufrette\Util;
use MongoDB\BSON\ObjectID;
use MongoDB\BSON\Regex;
use MongoDB\GridFS\Bucket;

/**
 * Adapter for the GridFS filesystem on GridFSBucket, MongoDB database PHP7 driver.
 *
 * @author Aleksandras Smirnovas <aleksandras.smirnovas@gmail.com>
 */
class GridFSBucket implements Adapter,
    ChecksumCalculator,
    StreamFactory,
    ListKeysAware,
    SizeCalculator
{
    protected $gridFSBucket = null;

    /**
     * @param Bucket $gridFSBucket
     */
    public function __construct(Bucket $gridFSBucket)
    {
        $this->gridFSBucket = $gridFSBucket;
    }

    /**
     * {@inheritdoc}
     */
    public function createStream($key)
    {
        $rStream = $this->gridFSBucket->openDownloadStreamByName($key);
        return $rStream;
    }

    /**
     * {@inheritdoc}
     */
    public function read($key)
    {
        $rStream = $this->gridFSBucket->openDownloadStreamByName($key);
        if (!$rStream) {
            return false;
        }
        $data = '';
        while (!feof($rStream)) {
            $data .= fread($rStream, 1024);
        }
        fclose($rStream);
        return $data;
    }

    /**
     * {@inheritdoc}
     */
    public function write($key, $content, array $metadata = [])
    {
        if (!is_resource($content) || get_resource_type($content) != 'stream') {
            $content = $this->createInMemoryStream($content);
        }
        $id = $this->gridFSBucket->uploadFromStream($key, $content, $metadata);
        return Util\Size::fromResource($content);
    }

    /**
     * {@inheritdoc}
     */
    public function isDirectory($key)
    {
        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function rename($sourceKey, $targetKey)
    {
        $file = $this->findFileByFilename($sourceKey);
        $this->gridFSBucket->rename($file->{'_id'}, $targetKey);
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function exists($key)
    {
        return (boolean)$this->findFileByFilename($key);
    }

    /**
     * {@inheritdoc}
     */
    public function keys()
    {
        $keys = array();
        $cursor = $this->gridFSBucket->find([]);

        foreach ($cursor as $file) {
            $keys[] = $file['filename'];
        }

        return $keys;
    }

    /**
     * {@inheritdoc}
     */
    public function mtime($key)
    {
        $file = $this->findFileByFilename($key);

        return ($file) ? (string)$file->uploadDate : false;
    }

    /**
     * {@inheritdoc}
     */
    public function checksum($key)
    {
        $file = $this->findFileByFilename($key);

        return ($file) ? (string)$file->md5 : false;
    }

    /**
     * {@inheritdoc}
     */
    public function size($key)
    {
        $file = $this->findFileByFilename($key);

        return ($file) ? (string)$file->length : false;
    }

    /**
     * {@inheritdoc}
     */
    public function delete($key)
    {
        $file = $this->findFileByFilename($key);
        $this->gridFSBucket->delete($file->{'_id'});
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function listKeys($prefix = '')
    {
        $prefix = trim($prefix);

        if ('' == $prefix) {
            return array(
                'dirs' => array(),
                'keys' => $this->keys(),
            );
        }

        $result = array(
            'dirs' => array(),
            'keys' => array(),
        );

        $gridFiles = $this->gridFSBucket->find([
            'filename' => new Regex(sprintf('/^%s/', $prefix), 'i'),
        ]);

        foreach ($gridFiles as $file) {
            $result['keys'][] = $file['filename'];;
        }

        return $result;
    }

    /**
     * Finds documents from the GridFS bucket's files collection by _id.
     *
     * @param string $key
     * @return null|object
     */
    private function findFileById($key)
    {
        $collection = $this->gridFSBucket->getCollectionWrapper();
        return $collection->findFileById(new ObjectID($key));
    }

    private function findFileByFilename($filename)
    {
        $collection = $this->gridFSBucket->getCollectionWrapper();
        //Revision numbers -1 = the most recent revision
        return $collection->findFileByFilenameAndRevision($filename, -1);
    }

    /**
     * Creates an in-memory stream with the given data.
     *
     * @param string $data
     * @return resource
     */
    protected function createInMemoryStream($data = '')
    {
        $stream = fopen('php://temp', 'w+b');
        fwrite($stream, $data);
        rewind($stream);
        return $stream;
    }
}
