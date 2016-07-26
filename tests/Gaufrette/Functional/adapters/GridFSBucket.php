<?php

$manager = new \MongoDB\Driver\Manager(
    'mongodb://localhost:27017',
    [
        'readPreference' => 'primaryPreferred',
        'connect' => false,
        'connectTimeoutMS' => 2000,
        'socketTimeoutMS' => 2000
    ]
);

$bucket = new \MongoDB\GridFS\Bucket(
    $manager,
    'gaufrette',
    [
        'bucketName' => 'filesBucket',
        'chunkSizeBytes' => 8192,
        'readPreference' => new \MongoDB\Driver\ReadPreference(\MongoDB\Driver\ReadPreference::RP_PRIMARY),
        'writeConcern' => new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY, 1000),
    ]
);
$bucket->drop();
return new Gaufrette\Adapter\GridFSBucket($bucket);