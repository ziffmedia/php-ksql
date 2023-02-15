<?php

namespace ZiffMedia\Ksql;

class PushQuery
{
    public string $queryId;

    /**
     * @param  string  $name
     * @param  string  $query
     * @param  callable  $handler
     * @param  Offset  $offset
     */
    public function __construct(public string $name, public string $query, public $handler, public Offset $offset = Offset::EARLIEST)
    {
    }
}
