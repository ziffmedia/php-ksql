<?php

namespace ZiffMedia\Ksql;

class PushQuery
{
    public string $queryId;

    /**
     * @param  callable  $handler
     */
    public function __construct(public string $name, public string $query, public $handler, public Offset $offset = Offset::EARLIEST)
    {
    }
}
