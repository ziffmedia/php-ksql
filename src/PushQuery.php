<?php

namespace ZiffMedia\Ksql;

class PushQuery extends Query
{
    /**
     * @param  callable  $handler
     */
    public function __construct(public string $name, public string $query, public $handler, public Offset $offset = Offset::EARLIEST)
    {
    }
}
