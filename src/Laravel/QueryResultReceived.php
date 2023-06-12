<?php

namespace ZiffMedia\Ksql\Laravel;

use ArrayAccess;
use ArrayIterator;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Queue\SerializesModels;
use IteratorAggregate;
use Traversable;

class QueryResultReceived implements IteratorAggregate, ArrayAccess
{
    use Dispatchable, SerializesModels;

    public function __construct(public array $schema, public array $data)
    {
    }

    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->data);
    }

    public function offsetExists(mixed $offset): bool
    {
        return isset($this->data[$offset]);
    }

    public function offsetGet(mixed $offset): mixed
    {
        return $this->data[$offset];
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {
    }

    public function offsetUnset(mixed $offset): void
    {
    }
}
