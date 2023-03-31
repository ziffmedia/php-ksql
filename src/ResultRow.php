<?php

namespace ZiffMedia\Ksql;

use ArrayAccess;
use ArrayIterator;
use Countable;
use IteratorAggregate;
use Traversable;

class ResultRow implements ArrayAccess, IteratorAggregate, Countable
{
    public function __construct(
        public Query $query,
        public array $data,
    ) {
    }

    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->data);
    }

    public function count(): int
    {
        return count($this->data);
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
        // read only
    }

    public function offsetUnset(mixed $offset): void
    {
        //read only
    }

    public function __get($key)
    {
        return $this->data[$key];
    }
}
