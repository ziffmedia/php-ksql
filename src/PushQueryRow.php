<?php

namespace ZiffMedia\Ksql;

use ArrayAccess;
use ArrayIterator;
use Countable;
use IteratorAggregate;
use Traversable;

class PushQueryRow implements ArrayAccess, IteratorAggregate, Countable
{
    public function __construct(
        public string $queryKey,
        public string $query,
        public string $queryId,
        public array $schema,
        public array $data,
    ) {
        $this->schema = array_change_key_case($this->schema);
        $this->data = array_change_key_case($this->data);
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
        return isset($this->data);
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
