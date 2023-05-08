<?php

namespace ZiffMedia\Ksql\Parser;

interface ParserInterface
{
    public static function parse(string $data): array;
}
