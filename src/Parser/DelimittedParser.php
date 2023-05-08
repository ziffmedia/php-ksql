<?php

namespace ZiffMedia\Ksql\Parser;

class DelimittedParser implements ParserInterface
{
    public static function parse(string $data): array
    {
        return json_decode($data, true);
    }
}
