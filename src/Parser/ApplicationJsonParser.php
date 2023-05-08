<?php

namespace ZiffMedia\Ksql\Parser;

class ApplicationJsonParser implements ParserInterface
{
    public static function parse(string $data): array
    {
        $data = trim($data);
        if (str_starts_with($data, '[{')) {
            $data = substr($data, 1, -1);
        } else {
            $data = substr($data, 0, -1);
        }

        return json_decode($data, true);
    }
}
