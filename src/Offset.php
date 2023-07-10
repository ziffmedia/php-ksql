<?php

namespace ZiffMedia\Ksql;

enum Offset: string
{
    case EARLIEST = 'earliest';
    case LATEST = 'latest';
}
