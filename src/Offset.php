<?php
namespace ZiffMedia\Ksql;

enum Offset: string
{
    case Earliest = 'earliest';
    case Latest = 'latest';
}