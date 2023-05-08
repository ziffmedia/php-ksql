<?php

namespace ZiffMedia\Ksql;

enum ContentType: string {
    case APPLICATION_JSON = 'application/json';
    case V1_DELIMITTED = 'application/vnd.ksqlapi.delimited.v1';
    case V1_JSON = 'application/vnd.ksql.v1+json';
}
