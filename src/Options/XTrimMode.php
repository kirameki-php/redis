<?php declare(strict_types=1);

namespace Kirameki\Redis\Options;

enum XTrimMode: string
{
    case MaxLen = 'MAXLEN';
    case MinId = 'MINID';
}
