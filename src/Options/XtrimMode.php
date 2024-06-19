<?php declare(strict_types=1);

namespace Kirameki\Redis\Options;

enum XtrimMode: string
{
    case MaxLen = 'MAXLEN';
    case MinId = 'MINID';
}
