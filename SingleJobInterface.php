<?php

namespace kfosoft\daemon;

/**
 * @package kfosoft\daemon
 * @version 20.05
 * @author (c) KFOSOFT <kfosoftware@gmail.com>
 */
interface SingleJobInterface
{
    public function sleepTime(): int;
}
