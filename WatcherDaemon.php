<?php

namespace kfosoft\daemon;

use Yii;
use yii\base\ExitException;
use yii\base\InvalidRouteException;
use yii\console\Exception;
use yii\console\ExitCode;

/**
 * @package kfosoft\daemon
 * @version 20.05
 * @author (c) KFOSOFT <kfosoftware@gmail.com>
 */
abstract class WatcherDaemon extends Daemon
{
    /**
     * @var bool flag for first iteration
     */
    protected $firstIteration = true;

    /**
     * Prevent double start
     * @throws ExitException
     */
    public function init(): void
    {
        $pid_file = $this->getPidPath();
        if (file_exists($pid_file) && ($pid = file_get_contents($pid_file)) && $this->isProcessRunning($pid)) {
            $this->halt(ExitCode::UNSPECIFIED_ERROR, 'Another Watcher is already running.');
        }
        parent::init();
    }

    /**
     * {@inheritdoc}
     * @param array $job
     * @return bool
     * @throws ExitException
     * @throws InvalidRouteException
     * @throws Exception
     */
    protected function doJob(array $job): bool
    {
        $pid_file = $this->getPidPath($job['daemon']);

        Yii::debug(sprintf('Check daemon %s', $job['daemon']));
        if (file_exists($pid_file)) {
            $pid = file_get_contents($pid_file);
            if ($this->isProcessRunning($pid)) {
                if ($job['enabled']) {
                    Yii::debug(sprintf('Daemon %s running and working fine', $job['daemon']));

                    return true;
                } else {
                    Yii::warning(sprintf('Daemon %s running, but disabled in config. Send SIGTERM signal.', $job['daemon']));
                    if (isset($job['hardKill']) && $job['hardKill']) {
                        posix_kill($pid, SIGKILL);
                    } else {
                        posix_kill($pid, SIGTERM);
                    }

                    return true;
                }
            }
        }

        Yii::error('Daemon pid not found.');
        if ($job['enabled']) {
            Yii::debug(sprintf('Try to run daemon %s.', $job['daemon']));
            $command_name = sprintf('%s%sindex', $job['daemon'], DIRECTORY_SEPARATOR);
            //flush log before fork
            $this->flushLog(true);
            //run daemon
            $pid = pcntl_fork();
            if ($pid === -1) {
                $this->halt(ExitCode::UNSPECIFIED_ERROR, 'pcntl_fork() returned error');
            } elseif ($pid === 0) {
                $this->cleanLog();
                Yii::$app->requestedRoute = $command_name;
                Yii::$app->runAction("$command_name", ['demonize' => 1]);
                $this->halt(0);
            } else {
                $this->initLogger();
                Yii::debug(sprintf('Daemon %s is running with pid %s', $job['daemon'], $pid));
            }
        }

        Yii::debug(sprintf('Daemon %s is checked.', $job['daemon']));

        return true;
    }

    /**
     * @return array
     */
    protected function defineJobs()
    {
        if ($this->firstIteration) {
            $this->firstIteration = false;
        } else {
            sleep($this->sleep);
        }

        return $this->getDaemonsList();
    }

    /**
     * Daemons for check. Better way - get it from database
     * [
     *  ['daemon' => 'one-daemon', 'enabled' => true]
     *  ...
     *  ['daemon' => 'another-daemon', 'enabled' => false]
     * ]
     * @return array
     */
    abstract protected function getDaemonsList(): array;

    /**
     * @param $pid
     *
     * @return bool
     */
    public function isProcessRunning($pid)
    {
        return file_exists(sprintf('/proc/%s', $pid));
    }
}