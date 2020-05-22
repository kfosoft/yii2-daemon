<?php

namespace kfosoft\daemon;

use Yii;
use yii\base\Action;
use yii\base\ExitException;
use yii\base\NotSupportedException;
use yii\console\Controller;
use yii\console\ExitCode;
use yii\db\Exception as DbException;
use yii\helpers\Console;
use yii\log\FileTarget;

/**
 * @package kfosoft\daemon
 * @version 20.05
 * @author (c) KFOSOFT <kfosoftware@gmail.com>
 */
abstract class Daemon extends Controller
{
    protected const EVENT_BEFORE_JOB = 'EVENT_BEFORE_JOB';
    protected const EVENT_AFTER_JOB = 'EVENT_AFTER_JOB';

    protected const EVENT_BEFORE_ITERATION = 'event_before_iteration';
    protected const EVENT_AFTER_ITERATION = 'event_after_iteration';

    /**
     * @var bool Run controller as Daemon
     * @default false
     */
    public $demonize = false;

    /**
     * @var bool allow daemon create a few instances
     * @see $maxChildProcesses
     * @default false
     */
    public $isMultiInstance = false;

    /**
     * @var int main procces pid
     */
    protected $parentPID;

    /**
     * @var int max daemon instances
     * @default 10
     */
    public $maxChildProcesses = 10;

    /**
     * @var array array of running instances
     */
    protected static $currentJobs = [];

    /**
     * @var int Memory limit for daemon, must bee less than php memory_limit
     * @default 32M
     */
    protected $memoryLimit = 268435456;

    /**
     * @var bool used for soft daemon stop, set 1 to stop
     */
    protected static $stopFlag = false;

    /**
     * @var int Delay between task list checking
     * @default 5sec
     */
    protected $sleep = 5;

    /**
     * @var string
     */
    protected $pidDir = '@runtime/daemons/pids';

    /**
     * @var string
     */
    protected $logDir = '@runtime/daemons/logs';

    /**
     * @var resource|null
     */
    private $stdIn;

    /**
     * @var resource|null
     */
    private $stdOut;

    /**
     * @var resource|null
     */
    private $stdErr;

    /**
     * {@inheritdoc}
     */
    public function init(): void
    {
        parent::init();

        //set PCNTL signal handlers
        pcntl_signal(SIGTERM, [self::class, 'signalHandler']);
        pcntl_signal(SIGINT, [self::class, 'signalHandler']);
        pcntl_signal(SIGHUP, [self::class, 'signalHandler']);
        pcntl_signal(SIGUSR1, [self::class, 'signalHandler']);
        pcntl_signal(SIGCHLD, [self::class, 'signalHandler']);
    }

    public function __destruct()
    {
        $this->deletePid();
    }

    /**
     * Adjusting logger. You can override it.
     */
    protected function initLogger(): void
    {
        $targets = Yii::$app->getLog()->targets;

        foreach ($targets as $name => $target) {
            $target->enabled = false;
        }

        $config = [
            'levels' => ['error', 'warning', 'trace', 'info'],
            'logFile' => sprintf('%s%s%s.log', Yii::getAlias($this->logDir), DIRECTORY_SEPARATOR, $this->getProcessName()),
            'logVars' => [],
            'except' => [
                'yii\db\*', // Don't include messages from db
            ],
        ];

        $targets['daemon'] = new FileTarget($config);
        Yii::$app->getLog()->targets = $targets;
        Yii::$app->getLog()->init();
    }

    /**
     * Daemon worker body
     *
     * @param array|string $job
     *
     * @return bool
     */
    abstract public function __invoke($job): bool;

    /**
     * Base action, you can't override or create another actions
     * @return int
     *
     * @throws DbException
     * @throws ExitException
     */
    final public function actionIndex(): ?int
    {
        if ($this->demonize) {
            $pid = pcntl_fork();
            if ($pid == -1) {
                $this->halt(ExitCode::UNSPECIFIED_ERROR, 'pcntl_fork() rise error');
            } elseif ($pid) {
                $this->cleanLog();
                $this->halt(ExitCode::OK);
            } else {
                posix_setsid();
                $this->closeStdStreams();
            }
        }
        $this->changeProcessName();

        //run loop
        return $this->loop();
    }

    /**
     * Set new process name
     */
    protected function changeProcessName(): void
    {
        cli_set_process_title($this->getProcessName());
    }

    /**
     * Close std streams and open to /dev/null
     * need some class properties
     */
    protected function closeStdStreams()
    {
        if (is_resource(STDIN)) {
            fclose(STDIN);
            $this->stdIn = fopen('/dev/null', 'r');
        }
        if (is_resource(STDOUT)) {
            fclose(STDOUT);
            $this->stdOut = fopen('/dev/null', 'ab');
        }
        if (is_resource(STDERR)) {
            fclose(STDERR);
            $this->stdErr = fopen('/dev/null', 'ab');
        }
    }

    /**
     * Prevent non index action running
     *
     * @param Action $action
     *
     * @return bool
     * @throws NotSupportedException
     */
    public function beforeAction($action): bool
    {
        if (parent::beforeAction($action)) {
            $this->initLogger();
            if ($action->id !== 'index') {
                throw new NotSupportedException('Only index action allowed in daemons. So, don\'t create and call another');
            }

            return true;
        } else {
            return false;
        }
    }

    /**
     * List options
     *
     * @param string $actionID
     *
     * @return array
     */
    public function options($actionID): array
    {
        return [
            'demonize',
            'taskLimit',
            'isMultiInstance',
            'maxChildProcesses',
        ];
    }

    /**
     * Fetch one task from array of tasks
     *
     * @param array $jobs
     *
     * @return mixed one task
     */
    protected function defineJobExtractor(array &$jobs)
    {
        return array_shift($jobs);
    }

    /**
     * Main Loop
     *
     * @return int|null 0|1
     *
     * @throws DbException
     * @throws ExitException
     */
    final private function loop(): ?int
    {
        if (file_put_contents($this->getPidPath(), getmypid())) {
            $this->parentPID = getmypid();
            Yii::debug(sprintf('Daemon %s pid %s started.', $this->getProcessName(), getmypid()));
            while (!self::$stopFlag) {
                if (memory_get_usage() > $this->memoryLimit) {
                    Yii::debug(sprintf('Daemon %s pid %s used %s bytes on %s bytes allowed by memory limit.', $this->getProcessName(), getmypid(), memory_get_usage(), $this->memoryLimit));
                    break;
                }
                $this->trigger(self::EVENT_BEFORE_ITERATION);
                $this->renewConnections();

                $jobs = $this instanceof SingleJobInterface ? ['className' => static::class, 'enabled' => true] : $this->defineJobs();
                if ($jobs && !empty($jobs)) {
                    while (($job = $this->defineJobExtractor($jobs)) !== null) {
                        //if no free workers, wait
                        if ($this->isMultiInstance && (count(static::$currentJobs) >= $this->maxChildProcesses)) {
                            Yii::debug('Reached maximum number of child processes. Waiting...');
                            while (count(static::$currentJobs) >= $this->maxChildProcesses) {
                                sleep(1);
                                pcntl_signal_dispatch();
                            }

                            Yii::debug(sprintf('Free workers found: %s worker(s). Delegate tasks.', $this->maxChildProcesses - count(static::$currentJobs)));
                        }

                        pcntl_signal_dispatch();
                        $this->runDaemon($job);
                    }
                } else {
                    sleep($this->sleep);
                }

                pcntl_signal_dispatch();
                $this->trigger(self::EVENT_AFTER_ITERATION);

                if ($this instanceof SingleJobInterface) {
                    sleep($this->sleepTime());
                }
            }

            Yii::info(sprintf('Daemon %s pid %s is stopped.', $this->getProcessName(), getmypid()));

            return ExitCode::OK;
        }

        $this->halt(ExitCode::UNSPECIFIED_ERROR, sprintf('Can\'t create pid file %s.', $this->getPidPath()));

        return null;
    }

    /**
     * Delete pid file
     */
    protected function deletePid(): void
    {
        $pid = $this->getPidPath();
        if (file_exists($pid)) {
            if (file_get_contents($pid) == getmypid()) {
                unlink($this->getPidPath());
            }

            return;
        }

        Yii::error(sprintf('Can\'t unlink pid file %s', $this->getPidPath()));
    }

    /**
     * PCNTL signals handler
     *
     * @param int   $signo
     * @param array $siginfo
     * @param null $status
     */
    final static function signalHandler($signo, $siginfo = [], $status = null): void
    {
        switch ($signo) {
            case SIGINT:
            case SIGTERM:
                //shutdown
                self::$stopFlag = true;
                break;
            case SIGHUP:
                //restart, not implemented
                break;
            case SIGUSR1:
                //user signal, not implemented
                break;
            case SIGCHLD:
                $pid = $siginfo['pid'] ?? null;
                if (!$pid) {
                    $pid = pcntl_waitpid(-1, $status, WNOHANG);
                }
                while ($pid > 0) {
                    if ($pid && isset(static::$currentJobs[$pid])) {
                        unset(static::$currentJobs[$pid]);
                    }
                    $pid = pcntl_waitpid(-1, $status, WNOHANG);
                }
                break;
        }
    }

    /**
     * Tasks runner
     *
     * @param string $job
     *
     * @return boolean
     * @throws DbException
     * @throws ExitException
     */
    final public function runDaemon($job): bool
    {
        if ($this->isMultiInstance) {
            $this->flushLog();
            $pid = pcntl_fork();
            if ($pid == -1) {
                return false;
            } elseif ($pid !== 0) {
                static::$currentJobs[$pid] = true;

                return true;
            } else {
                $this->cleanLog();
                $this->renewConnections();
                //child process must die
                $this->trigger(self::EVENT_BEFORE_JOB);
                $status = $this($job);
                $this->trigger(self::EVENT_AFTER_JOB);
                if ($status) {
                    $this->halt(ExitCode::OK);
                } else {
                    $this->halt(ExitCode::UNSPECIFIED_ERROR, sprintf('Child process #%s return error.', $pid));
                }
            }
        } else {
            $this->trigger(self::EVENT_BEFORE_JOB);
            $status = $this($job);
            $this->trigger(self::EVENT_AFTER_JOB);

            return (bool) $status;
        }

        return null;
    }

    /**
     * Stop process and show or write message
     *
     * @param $code int -1|0|1
     * @param $message string
     * @throws ExitException
     */
    protected function halt($code, $message = null): void
    {
        if ($message !== null) {
            if ($code === ExitCode::UNSPECIFIED_ERROR) {
                Yii::error($message);
                if (!$this->demonize) {
                    $message = Console::ansiFormat($message, [Console::FG_RED]);
                }
            } else {
                Yii::debug($message);
            }
            if (!$this->demonize) {
                $this->writeConsole($message);
            }
        }
        if ($code !== -1) {
            Yii::$app->end($code);
        }
    }

    /**
     * Renew connections
     * @throws DbException
     */
    protected function renewConnections(): void
    {
        if (isset(Yii::$app->db)) {
            Yii::$app->db->close();
            Yii::$app->db->open();
        }
    }

    /**
     * Show message in console
     * @param string $message
     */
    private function writeConsole(string $message): void
    {
        $out = Console::ansiFormat(sprintf('[%s] ', date('d.m.Y H:i:s')), [Console::BOLD]);
        $this->stdout(sprintf('%s%s%s', $out, $message, PHP_EOL));
    }

    /**
     * @param string $daemon
     *
     * @return string
     */
    public function getPidPath(?string $daemon = null): string
    {
        $dir = Yii::getAlias($this->pidDir);
        if (!file_exists($dir)) {
            mkdir($dir, 0744, true);
        }

        $daemon = $this->getProcessName($daemon);

        return sprintf('%s%s%s', $dir, DIRECTORY_SEPARATOR, $daemon);
    }

    /**
     * @param string|null $route
     *
     * @return string
     */
    public function getProcessName(?string $route = null): string
    {
        if (is_null($route)) {
            $route = Yii::$app->requestedRoute;
        }

        return str_replace(['/index', '/'], ['', '.'], $route);
    }

    /**
     *  If in daemon mode - no write to console
     *
     * @param string $string
     *
     * @return bool|int
     */
    public function stdout($string)
    {
        if (!$this->demonize && is_resource(STDOUT)) {
            return parent::stdout($string);
        } else {
            return false;
        }
    }

    /**
     * If in daemon mode - no write to console
     *
     * @param string $string
     *
     * @return bool|int
     */
    public function stderr($string)
    {
        if (!$this->demonize && is_resource(\STDERR)) {
            return parent::stderr($string);
        } else {
            return false;
        }
    }

    /**
     * Empty log queue
     */
    protected function cleanLog(): void
    {
        Yii::$app->log->logger->messages = [];
    }

    /**
     * Empty log queue
     */
    protected function flushLog(): void
    {
        Yii::$app->log->logger->flush(true);
    }
}