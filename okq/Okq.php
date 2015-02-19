<?php

class Okq extends Redis
{
    const EVENT_ACK = 'ack';
    const EVENT_NOACK = 'noack';
    const EVENT_STOP = 'stop';

    private function doPush($queue, $eventId, $contents, $pushRight = false)
    {
        $queue = (string)$queue;
        $contents = (string)$contents;
        if (is_null($eventId)) {
            $eventId = self::getUUID(microtime(true) . $contents);
        } else {
            $eventId = (string)$eventId;
        }
        if ($pushRight) {
            $command = 'QRPUSH';
        } else {
            $command = 'QLPUSH';
        }
        $response = $this->rawCommand($command, $queue, $eventId, $contents);
        if ($response === false) {
            $lastError = $this->getLastError();
            trigger_error("Okq - failed to send $command $queue $eventId: $lastError", E_USER_ERROR);
            return false;
        }

        if ($response) {
            return true;
        } else {
            return false;
        }
    }

    public function qregister($queues)
    {
        $rawCommandArgs = $queues;
        array_unshift($rawCommandArgs, 'QREGISTER');
        $response = call_user_func_array(array($this, 'rawCommand'), $rawCommandArgs);
        return $response;
    }

    public function qrpop($queue, $ackTimeout = null, $noAck = false)
    {
        $queue = (string)$queue;
        $rawCommandArgs = array('QRPOP', $queue);
        if (!is_null($ackTimeout)) {
            $rawCommandArgs[] = (string)$ackTimeout;
        }
        if ($noAck) {
            $rawCommandArgs[] = 'NOACK';
        }
        $event = call_user_func_array(array($this, 'rawCommand'), $rawCommandArgs);
        return $event;
    }

    public function qlpeek($queue)
    {
        $queue = (string)$queue;
        $event = $this->rawCommand('QLPEEK', $queue);
        return $event;
    }

    public function qrpeek($queue)
    {
        $queue = (string)$queue;
        $event = $this->rawCommand('QRPEEK', $queue);
        return $event;
    }

    public function qack($queue, $eventId)
    {
        $queue = (string)$queue;
        $eventId = (string)$eventId;
        $response = $this->rawCommand('QACK', $queue, $eventId);
        return $response;
    }

    public function qlpush($queue, $contents, $eventId = null)
    {
        return $this->doPush($queue, $eventId, $contents, false);
    }

    public function qrpush($queue, $contents, $eventId = null)
    {
        return $this->doPush($queue, $eventId, $contents, true);
    }

    public function qnotify($timeout)
    {
        $queue = $this->rawCommand('QNOTIFY', $timeout);
        return $queue;
    }

    public function qstatus($queues)
    {
        if (is_array($queues)) {
            array_unshift($queues, 'STATUS');
            $response = call_user_func_array(array($this, 'rawCommand'), $queues);
        } else {
            $response = $this->rawCommand('STATUS');
        }
        return $response;
    }

    public function consume($callback, $queues = null, $timeout = 30)
    {
        if (is_array($queues) && empty($queues)) {
            throw new Exception('Okq - no queues provided');
        }
        if (!is_callable($callback)) {
            throw new Exception('Okq - the provided callback is not callable');
        }
        if (!is_numeric($timeout) || $timeout < 0) {
            throw new Exception('Okq - timeout must be at least 0');
        }
        $this->clearLastError();

        if (!empty($queues) && is_array($queues)) {
            $response = $this->qregister($queues);
            if ($response === false) {
                $lastError = $this->getLastError();
                trigger_error("Okq - failed to send QREGISTER: $lastError", E_USER_ERROR);
                return false;
            }
        }

        $continue = true;
        $timeout = (string)$timeout;
        while ($continue) {
            $queue = $this->qnotify($timeout);

            if (!empty($queue)) {
                $queue = (string)$queue;
                $event = $this->rawCommand('QRPOP', $queue);
                if ($event === false) {
                    break;
                }
            } else {
                $event = null;
            }

            $response = call_user_func($callback, $queue, $event);
            switch ($response) {
                case self::EVENT_ACK:
                    if (!is_null($event) && !isset($event[0])) {
                        $eventId = (string)$event[0];
                        $ack = $this->qack($queue, $eventId);
                        if ($ack === false) {
                            $continue = false;
                            break;
                        }
                    }
                    break;
                case self::EVENT_STOP:
                    $continue = false;
                    break;
                case self::EVENT_NOACK:
                default:
                    break;
            }
        }

        return true;
    }

    public static function getUUID($input = null)
    {
        $format = '%04x%04x-%04x-%04x-%04x-%04x%04x%04x';
        $v = array();
        if ($input) {
            $data = md5($input);
            $result = unpack('S*', hex2bin($data));
            $v[] = $result[1];
            $v[] = $result[2];
            $v[] = $result[3];
            $v[] = ($result[4] & 0x0fff) | 0x4000;
            $v[] = ($result[5] & 0x3fff) | 0x8000;
            $v[] = $result[6];
            $v[] = $result[7];
            $v[] = $result[8];
        } else {
            $v[] = mt_rand(0, 0xffff);
            $v[] = mt_rand(0, 0xffff);
            $v[] = mt_rand(0, 0xffff);
            $v[] = mt_rand(0, 0x0fff) | 0x4000;
            $v[] = mt_rand(0, 0x3fff) | 0x8000;
            $v[] = mt_rand(0, 0xffff);
            $v[] = mt_rand(0, 0xffff);
            $v[] = mt_rand(0, 0xffff);
        }
        return sprintf($format, $v[0], $v[1], $v[2], $v[3], $v[4], $v[5], $v[6], $v[7]);
    }
}
